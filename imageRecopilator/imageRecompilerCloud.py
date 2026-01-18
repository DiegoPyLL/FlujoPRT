import asyncio
import aiohttp
import aioboto3
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import os
import ssl
import hashlib
import signal
import logging
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image
import io

# uvloop para 2-4x performance en Linux
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass  # Fallback a asyncio normal si no está disponible


# =========================
# Configuración por entorno
# =========================

BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIX = os.getenv("S3_PREFIX", "capturas")


INTERVALO = int(os.getenv("INTERVALO", "60"))  # segundos


TZ = os.getenv("TZ", "America/Santiago")
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "75"))  # calidad compresión
MAX_DESCARGAS_SIMULTANEAS = int(os.getenv("MAX_DESCARGAS", "10"))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "100"))
NUM_UPLOADERS = int(os.getenv("NUM_UPLOADERS", "3"))
METRICAS_INTERVALO = int(os.getenv("METRICAS_INTERVALO", "900"))  # 15 min

# Modificación en imageRecompilerCloud.py
os.environ["TZ"] = TZ

try:
    time.tzset()
except AttributeError:
    # Windows no soporta tzset(), se ignora para permitir el funcionamiento local
    pass


# =========================
# Logging optimizado
# =========================

logging.basicConfig(
    level=logging.WARNING,  # Solo WARNING+ en producción
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("flujo-prt")


# =========================
# Cámaras
# =========================

camaras = {
    "Huechuraba": "10.57.6.222_Cam08",
    "La Florida": "10.57.0.222_Cam03",
    "La Pintana": "10.57.5.222_Cam09",
    "Pudahuel": "10.57.4.222_Cam07",
    "Quilicura": "10.57.2.222_Cam06",
    "Recoleta": "10.57.7.222_Cam09",
    "San Joaquin": "10.57.3.222_Cam07",
    "Temuco": "10.57.32.222_Cam01",
    "Villarica": "10.57.33.222_Cam04",
    "Chillan": "10.57.12.70",
    "Yungay": "10.57.20.70",
    "Concepcion": "10.57.19.70",
    "San Pedro de la Paz": "10.57.16.70",
    "Yumbel": "10.57.17.70"
}


# =========================
# Horarios
# =========================

HORARIOS = {
    "Huechuraba": {"semana": ("07:10", "16:50"), "sabado": ("07:10", "16:50")},
    "La Florida": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "La Pintana": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Pudahuel": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Quilicura": {"semana": ("07:10", "16:50"), "sabado": ("07:10", "16:50")},
    "Recoleta": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "San Joaquin": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Temuco": {"semana": ("08:10", "18:20"), "sabado": ("08:10", "13:50")},
    "Villarica": {"semana": ("07:10", "17:50"), "sabado": ("07:40", "13:50")},
    "Chillan": {"semana": ("06:40", "17:20"), "sabado": ("07:10", "13:50")},
    "Yungay": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Concepcion": {"semana": ("07:40", "20:20"), "sabado": ("08:10", "16:50")},
    "San Pedro de la Paz": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Yumbel": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")}
}


# =========================
# SSL (red interna)
# =========================

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# =========================
# Control de apagado limpio
# =========================

RUNNING = True

def shutdown_handler(signum, frame):
    global RUNNING
    logger.warning("Señal de apagado recibida")
    RUNNING = False

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


# =========================
# Semáforo y Cola global
# =========================

SEM_DESCARGAS = asyncio.Semaphore(MAX_DESCARGAS_SIMULTANEAS)
cola_subida = asyncio.Queue(maxsize=QUEUE_SIZE)


# =========================
# ThreadPool para compresión CPU-bound
# =========================

executor = ThreadPoolExecutor(max_workers=2)


# =========================
# Métricas en memoria
# =========================

class Metricas:
    def __init__(self):
        self.imagenes_capturadas = 0
        self.imagenes_subidas = 0
        self.imagenes_duplicadas = 0
        self.errores_descarga = 0
        self.errores_s3 = 0
        self.bytes_comprimidos = 0
        self.bytes_originales = 0
        self.ultima_impresion = time.time()
        self.lock = asyncio.Lock()
    
    async def registrar_captura(self):
        async with self.lock:
            self.imagenes_capturadas += 1
    
    async def registrar_subida(self, bytes_orig, bytes_comp):
        async with self.lock:
            self.imagenes_subidas += 1
            self.bytes_originales += bytes_orig
            self.bytes_comprimidos += bytes_comp
    
    async def registrar_duplicada(self):
        async with self.lock:
            self.imagenes_duplicadas += 1
    
    async def registrar_error_descarga(self):
        async with self.lock:
            self.errores_descarga += 1
    
    async def registrar_error_s3(self):
        async with self.lock:
            self.errores_s3 += 1
    
    async def imprimir_si_toca(self):
        """Imprime métricas periódicamente"""
        ahora = time.time()
        async with self.lock:
            if ahora - self.ultima_impresion >= METRICAS_INTERVALO:
                ahorro_pct = 0
                if self.bytes_originales > 0:
                    ahorro_pct = ((self.bytes_originales - self.bytes_comprimidos) / self.bytes_originales) * 100
                
                logger.warning("="*60)
                logger.warning(f"MÉTRICAS ({METRICAS_INTERVALO/60:.0f} min):")
                logger.warning(f"  Capturadas: {self.imagenes_capturadas} | Subidas: {self.imagenes_subidas} | Duplicadas: {self.imagenes_duplicadas}")
                logger.warning(f"  Errores: Descarga={self.errores_descarga} S3={self.errores_s3}")
                logger.warning(f"  Compresión: {self.bytes_originales/1024/1024:.1f}MB -> {self.bytes_comprimidos/1024/1024:.1f}MB (ahorro {ahorro_pct:.1f}%)")
                logger.warning(f"  Cola: {cola_subida.qsize()}/{QUEUE_SIZE}")
                logger.warning("="*60)
                
                # Reset
                self.imagenes_capturadas = 0
                self.imagenes_subidas = 0
                self.imagenes_duplicadas = 0
                self.errores_descarga = 0
                self.errores_s3 = 0
                self.bytes_comprimidos = 0
                self.bytes_originales = 0
                self.ultima_impresion = ahora

metricas = Metricas()


# =========================
# Utilidades
# =========================

def dentro_horario(planta):
    """Verifica si estamos dentro del horario de operación"""
    ahora = datetime.now()
    dia = ahora.weekday()

    if dia == 6:  # Domingo
        return False

    tipo = "sabado" if dia == 5 else "semana"
    inicio, fin = HORARIOS[planta][tipo]

    h_ini = datetime.strptime(inicio, "%H:%M").time()
    h_fin = datetime.strptime(fin, "%H:%M").time()

    return h_ini <= ahora.time() <= h_fin


def segundos_hasta_apertura(planta):
    """Calcula segundos hasta la próxima apertura de la planta"""
    ahora = datetime.now()
    dia = ahora.weekday()
    
    # Si es domingo, esperar hasta el lunes
    if dia == 6:
        dias_hasta_apertura = 1
        tipo = "semana"
    # Si es sábado
    elif dia == 5:
        tipo = "sabado"
        dias_hasta_apertura = 0
    else:
        tipo = "semana"
        dias_hasta_apertura = 0
    
    inicio_str, _ = HORARIOS[planta][tipo]
    h_ini = datetime.strptime(inicio_str, "%H:%M").time()
    
    # Crear datetime de apertura
    apertura = datetime.combine(ahora.date(), h_ini)
    
    # Si ya pasó la hora de apertura hoy
    if ahora.time() > h_ini:
        # Calcular próxima apertura (mañana o lunes)
        if dia == 5:  # Sábado -> Lunes
            dias_hasta_apertura = 2
            tipo = "semana"
            inicio_str, _ = HORARIOS[planta][tipo]
            h_ini = datetime.strptime(inicio_str, "%H:%M").time()
        elif dia == 6:  # Domingo -> Lunes
            dias_hasta_apertura = 1
            tipo = "semana"
            inicio_str, _ = HORARIOS[planta][tipo]
            h_ini = datetime.strptime(inicio_str, "%H:%M").time()
        else:  # Lun-Vie -> Mañana
            dias_hasta_apertura = 1
            
        apertura = datetime.combine(ahora.date() + timedelta(days=dias_hasta_apertura), h_ini)
    
    segundos = (apertura - ahora).total_seconds()
    return max(60, int(segundos))  # Mínimo 60 segundos


def hash_imagen(data: bytes) -> str:
    """Calcula hash MD5 de la imagen comprimida"""
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def recomprimir_jpeg_sync(data: bytes) -> bytes:
    """
    Recomprime JPEG (versión sync para ThreadPool)
    Elimina metadata EXIF que cambia entre frames
    """
    try:
        img = Image.open(io.BytesIO(data))
        # Eliminar metadata EXIF/GPS que puede variar
        if 'exif' in img.info:
            img.info.pop('exif')
        
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=JPEG_QUALITY, optimize=True)
        return buffer.getvalue()
    except Exception as e:
        logger.error(f"Error recompresión: {e}")
        return data


async def recomprimir_jpeg(data: bytes) -> bytes:
    """Wrapper async para ejecutar compresión en ThreadPool"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, recomprimir_jpeg_sync, data)


def generar_s3_key(planta: str, fecha_str: str) -> str:
    """
    Genera key S3 particionada por fecha para mejor rendimiento
    capturas/planta/YYYY/MM/DD/HHMMSS.jpg
    """
    dt = datetime.strptime(fecha_str, "%Y%m%d_%H%M%S")
    return f"{S3_PREFIX}/{planta}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{dt.strftime('%H%M%S')}.jpg"


# =========================
# Worker de subida S3 (sin recompresión)
# =========================

async def worker_subida_s3(worker_id: int):
    """
    Worker que procesa la cola de subidas a S3
    IMPORTANTE: Recibe imagen YA comprimida, solo sube
    """
    session = aioboto3.Session()
    
    logger.warning(f"Worker S3 #{worker_id} iniciado")
    
    # Cliente S3 compartido para todas las subidas de este worker
    async with session.client('s3') as s3:
        while RUNNING or not cola_subida.empty():
            try:
                # Esperar un item de la cola (timeout para chequear RUNNING)
                item = await asyncio.wait_for(cola_subida.get(), timeout=5.0)
                
                planta, fecha_str, data_comprimida, bytes_originales = item
                
                # Generar key particionada
                key = generar_s3_key(planta, fecha_str)
                
                try:
                    # Subir directamente (ya está comprimida)
                    await s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=key,
                        Body=data_comprimida,
                        ContentType="image/jpeg",
                        StorageClass="INTELLIGENT_TIERING"
                    )
                    
                    await metricas.registrar_subida(bytes_originales, len(data_comprimida))
                    
                except (BotoCoreError, ClientError) as e:
                    await metricas.registrar_error_s3()
                    logger.error(f"[W{worker_id}] S3 {planta}: {e}")
                
                finally:
                    cola_subida.task_done()
                    
            except asyncio.TimeoutError:
                # Timeout normal, continuar
                continue
            except Exception as e:
                logger.error(f"[W{worker_id}] Error: {e}")
    
    logger.warning(f"Worker S3 #{worker_id} finalizado")


# =========================
# Captura con compresión única
# =========================

async def capturar_camara(session, planta, cam_id):
    """
    Captura imágenes de una cámara
    OPTIMIZACIÓN CRÍTICA: Comprime UNA SOLA VEZ, envía comprimida a cola
    """
    ultimo_hash = None
    errores_consecutivos = 0
    backoff_actual = INTERVALO

    while RUNNING:

        # Verificar horario
        if not dentro_horario(planta):
            segundos_espera = segundos_hasta_apertura(planta)
            await asyncio.sleep(segundos_espera)
            continue

        pitime = int(time.time())
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"

        try:
            # Semáforo para limitar descargas simultáneas
            async with SEM_DESCARGAS:
                async with session.get(url, params={"pitime": pitime}) as resp:
                    if resp.status != 200:
                        errores_consecutivos += 1
                        await metricas.registrar_error_descarga()
                        logger.warning(f"{planta} HTTP {resp.status}")
                    else:
                        data_original = await resp.read()
                        bytes_originales = len(data_original)
                        await metricas.registrar_captura()
                        
                        # COMPRESIÓN UNA SOLA VEZ (CPU-bound en ThreadPool)
                        data_comprimida = await recomprimir_jpeg(data_original)
                        
                        # Hash de imagen comprimida (sin metadata variable)
                        h = hash_imagen(data_comprimida)

                        # Solo encolar si la imagen cambió
                        if h != ultimo_hash:
                            try:
                                # Enviar imagen YA comprimida a la cola
                                await asyncio.wait_for(
                                    cola_subida.put((planta, fecha_str, data_comprimida, bytes_originales)),
                                    timeout=5.0
                                )
                                ultimo_hash = h
                                errores_consecutivos = 0
                                backoff_actual = INTERVALO
                            except asyncio.TimeoutError:
                                logger.warning(f"{planta} cola llena")
                        else:
                            await metricas.registrar_duplicada()
                            errores_consecutivos = 0

        except asyncio.TimeoutError:
            errores_consecutivos += 1
            await metricas.registrar_error_descarga()
            logger.error(f"{planta} timeout")
        except Exception as e:
            errores_consecutivos += 1
            await metricas.registrar_error_descarga()
            logger.error(f"{planta} error: {e}")

        # Backoff exponencial
        if errores_consecutivos > 0:
            backoff_actual = min(INTERVALO * (2 ** errores_consecutivos), 3600)
        
        # Alarma crítica
        if errores_consecutivos >= 10:
            logger.critical(f"{planta} 10 errores consecutivos")
            await asyncio.sleep(1800)  # 30 min

        # Jitter
        jitter = hash(planta) % 5
        await asyncio.sleep(backoff_actual + jitter)
        
        # Métricas periódicas
        await metricas.imprimir_si_toca()


# =========================
# Main
# =========================

async def main():
    """Inicia el sistema de captura con configuración optimizada"""
    
    # Timeouts diferenciados
    timeout = aiohttp.ClientTimeout(
        total=15,
        sock_connect=3,
        sock_read=10
    )
    
    # Connector optimizado
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,
        limit=50,
        limit_per_host=5,
        ttl_dns_cache=300
    )

    logger.warning("="*60)
    logger.warning("INICIANDO SISTEMA CAPTURA CCTV")
    logger.warning(f"Event Loop: {'uvloop' if 'uvloop' in str(asyncio.get_event_loop_policy()) else 'asyncio'}")
    logger.warning(f"Cámaras: {len(camaras)} | Intervalo: {INTERVALO}s")
    logger.warning(f"JPEG Quality: {JPEG_QUALITY} | Workers S3: {NUM_UPLOADERS}")
    logger.warning(f"S3: s3://{S3_BUCKET}/{S3_PREFIX}")
    logger.warning(f"Métricas cada: {METRICAS_INTERVALO/60:.0f} min")
    logger.warning("="*60)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        
        # Workers S3
        workers_s3 = [
            asyncio.create_task(worker_subida_s3(i))
            for i in range(NUM_UPLOADERS)
        ]
        
        # Tasks captura
        tasks_captura = [
            asyncio.create_task(capturar_camara(session, planta, cam_id))
            for planta, cam_id in camaras.items()
        ]
        
        # Ejecutar todo
        await asyncio.gather(*tasks_captura, *workers_s3, return_exceptions=True)
        
        # Vaciar cola
        await cola_subida.join()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrumpido por usuario")
    finally:
        executor.shutdown(wait=True)
        logger.warning("Proceso finalizado")
        
        