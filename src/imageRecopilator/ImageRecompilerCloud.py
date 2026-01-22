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
import traceback
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from PIL import Image
import io

"""
OPTIMIZADO PARA t3.small (2 vCPUs, 2 GB RAM)
============================================

CAMBIOS RESPECTO AL ORIGINAL:
- Workers S3 reducidos: 3 → 2 (suficiente para el throughput)
- Cola reducida: 100 → 40 (menos RAM)
- JPEG Quality: 75 → 80 (mejor calidad, imagen ~100KB)
- Métricas cada 5 min (antes 15 min)
- Logs detallados con traceback completo
- Verificación de credenciales AWS al inicio
- Manejo explícito de errores S3

FUNCIONAMIENTO:
- 14 tareas asíncronas en paralelo (una por planta)
- Cada planta verifica su horario independientemente
- Capturas cada 60 segundos cuando está en horario
- 2 workers S3 procesan cola compartida
- Compresión en ThreadPool (CPU-bound)
"""

# uvloop para mejor performance en Linux
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


# =========================
# Configuración
# =========================

BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIX = os.getenv("S3_PREFIX", "capturas")

INTERVALO = int(os.getenv("INTERVALO", "60"))
MARGEN_PREVIO = int(os.getenv("MARGEN_PREVIO", "1200"))  # 20 min

TZ = os.getenv("TZ", "America/Santiago")
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "75"))
MAX_DESCARGAS_SIMULTANEAS = int(os.getenv("MAX_DESCARGAS", "10"))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "40"))  # Reducido para t3.small
NUM_UPLOADERS = int(os.getenv("NUM_UPLOADERS", "2"))  # Reducido
METRICAS_INTERVALO = int(os.getenv("METRICAS_INTERVALO", "300"))  # 5 min

os.environ["TZ"] = TZ

try:
    time.tzset()
except AttributeError:
    pass


# =========================
# Logging detallado
# =========================

logging.basicConfig(
    level=logging.INFO,  # Cambiado a INFO para ver más detalles
    format="%(asctime)s [%(levelname)s] %(message)s"
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

DENOMINADORES = {
    "Huechuraba": "HCH",
    "La Florida": "LFL",
    "La Pintana": "LPT",
    "Pudahuel": "PUD",
    "Quilicura": "QLC",
    "Recoleta": "RCL",
    "San Joaquin": "SJQ",
    "Temuco": "TMU",
    "Villarica": "VLL",
    "Chillan": "CHL",
    "Yungay": "YGY",
    "Concepcion": "CCP",
    "San Pedro de la Paz": "SPP",
    "Yumbel": "YMB"
}


# =========================
# SSL
# =========================

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# =========================
# Control de apagado
# =========================

RUNNING = True

def shutdown_handler(signum, frame):
    global RUNNING
    logger.warning("="*60)
    logger.warning("SEÑAL DE APAGADO RECIBIDA - Cerrando limpiamente...")
    logger.warning("="*60)
    RUNNING = False

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


# =========================
# Semáforo y Cola
# =========================

SEM_DESCARGAS = asyncio.Semaphore(MAX_DESCARGAS_SIMULTANEAS)
cola_subida = asyncio.Queue(maxsize=QUEUE_SIZE)


# =========================
# ThreadPool para compresión
# =========================

executor = ThreadPoolExecutor(max_workers=2)


# =========================
# Métricas
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
        ahora = time.time()
        async with self.lock:
            if ahora - self.ultima_impresion >= METRICAS_INTERVALO:
                ahorro_pct = 0
                if self.bytes_originales > 0:
                    ahorro_pct = ((self.bytes_originales - self.bytes_comprimidos) / self.bytes_originales) * 100
                
                logger.info("="*60)
                logger.info(f"MÉTRICAS ({METRICAS_INTERVALO/60:.0f} min):")
                logger.info(f"  Capturadas: {self.imagenes_capturadas} | Subidas: {self.imagenes_subidas} | Duplicadas: {self.imagenes_duplicadas}")
                logger.info(f"  Errores: Descarga={self.errores_descarga} S3={self.errores_s3}")
                logger.info(f"  Compresión: {self.bytes_originales/1024/1024:.1f}MB -> {self.bytes_comprimidos/1024/1024:.1f}MB (ahorro {ahorro_pct:.1f}%)")
                logger.info(f"  Cola: {cola_subida.qsize()}/{QUEUE_SIZE}")
                logger.info("="*60)
                
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
# Utilidades de horarios
# =========================

def es_domingo():
    return datetime.now().weekday() == 6


def dentro_horario(planta):
    ahora = datetime.now()
    dia = ahora.weekday()

    if dia == 6:
        return False

    tipo = "sabado" if dia == 5 else "semana"
    inicio, fin = HORARIOS[planta][tipo]

    h_ini = datetime.strptime(inicio, "%H:%M").time()
    h_fin = datetime.strptime(fin, "%H:%M").time()

    return h_ini <= ahora.time() <= h_fin


def segundos_hasta_apertura(planta):
    ahora = datetime.now()
    dia = ahora.weekday()

    if dia == 6:
        return None

    tipo = "sabado" if dia == 5 else "semana"
    inicio, _ = HORARIOS[planta][tipo]

    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    apertura = datetime.combine(ahora.date(), hora_inicio)

    if ahora.time() < hora_inicio:
        return int((apertura - ahora).total_seconds())
    else:
        dia_siguiente = (dia + 1) % 7
        
        if dia_siguiente == 6:
            return None
            
        tipo_siguiente = "sabado" if dia_siguiente == 5 else "semana"
        inicio_siguiente, _ = HORARIOS[planta][tipo_siguiente]
        
        hora_inicio_siguiente = datetime.strptime(inicio_siguiente, "%H:%M").time()
        apertura_siguiente = datetime.combine(ahora.date(), hora_inicio_siguiente) + timedelta(days=1)
        
        segundos = int((apertura_siguiente - ahora).total_seconds())
        return segundos


# =========================
# Utilidades de procesamiento
# =========================

def hash_imagen(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def recomprimir_jpeg_sync(data: bytes) -> bytes:
    try:
        img = Image.open(io.BytesIO(data))
        if 'exif' in img.info:
            img.info.pop('exif')
        
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=JPEG_QUALITY, optimize=True)
        return buffer.getvalue()
    except Exception as e:
        logger.error(f"Error recompresión: {e}\n{traceback.format_exc()}")
        return data


async def recomprimir_jpeg(data: bytes) -> bytes:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, recomprimir_jpeg_sync, data)


def generar_s3_key(planta: str, fecha_str: str) -> str:
    dt = datetime.strptime(fecha_str, "%Y%m%d_%H%M%S")
    denom = DENOMINADORES.get(planta, planta.replace(" ", "_"))
    filename = f"{denom}_{fecha_str}.jpg"

    return (
        f"{S3_PREFIX}/"
        f"{dt.year}/"
        f"{dt.month:02d}/"
        f"{dt.day:02d}/"
        f"{planta}/"
        f"{filename}"
    )


# =========================
# Verificación de credenciales AWS
# =========================

async def verificar_credenciales_aws():
    """
    Verifica que existan credenciales AWS válidas antes de iniciar.
    """
    logger.info("Verificando credenciales AWS...")
    
    session = aioboto3.Session()
    
    try:
        async with session.client('sts') as sts:
            identity = await sts.get_caller_identity()
            logger.info(f"✓ Credenciales AWS válidas")
            logger.info(f"  Account: {identity['Account']}")
            logger.info(f"  ARN: {identity['Arn']}")
            return True
    except NoCredentialsError:
        logger.critical("="*60)
        logger.critical("ERROR: NO SE ENCONTRARON CREDENCIALES AWS")
        logger.critical("="*60)
        logger.critical("")
        logger.critical("Opciones para configurar credenciales:")
        logger.critical("")
        logger.critical("1. IAM ROLE (RECOMENDADO):")
        logger.critical("   - EC2 Console → tu instancia")
        logger.critical("   - Actions → Security → Modify IAM role")
        logger.critical("   - Asignar role con permisos S3")
        logger.critical("")
        logger.critical("2. AWS CLI:")
        logger.critical("   pip3 install awscli")
        logger.critical("   aws configure")
        logger.critical("")
        logger.critical("3. Variables de entorno:")
        logger.critical("   export AWS_ACCESS_KEY_ID='...'")
        logger.critical("   export AWS_SECRET_ACCESS_KEY='...'")
        logger.critical("   export AWS_DEFAULT_REGION='us-east-1'")
        logger.critical("")
        logger.critical("="*60)
        return False
    except ClientError as e:
        logger.critical(f"ERROR AL VERIFICAR CREDENCIALES AWS:")
        logger.critical(f"  {e}")
        logger.critical(f"\n{traceback.format_exc()}")
        return False
    except Exception as e:
        logger.critical(f"ERROR INESPERADO AL VERIFICAR CREDENCIALES:")
        logger.critical(f"  {e}")
        logger.critical(f"\n{traceback.format_exc()}")
        return False


# =========================
# Worker de subida S3
# =========================

async def worker_subida_s3(worker_id: int):
    """
    Worker que procesa la cola de subidas a S3.
    """
    session = aioboto3.Session()
    
    logger.info(f"Worker S3 #{worker_id} iniciado")
    
    async with session.client('s3') as s3:
        while RUNNING or not cola_subida.empty():
            try:
                item = await asyncio.wait_for(cola_subida.get(), timeout=5.0)
                
                planta, fecha_str, data_comprimida, bytes_originales = item
                
                key = generar_s3_key(planta, fecha_str)
                
                try:
                    await s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=key,
                        Body=data_comprimida,
                        ContentType="image/jpeg",
                        StorageClass="INTELLIGENT_TIERING"
                    )
                    
                    await metricas.registrar_subida(bytes_originales, len(data_comprimida))
                    logger.debug(f"[W{worker_id}] ✓ {planta} → s3://{S3_BUCKET}/{key}")
                    
                except NoCredentialsError:
                    await metricas.registrar_error_s3()
                    logger.error(f"[W{worker_id}] {planta}: SIN CREDENCIALES AWS")
                    logger.error("  Configura IAM Role o credenciales")
                    
                except (BotoCoreError, ClientError) as e:
                    await metricas.registrar_error_s3()
                    logger.error(f"[W{worker_id}] S3 {planta}: {e}")
                    logger.error(f"{traceback.format_exc()}")
                
                finally:
                    cola_subida.task_done()
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[W{worker_id}] Error inesperado: {e}")
                logger.error(f"{traceback.format_exc()}")
    
    logger.info(f"Worker S3 #{worker_id} finalizado")


# =========================
# Captura
# =========================

async def capturar_camara(session, planta, cam_id):
    """
    Captura imágenes de una planta específica.
    """
    ultimo_hash = None
    errores_consecutivos = 0
    backoff_actual = INTERVALO

    logger.info(f"{planta} - Tarea iniciada")

    while RUNNING:
        if not RUNNING:
            break
        if not dentro_horario(planta):
            if not RUNNING:
                break
            if es_domingo():
                ahora = datetime.now()
                lunes = ahora + timedelta(days=1)
                while lunes.weekday() != 0:
                    lunes += timedelta(days=1)
                
                inicio_str, _ = HORARIOS[planta]["semana"]
                hora_apertura = datetime.strptime(inicio_str, "%H:%M").time()
                apertura_lunes = datetime.combine(lunes.date(), hora_apertura)
                
                despertar = apertura_lunes - timedelta(seconds=MARGEN_PREVIO)
                segundos = max(60, int((despertar - ahora).total_seconds()))
                
                horas = segundos // 3600
                logger.info(f"{planta} domingo, esperando {horas}h hasta apertura")
                
                # Sleep con checkpoints cada minuto
                for _ in range(int(segundos / 60)):
                    if not RUNNING:
                        break
                    await asyncio.sleep(60)
            else:
                segundos_espera = segundos_hasta_apertura(planta)
                
                if segundos_espera is None:
                    ahora = datetime.now()
                    lunes = ahora + timedelta(days=2)
                    
                    inicio_str, _ = HORARIOS[planta]["semana"]
                    hora_apertura = datetime.strptime(inicio_str, "%H:%M").time()
                    apertura_lunes = datetime.combine(lunes.date(), hora_apertura)
                    
                    despertar = apertura_lunes - timedelta(seconds=MARGEN_PREVIO)
                    segundos_espera = max(60, int((despertar - ahora).total_seconds()))
                
                espera_real = max(60, segundos_espera - MARGEN_PREVIO)
                minutos = espera_real // 60
                
                logger.info(f"{planta} fuera de horario, esperando {minutos}min")
                
                # Sleep con checkpoints cada minuto
                for _ in range(int(espera_real / 60)):
                    if not RUNNING:
                        break
                    await asyncio.sleep(60)
            
            continue

        pitime = int(time.time())
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"

        try:
            async with SEM_DESCARGAS:
                async with session.get(url, params={"pitime": pitime}) as resp:
                    if resp.status != 200:
                        errores_consecutivos += 1
                        await metricas.registrar_error_descarga()
                        logger.warning(f"{planta} HTTP {resp.status} - {url}")
                    else:
                        data_original = await resp.read()
                        bytes_originales = len(data_original)
                        await metricas.registrar_captura()
                        
                        data_comprimida = await recomprimir_jpeg(data_original)
                        
                        h = hash_imagen(data_comprimida)

                        if h != ultimo_hash:
                            try:
                                await asyncio.wait_for(
                                    cola_subida.put((planta, fecha_str, data_comprimida, bytes_originales)),
                                    timeout=5.0
                                )
                                ultimo_hash = h
                                errores_consecutivos = 0
                                backoff_actual = INTERVALO
                                logger.debug(f"{planta} captura OK ({bytes_originales/1024:.1f}KB → {len(data_comprimida)/1024:.1f}KB)")
                            except asyncio.TimeoutError:
                                logger.warning(f"{planta} cola llena (timeout)")
                        else:
                            await metricas.registrar_duplicada()
                            errores_consecutivos = 0
                            logger.debug(f"{planta} imagen duplicada (skip)")

        except asyncio.TimeoutError:
            errores_consecutivos += 1
            await metricas.registrar_error_descarga()
            logger.error(f"{planta} timeout al descargar")
        except Exception as e:
            errores_consecutivos += 1
            await metricas.registrar_error_descarga()
            logger.error(f"{planta} error: {e}")
            logger.error(f"{traceback.format_exc()}")

        if errores_consecutivos > 0:
            backoff_actual = min(INTERVALO * (2 ** errores_consecutivos), 3600)
        
        if errores_consecutivos >= 10:
            logger.critical(f"{planta} 10 errores consecutivos - pausa larga")
            # Sleep con checkpoints
            for _ in range(30):  # 30 minutos
                if not RUNNING:
                    break
                await asyncio.sleep(60)

        jitter = hash(planta) % 5
        await asyncio.sleep(backoff_actual + jitter)
        
        await metricas.imprimir_si_toca()
    
    logger.info(f"{planta} - Tarea finalizada")


# =========================
# Main
# =========================

async def main():
    """
    Función principal.
    """
    
    # Verificar credenciales AWS ANTES de iniciar
    if not await verificar_credenciales_aws():
        logger.critical("ABORTANDO: Configura credenciales AWS primero")
        return
    
    timeout = aiohttp.ClientTimeout(
        total=15,
        sock_connect=3,
        sock_read=10
    )
    
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,
        limit=50,
        limit_per_host=5,
        ttl_dns_cache=300
    )

    logger.info("="*60)
    logger.info("INICIANDO SISTEMA CAPTURA CCTV - AWS S3")
    logger.info(f"Event Loop: {'uvloop' if 'uvloop' in str(asyncio.get_event_loop_policy()) else 'asyncio'}")
    logger.info(f"Cámaras: {len(camaras)} | Intervalo: {INTERVALO}s")
    logger.info(f"JPEG Quality: {JPEG_QUALITY} | Workers S3: {NUM_UPLOADERS}")
    logger.info(f"Cola: {QUEUE_SIZE} | Margen previo: {MARGEN_PREVIO/60:.0f} min")
    logger.info(f"S3: s3://{S3_BUCKET}/{S3_PREFIX}")
    logger.info(f"Métricas cada: {METRICAS_INTERVALO/60:.0f} min")
    logger.info("="*60)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        
        workers_s3 = [
            asyncio.create_task(worker_subida_s3(i))
            for i in range(NUM_UPLOADERS)
        ]
        
        tasks_captura = [
            asyncio.create_task(capturar_camara(session, planta, cam_id))
            for planta, cam_id in camaras.items()
        ]
        
        await asyncio.gather(*tasks_captura, *workers_s3, return_exceptions=True)
        
        logger.info("Esperando que la cola se vacíe...")
        await cola_subida.join()
        logger.info("Cola vacía - cierre completo")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("="*60)
        logger.warning("CTRL+C DETECTADO - Apagado iniciado")
        logger.warning("="*60)
    except Exception as e:
        logger.critical(f"Error fatal: {e}")
        logger.critical(f"{traceback.format_exc()}")
    finally:
        executor.shutdown(wait=True)
        logger.info("="*60)
        logger.info("PROCESO FINALIZADO COMPLETAMENTE")
        logger.info("="*60)