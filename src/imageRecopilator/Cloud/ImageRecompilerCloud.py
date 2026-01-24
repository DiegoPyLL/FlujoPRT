import asyncio
import json
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
import subprocess
import tempfile
import io
from collections import defaultdict
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from PIL import Image

"""
SISTEMA COMPLETO: CAPTURA + PROCESAMIENTO DOMINICAL
====================================================

FUNCIONAMIENTO:
---------------
1. Loop principal infinito
2. Lunes-Sábado: Captura imágenes cada 60s y sube a S3
3. Domingos: Ejecuta job de procesamiento (timelapses)
4. Vuelve al paso 1

OPTIMIZACIONES DOMINGO:
-----------------------
- Deduplicación temprana (antes de descargar)
- Procesamiento por lotes (2 plantas en paralelo)
- Descargas paralelas (5 a la vez)
- Borrado progresivo
- FFmpeg optimizado (preset=fast, crf=28)
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
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))
MAX_DESCARGAS_SIMULTANEAS = int(os.getenv("MAX_DESCARGAS", "10"))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "40"))
NUM_UPLOADERS = int(os.getenv("NUM_UPLOADERS", "2"))
METRICAS_INTERVALO = int(os.getenv("METRICAS_INTERVALO", "300"))  # 5 min

os.environ["TZ"] = TZ

try:
    time.tzset()
except AttributeError:
    pass


# =========================
# Logging
# =========================

logging.basicConfig(
    level=logging.INFO,
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


def todas_fuera_de_horario():
    for planta in camaras.keys():
        if dentro_horario(planta):
            return False
    return True


def obtener_tiempos_restantes():
    tiempos = {}
    for planta in camaras.keys():
        if es_domingo():
            tiempos[planta] = None
        elif dentro_horario(planta):
            tiempos[planta] = 0
        else:
            tiempos[planta] = segundos_hasta_apertura(planta)
    return tiempos


def obtener_menor_tiempo_espera():
    tiempos = obtener_tiempos_restantes()
    tiempos_validos = [t for t in tiempos.values() if t is not None and t > 0]
    
    if not tiempos_validos:
        return None
    
    return min(tiempos_validos)


async def esperar_hasta_apertura():
    """
    Suspensión COMPARTIDA - todas las plantas esperan juntas.
    """
    while RUNNING:
        if es_domingo():
            ahora = datetime.now()
            lunes = ahora + timedelta(days=1)
            while lunes.weekday() != 0:
                lunes += timedelta(days=1)
            
            primer_hora = min(HORARIOS[p]["semana"][0] for p in camaras.keys())
            hora_apertura = datetime.strptime(primer_hora, "%H:%M").time()
            apertura_lunes = datetime.combine(lunes.date(), hora_apertura)
            
            despertar = apertura_lunes - timedelta(seconds=MARGEN_PREVIO)
            segundos = int((despertar - ahora).total_seconds())
            
            horas = segundos // 3600
            minutos = (segundos % 3600) // 60
            logger.info(f"Domingo. Suspendiendo {horas}h {minutos}min (hasta 20 min antes de apertura del lunes)...")
            
            # Sleep con checkpoints cada minuto
            for _ in range(int(segundos / 60)):
                if not RUNNING:
                    return
                await asyncio.sleep(60)
            continue
        
        if todas_fuera_de_horario():
            menor = obtener_menor_tiempo_espera()
            if menor:
                espera_real = max(0, menor - MARGEN_PREVIO)
                minutos = int(espera_real / 60)
                logger.info(f"Todas fuera de horario. Suspendiendo {minutos} min (hasta 20 min antes de apertura)...")
                
                # Sleep con checkpoints cada minuto
                for _ in range(int(espera_real / 60)):
                    if not RUNNING:
                        return
                    await asyncio.sleep(60)
            else:
                logger.info("Sin aperturas inmediatas. Verificando en 1 hora...")
                for _ in range(60):
                    if not RUNNING:
                        return
                    await asyncio.sleep(60)
        else:
            break


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
        logger.error(f"Error recompresión: {e}")
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
# Verificación AWS
# =========================

async def verificar_credenciales_aws():
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
        return False
    except Exception as e:
        logger.critical(f"ERROR AL VERIFICAR CREDENCIALES: {e}")
        return False


# =========================
# Worker S3
# =========================

async def worker_subida_s3(worker_id: int):
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
                    
                except (BotoCoreError, ClientError) as e:
                    await metricas.registrar_error_s3()
                    logger.error(f"[W{worker_id}] S3 {planta}: {e}")
                
                finally:
                    cola_subida.task_done()
                    
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                # Permite cancelación limpia si se solicita desde main
                break
            except Exception as e:
                logger.error(f"[W{worker_id}] Error: {e}")
    
    logger.info(f"Worker S3 #{worker_id} finalizado")


# =========================
# Captura
# =========================

async def capturar_camara(session, planta, cam_id):
    """
    Captura con ciclo independiente de 60 segundos.
    """
    ultimo_hash = None
    errores_consecutivos = 0

    logger.info(f"{planta} - Tarea iniciada")

    while RUNNING:
        if not RUNNING:
            break

        if not dentro_horario(planta):
            try:
                await asyncio.sleep(INTERVALO)
            except asyncio.CancelledError:
                break
            continue

        pitime = int(time.time())
        fecha_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"

        exito = False

        for intento in range(5):
            try:
                async with SEM_DESCARGAS:
                    async with session.get(url, params={"pitime": pitime}) as resp:
                        if resp.status != 200:
                            logger.warning(f"{planta} - Intento {intento + 1}/5 HTTP {resp.status}")
                            await asyncio.sleep(2.5)
                            continue
                        
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
                                logger.info(f"{planta} - Imagen guardada: {DENOMINADORES[planta]}_{fecha_str}.jpg")
                            except asyncio.TimeoutError:
                                logger.warning(f"{planta} cola llena")
                        else:
                            await metricas.registrar_duplicada()
                        
                        exito = True
                        errores_consecutivos = 0
                        break

            except asyncio.TimeoutError:
                logger.warning(f"{planta} - Intento {intento + 1}/5 timeout")
                await asyncio.sleep(2.5)
            except asyncio.CancelledError:
                raise # Re-lanzar para salir del loop
            except Exception as e:
                logger.warning(f"{planta} - Intento {intento + 1}/5 error: {e}")
                await asyncio.sleep(2.5)

        if not exito:
            errores_consecutivos += 1
            await metricas.registrar_error_descarga()
            logger.error(f"{planta} no respondió después de 5 intentos")
            
            if errores_consecutivos >= 10:
                logger.critical(f"{planta} - 10 errores consecutivos, pausa de 10 min")
                for _ in range(10):
                    if not RUNNING:
                        break
                    await asyncio.sleep(60)
                errores_consecutivos = 0

        jitter = hash(planta) % 5
        try:
            await asyncio.sleep(INTERVALO + jitter)
        except asyncio.CancelledError:
            break
        
        await metricas.imprimir_si_toca()
    
    logger.info(f"{planta} - Tarea finalizada")


# ====================================
# SUNDAY WORKER - Procesamiento Dominical
# ====================================

class SundayWorker:
    def __init__(self):
        self.session = aioboto3.Session()
        self.procesado_semana = None  # Evita reprocesar la misma semana
    
    def obtener_semana_anterior(self):
        """Retorna (año, semana) del lunes-sábado pasado"""
        hoy = datetime.now()
        inicio_semana_anterior = hoy - timedelta(days=hoy.weekday() + 1)
        return inicio_semana_anterior.isocalendar()[:2]
    
    async def identificar_conjuntos(self, semana):
        """Lista imágenes Y deduplica en memoria usando ETag"""
        año, num_semana = semana
        inicio = datetime.strptime(f"{año}-W{num_semana:02d}-1", "%Y-W%W-%w")
        
        conjuntos = defaultdict(list)
        etags_globales = {}
        duplicados_identificados = []
        
        async with self.session.client('s3') as s3:
            for dia_offset in range(6):
                fecha = inicio + timedelta(days=dia_offset)
                prefix = f"{S3_PREFIX}/{fecha.year}/{fecha.month:02d}/{fecha.day:02d}/"
                
                paginator = s3.get_paginator('list_objects_v2')
                async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                    if 'Contents' not in page:
                        continue
                    
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.jpg'):
                            parts = obj['Key'].split('/')
                            planta = parts[4] if len(parts) > 4 else 'unknown'
                            etag = obj['ETag'].strip('"')
                            
                            etag_key = f"{planta}:{etag}"
                            if etag_key in etags_globales:
                                duplicados_identificados.append(obj['Key'])
                                continue
                            
                            etags_globales[etag_key] = obj['Key']
                            dia_key = f"{fecha.date()}"
                            
                            conjuntos[(planta, dia_key)].append({
                                'key': obj['Key'],
                                'etag': etag,
                                'size': obj['Size']
                            })
        
        logger.info(f"Identificados {len(conjuntos)} conjuntos planta/día")
        logger.info(f"Duplicados: {len(duplicados_identificados)} (no se descargarán)")
        
        if duplicados_identificados:
            await self.borrar_keys(duplicados_identificados)
        
        return conjuntos
    
    async def generar_timelapses(self, conjuntos):
        """Genera timelapses de a 2 plantas en paralelo"""
        por_planta = defaultdict(list)
        
        for (planta, dia), imagenes in conjuntos.items():
            por_planta[planta].extend(imagenes)
        
        plantas = list(por_planta.items())
        
        for i in range(0, len(plantas), 2):
            if not RUNNING:
                break
            
            batch = plantas[i:i+2]
            tasks = [
                self.procesar_planta_timelapse(planta, imagenes)
                for planta, imagenes in batch
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(5)

    async def procesar_planta_timelapse(self, planta, imagenes):
        """Procesa una planta individual"""
        if not imagenes:
            return
        
        imagenes_sorted = sorted(imagenes, key=lambda x: x['key'])
        
        input_hash = hashlib.md5(
            '|'.join([img['key'] for img in imagenes_sorted]).encode()
        ).hexdigest()
        
        año, semana = self.obtener_semana_anterior()
        manifest_key = f"timelapses/{año}/semana_{semana:02d}/{planta}.manifest"
        
        async with self.session.client('s3') as s3:
            try:
                obj = await s3.get_object(Bucket=S3_BUCKET, Key=manifest_key)
                manifest = json.loads(await obj['Body'].read())
                
                if manifest.get('input_hash') == input_hash:
                    logger.info(f"[SKIP] {planta} - timelapse ya existe")
                    keys_borrar = [img['key'] for img in imagenes_sorted]
                    await self.borrar_keys(keys_borrar)
                    return
            except:
                pass
        
        logger.info(f"[GENERANDO] {planta} - {len(imagenes_sorted)} frames")
        
        video_key = await self.crear_timelapse(planta, imagenes_sorted, año, semana)
        
        if video_key:
            async with self.session.client('s3') as s3:
                manifest = {
                    'input_hash': input_hash,
                    'num_frames': len(imagenes_sorted),
                    'video_key': video_key,
                    'timestamp': datetime.now().isoformat()
                }
                
                await s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=manifest_key,
                    Body=json.dumps(manifest, indent=2)
                )
            
            logger.info(f"  → {planta} completado: s3://{S3_BUCKET}/{video_key}")

    async def crear_timelapse(self, planta, imagenes, año, semana):
        """Descarga con paralelismo, descomprime a quality=100 y genera video"""
        
        # Calcular rango de fechas
        inicio = datetime.strptime(f"{año}-W{semana:02d}-1", "%Y-W%W-%w")
        fin = inicio + timedelta(days=5)  # sábado
        nombre_rango = f"{inicio.day:02d}_{inicio.month:02d}-{fin.day:02d}_{fin.month:02d}"
        
        with tempfile.TemporaryDirectory() as tmpdir:
            resolucion_ref = None
            frames_validos = []
            keys_descargadas = []
            
            config = aioboto3.session.Config(
                max_pool_connections=50,
                retries={'max_attempts': 3, 'mode': 'adaptive'}
            )
            
            async with self.session.client('s3', config=config) as s3:
                sem = asyncio.Semaphore(5)
                
                async def descargar_y_validar(img_info):
                    async with sem:
                        obj = await s3.get_object(Bucket=S3_BUCKET, Key=img_info['key'])
                        data = await obj['Body'].read()
                        return data, img_info['key']
                
                tasks = [descargar_y_validar(img) for img in imagenes]
                resultados = await asyncio.gather(*tasks)
                
                for data, key in resultados:
                    keys_descargadas.append(key)
                    
                    try:
                        img = Image.open(io.BytesIO(data))
                        resolucion = img.size
                        
                        if resolucion_ref is None:
                            resolucion_ref = resolucion
                        elif resolucion != resolucion_ref:
                            logger.warning(f"  [WARN] Resolución inconsistente: {key}")
                            continue
                        
                        # DESCOMPRIMIR a quality=100
                        img = img.convert("RGB")
                        frame_path = f"{tmpdir}/{len(frames_validos):06d}.jpg"
                        img.save(
                            frame_path,
                            format="JPEG",
                            quality=100,
                            optimize=False,
                            subsampling=0
                        )
                        
                        frames_validos.append(frame_path)
                        
                    except Exception as e:
                        logger.error(f"  [ERROR] Procesando {key}: {e}")
                        continue
                    
                    if len(frames_validos) % 100 == 0 and len(frames_validos) > 0:
                        logger.info(f"  Procesados {len(frames_validos)} frames...")
            
            if len(frames_validos) < 10:
                logger.error(f"  [ERROR] Solo {len(frames_validos)} frames válidos, abortando")
                return None
            
            logger.info(f"  Total frames válidos: {len(frames_validos)}")
            logger.info(f"  Generando video con ffmpeg...")
            
            video_path = f"{tmpdir}/timelapse.mp4"
            result = subprocess.run([
                'ffmpeg', '-y',
                '-framerate', '30',
                '-pattern_type', 'glob',
                '-i', f'{tmpdir}/*.jpg',
                '-c:v', 'libx264',
                '-preset', 'fast',
                '-crf', '28',
                '-pix_fmt', 'yuv420p',
                video_path
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"  [ERROR] ffmpeg falló: {result.stderr}")
                return None
            
            # Nombre con rango de fechas
            video_key = f"timelapses/{año}/semana_{semana:02d}/{planta}_{nombre_rango}.mp4"
            async with self.session.client('s3') as s3:
                with open(video_path, 'rb') as f:
                    await s3.upload_fileobj(f, S3_BUCKET, video_key)
            
            logger.info(f"  Video generado, borrando {len(keys_descargadas)} imágenes...")
            await self.borrar_keys(keys_descargadas)

        return video_key

    async def borrar_keys(self, keys):
        """Borra keys en batches de 1000"""
        if not keys:
            return
        
        async with self.session.client('s3') as s3:
            total = 0
            for i in range(0, len(keys), 1000):
                batch = [{'Key': k} for k in keys[i:i+1000]]
                await s3.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete={'Objects': batch, 'Quiet': True}
                )
                total += len(batch)
            
            logger.info(f"  Borradas {total} imágenes de S3")
    
    async def ejecutar(self):
        """Ejecuta el procesamiento dominical"""
        semana_actual = self.obtener_semana_anterior()
        
        # Evitar reprocesar la misma semana múltiples veces
        if self.procesado_semana == semana_actual:
            logger.info("Semana ya procesada, esperando próximo domingo...")
            return
        
        año, num_semana = semana_actual
        
        logger.info("="*60)
        logger.info("INICIO PROCESAMIENTO DOMINICAL")
        logger.info(f"Procesando semana {año}-W{num_semana:02d}")
        logger.info("="*60)
        
        conjuntos = await self.identificar_conjuntos(semana_actual)
        await self.generar_timelapses(conjuntos)
        
        self.procesado_semana = semana_actual
        
        logger.info("="*60)
        logger.info("FIN PROCESAMIENTO DOMINICAL")
        logger.info("="*60)


# =========================
# Main
# =========================

async def main():
    if not await verificar_credenciales_aws():
        logger.critical("ABORTANDO: Configura credenciales AWS primero")
        return
    
    timeout = aiohttp.ClientTimeout(
        total=20,
        sock_connect=5,
        sock_read=15
    )
    
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,
        limit=50,
        limit_per_host=5,
        ttl_dns_cache=300
    )

    logger.info("="*60)
    logger.info("INICIANDO SISTEMA CAPTURA + PROCESAMIENTO CCTV")
    logger.info(f"Event Loop: {'uvloop' if 'uvloop' in str(asyncio.get_event_loop_policy()) else 'asyncio'}")
    logger.info(f"Cámaras: {len(camaras)} | Intervalo: {INTERVALO}s")
    logger.info(f"JPEG Quality: {JPEG_QUALITY} | Workers S3: {NUM_UPLOADERS}")
    logger.info(f"S3: s3://{S3_BUCKET}/{S3_PREFIX}")
    logger.info("="*60)

    sunday_worker = SundayWorker()

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        
        # BUCLE PRINCIPAL INFINITO (IMPERATIVO: NO BREAK)
        while RUNNING:
            # 1. ¿Es Domingo? Ejecutar procesamiento y esperar
            if es_domingo():
                logger.info("DOMINGO DETECTADO - Iniciando procesamiento de semana anterior...")
                await sunday_worker.ejecutar()
                
                # Suspender hasta el lunes para evitar re-entradas rápidas
                await esperar_hasta_apertura()
                # Al volver del await, el bucle reinicia y verificará de nuevo condiciones
                continue
            
            # 2. Lunes-Sábado: Configuración de tareas de captura
            await esperar_hasta_apertura()
            
            if not RUNNING:
                # Si durante la espera se recibió señal de apagado, el while principal lo detectará
                continue
            
            logger.info("Iniciando ciclo semanal de capturas...")
            
            # Lanzar workers S3
            workers_s3 = [
                asyncio.create_task(worker_subida_s3(i))
                for i in range(NUM_UPLOADERS)
            ]
            
            # Lanzar capturas en paralelo
            tasks_captura = [
                asyncio.create_task(capturar_camara(session, planta, cam_id))
                for planta, cam_id in camaras.items()
            ]
            
            # 3. MONITOREO DEL CICLO SEMANAL
            # Esperar hasta que termine el día (sábado a las 23:59 o se detecte domingo)
            # O se reciba señal de apagado (RUNNING = False)
            while RUNNING and not es_domingo():
                await asyncio.sleep(60)
            
            # 4. LIMPIEZA DE TRANSICIÓN (Llegó Domingo o Apagado)
            logger.info("Transición detectada (Domingo o Shutdown) - Deteniendo tareas...")

            # Cancelar tareas de captura primero (para dejar de meter items a la cola)
            for task in tasks_captura:
                task.cancel()
            
            # Esperar confirmación de cancelación de capturas
            await asyncio.gather(*tasks_captura, return_exceptions=True)
            
            # Opcional: Intentar vaciar la cola antes de matar a los workers S3
            if not cola_subida.empty():
                logger.info("Drenando cola de subida antes de cancelar workers...")
                # Dar un tiempo razonable para vaciar, si no, cancelar
                try:
                    # Esperamos hasta que la cola esté vacía (task_done llamado por workers)
                    # Ojo: si los workers mueren antes, esto se cuelga, pero aquí siguen vivos.
                    await asyncio.wait_for(cola_subida.join(), timeout=300)
                except asyncio.TimeoutError:
                    logger.warning("Timeout drenando cola - procediendo a cancelación forzada")

            # Cancelar workers S3
            for task in workers_s3:
                task.cancel()
            
            await asyncio.gather(*workers_s3, return_exceptions=True)
            
            logger.info("Ciclo de captura detenido. Reiniciando bucle principal...")
            # Aquí termina el while, vuelve al inicio. Si es Domingo, entra al if es_domingo().
        
        # Fuera del while RUNNING (Solo ocurre en apagado total)
        logger.info("Esperando que la cola se vacíe (cleanup final)...")
        if not cola_subida.empty():
             try:
                 await asyncio.wait_for(cola_subida.join(), timeout=60)
             except Exception:
                 pass
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