import asyncio
import aiohttp
import time
from datetime import datetime
import os
import ssl
import hashlib
import signal
import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError


# =========================
# Configuración por entorno
# =========================

BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIX = os.getenv("S3_PREFIX", "capturas")

INTERVALO = int(os.getenv("INTERVALO", "60"))  # segundos
TZ = os.getenv("TZ", "America/Santiago")

os.environ["TZ"] = TZ
time.tzset()


# =========================
# Logging mínimo (CloudWatch)
# =========================

logging.basicConfig(
    level=logging.INFO,
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
    "Huechuraba": {"semana": ("07:30", "16:30"), "sabado": ("07:30", "16:30")},
    "La Florida": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "La Pintana": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "Pudahuel": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "Quilicura": {"semana": ("07:30", "16:30"), "sabado": ("07:30", "16:30")},
    "Recoleta": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "San Joaquin": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "Temuco": {"semana": ("08:30", "18:00"), "sabado": ("08:30", "13:30")},
    "Villarica": {"semana": ("07:30", "17:30"), "sabado": ("08:00", "13:30")},
    "Chillan": {"semana": ("07:00", "17:00"), "sabado": ("07:30", "13:30")},
    "Yungay": {"semana": ("08:00", "17:00"), "sabado": ("08:30", "13:30")},
    "Concepcion": {"semana": ("08:00", "20:00"), "sabado": ("08:30", "16:30")},
    "San Pedro de la Paz": {"semana": ("08:00", "17:00"), "sabado": ("08:30", "13:30")},
    "Yumbel": {"semana": ("08:00", "17:00"), "sabado": ("08:30", "13:30")}
}


# =========================
# SSL (red interna)
# =========================

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# =========================
# AWS S3
# =========================

s3 = boto3.client("s3")


# =========================
# Control de apagado limpio
# =========================

RUNNING = True

def shutdown_handler(signum, frame):
    global RUNNING
    logger.info("Señal de apagado recibida")
    RUNNING = False

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


# =========================
# Utilidades
# =========================

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


def hash_imagen(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


async def subir_s3(planta, fecha, data):
    key = f"{S3_PREFIX}/{planta}/{fecha}.jpg"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data,
            ContentType="image/jpeg"
        )
    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 error {planta}: {e}")


# =========================
# Captura
# =========================

async def capturar_camara(session, planta, cam_id):
    ultimo_hash = None
    errores = 0

    while RUNNING:

        if not dentro_horario(planta):
            await asyncio.sleep(300)
            continue

        pitime = int(time.time())
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"

        try:
            async with session.get(url, params={"pitime": pitime}) as resp:
                if resp.status != 200:
                    errores += 1
                    logger.warning(f"{planta} HTTP {resp.status}")
                else:
                    data = await resp.read()
                    h = hash_imagen(data)

                    if h != ultimo_hash:
                        await subir_s3(planta, fecha, data)
                        ultimo_hash = h
                        logger.info(f"{planta} imagen nueva subida")

                    errores = 0

        except Exception as e:
            errores += 1
            logger.error(f"{planta} error: {e}")

        if errores >= 5:
            logger.error(f"{planta} deshabilitada por errores")
            break

        await asyncio.sleep(INTERVALO + (hash(planta) % 5))


# =========================
# Main
# =========================

async def main():
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=30)
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        await asyncio.gather(*[
            capturar_camara(session, planta, cam_id)
            for planta, cam_id in camaras.items()
        ])


asyncio.run(main())
logger.info("Proceso finalizado")
