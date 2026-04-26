"""
Validacion y registro vehicular de capturas en S3.

Recorre el prefijo S3 indicado, detecta vehiculos en cada imagen con YOLOv8
y escribe un log JSONL con metadata + conteos por tipo.

Uso:
    python scripts/VehicleRecognition/validate_vehiculos.py
    python scripts/VehicleRecognition/validate_vehiculos.py --prefijo capturas/2026/ --salida /ruta/salida.jsonl
    python scripts/VehicleRecognition/validate_vehiculos.py --bucket otro-bucket --prefijo capturas/2025/04/
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime
from logging.handlers import RotatingFileHandler

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image
from tqdm import tqdm

# Permite importar detector.py desde el mismo directorio
sys.path.insert(0, os.path.dirname(__file__))
from detector import cargar_modelo, detectar_vehiculos  # noqa: E402

# --- Configuracion por defecto ---
S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIJO = os.getenv("S3_PREFIJO", "capturas/2026/")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_JSONL_DEFAULT = os.path.join(LOG_DIR, "registro_vehicular.jsonl")
LOG_TEXTO = os.path.join(LOG_DIR, "validate_vehiculos.log")

PATRON_ARCHIVO = re.compile(r"^([A-Z]{2,3})_(\d{8})_(\d{6})\.jpg$", re.IGNORECASE)


def configurar_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("validate-vehiculos")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = RotatingFileHandler(LOG_TEXTO, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def listar_objetos_s3(s3_client, bucket: str, prefijo: str) -> list[dict]:
    """Lista todos los objetos JPG bajo el prefijo S3 que coincidan con el patron."""
    objetos = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=bucket, Prefix=prefijo):
        for obj in pagina.get("Contents", []):
            clave = obj["Key"]
            nombre = clave.rsplit("/", 1)[-1]
            if PATRON_ARCHIVO.match(nombre):
                objetos.append({"clave": clave, "tamano": obj["Size"]})
    objetos.sort(key=lambda o: o["clave"])
    return objetos


def parsear_nombre(nombre: str) -> dict:
    """Extrae planta, fecha y hora del nombre de archivo."""
    m = PATRON_ARCHIVO.match(nombre)
    if not m:
        return {}
    codigo, fecha_str, hora_str = m.group(1), m.group(2), m.group(3)
    fecha = f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:]}"
    hora = f"{hora_str[:2]}:{hora_str[2:4]}:{hora_str[4:]}"
    timestamp = f"{fecha}T{hora}"
    return {"planta_codigo": codigo.upper(), "fecha": fecha, "hora": hora, "timestamp_imagen": timestamp}


def dimensiones_desde_bytes(datos: bytes) -> tuple[int, int]:
    try:
        with Image.open(io.BytesIO(datos)) as img:
            return img.size
    except Exception:
        return 0, 0


def construir_conteo(detecciones: list[dict]) -> dict:
    conteo = {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0}
    for d in detecciones:
        tipo = d.get("tipo")
        if tipo in conteo:
            conteo[tipo] += 1
    conteo["total"] = conteo["auto"] + conteo["moto"] + conteo["bus"] + conteo["camion"]
    return conteo


def procesar_objeto(modelo, s3_client, bucket: str, obj: dict) -> dict:
    """Descarga el objeto S3 y genera el registro JSONL."""
    clave = obj["clave"]
    nombre = clave.rsplit("/", 1)[-1]
    meta = parsear_nombre(nombre)

    registro = {
        "archivo": nombre,
        "s3_key": clave,
        **meta,
        "bytes_archivo": obj["tamano"],
        "ancho_px": 0,
        "alto_px": 0,
        "conteo": {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0},
        "detecciones": [],
        "procesado_en": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "error": None,
    }

    try:
        respuesta = s3_client.get_object(Bucket=bucket, Key=clave)
        datos = respuesta["Body"].read()

        ancho, alto = dimensiones_desde_bytes(datos)
        registro["ancho_px"] = ancho
        registro["alto_px"] = alto

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp.write(datos)
            ruta_tmp = tmp.name

        try:
            detecciones = detectar_vehiculos(modelo, ruta_tmp)
            registro["detecciones"] = detecciones
            registro["conteo"] = construir_conteo(detecciones)
        finally:
            os.unlink(ruta_tmp)

    except (BotoCoreError, ClientError) as exc:
        registro["error"] = f"S3: {exc}"
    except Exception as exc:
        registro["error"] = str(exc)

    return registro


def main():
    parser = argparse.ArgumentParser(description="Registro vehicular de capturas en S3")
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help="Nombre del bucket S3 (default: flujo-prt-imagenes)",
    )
    parser.add_argument(
        "--prefijo",
        default=S3_PREFIJO,
        help="Prefijo S3 a recorrer (default: capturas/2026/)",
    )
    parser.add_argument(
        "--salida",
        default=LOG_JSONL_DEFAULT,
        help="Ruta del archivo JSONL de salida",
    )
    args = parser.parse_args()

    logger = configurar_logger()
    salida = os.path.normpath(args.salida)
    os.makedirs(os.path.dirname(salida), exist_ok=True)

    s3 = boto3.client("s3")

    logger.info("Listando objetos en s3://%s/%s ...", args.bucket, args.prefijo)
    try:
        objetos = listar_objetos_s3(s3, args.bucket, args.prefijo)
    except (BotoCoreError, ClientError) as exc:
        logger.error("No se pudo listar S3: %s", exc)
        sys.exit(1)

    total = len(objetos)
    if total == 0:
        logger.warning("No se encontraron imagenes con el patron esperado en el prefijo.")
        sys.exit(0)

    logger.info("Imagenes encontradas: %d", total)
    logger.info("Cargando modelo YOLOv8...")
    modelo = cargar_modelo()

    logger.info("Escribiendo log en: %s", salida)

    acum = {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0, "errores": 0}

    with open(salida, "w", encoding="utf-8") as f_out:
        for obj in tqdm(objetos, desc="Procesando", unit="img"):
            registro = procesar_objeto(modelo, s3, args.bucket, obj)
            f_out.write(json.dumps(registro, ensure_ascii=False) + "\n")

            if registro["error"]:
                acum["errores"] += 1
                logger.warning("Error en %s: %s", registro["archivo"], registro["error"])
            else:
                for tipo in ("auto", "moto", "bus", "camion", "total"):
                    acum[tipo] += registro["conteo"][tipo]

    logger.info("=" * 50)
    logger.info("Procesamiento completado")
    logger.info("  Imagenes procesadas : %d", total)
    logger.info("  Errores             : %d", acum["errores"])
    logger.info("  Autos detectados    : %d", acum["auto"])
    logger.info("  Motos detectadas    : %d", acum["moto"])
    logger.info("  Buses detectados    : %d", acum["bus"])
    logger.info("  Camiones detectados : %d", acum["camion"])
    logger.info("  Total vehiculos     : %d", acum["total"])
    logger.info("  Log JSONL guardado  : %s", salida)
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
