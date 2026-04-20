"""
Validador automático de metadata JSON subida a S3.
Corre en loop cada INTERVAL segundos validando los JSONs del día actual.
Los errores quedan en logs/validation_errors.log.
"""

import argparse
import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

VALID_PLANTA_IDS = {
    "HCH", "LFL", "LPT", "PUD", "QLC", "RCL",
    "SJQ", "TMU", "VLL", "CHL", "YGY", "CCP", "SPP", "YMB"
}

FECHA_STR_RE = re.compile(r"^\d{8}_\d{6}$")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")


def _setup_logging() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, "validation_errors.log")

    logger = logging.getLogger("validate-metadata")
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = _setup_logging()

# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------

def _iso_parseable(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
        return True
    except (ValueError, TypeError):
        return False


def validar_registro(key: str, data: dict) -> list[str]:
    errores: list[str] = []

    def campo_ausente(campo: str) -> bool:
        if campo not in data:
            errores.append(f"[INVALID] {key} → campo '{campo}' ausente")
            return True
        return False

    # version
    if not campo_ausente("version"):
        if data["version"] != "1":
            errores.append(f"[INVALID] {key} → 'version' debe ser \"1\", recibido: {data['version']!r}")

    # planta_id
    if not campo_ausente("planta_id"):
        if not isinstance(data["planta_id"], str) or not data["planta_id"].strip():
            errores.append(f"[INVALID] {key} → 'planta_id' debe ser str no vacío, recibido: {data['planta_id']!r}")
        elif data["planta_id"] not in VALID_PLANTA_IDS:
            errores.append(f"[INVALID] {key} → 'planta_id' valor desconocido: {data['planta_id']!r}")

    # planta_nombre
    if not campo_ausente("planta_nombre"):
        if not isinstance(data["planta_nombre"], str) or not data["planta_nombre"].strip():
            errores.append(f"[INVALID] {key} → 'planta_nombre' debe ser str no vacío, recibido: {data['planta_nombre']!r}")

    # plataforma
    if not campo_ausente("plataforma"):
        if not isinstance(data["plataforma"], str) or not data["plataforma"].strip():
            errores.append(f"[INVALID] {key} → 'plataforma' debe ser str no vacío, recibido: {data['plataforma']!r}")

    # timestamp_captura
    if not campo_ausente("timestamp_captura"):
        if not isinstance(data["timestamp_captura"], str) or not _iso_parseable(data["timestamp_captura"]):
            errores.append(f"[INVALID] {key} → 'timestamp_captura' no es ISO 8601 válido, recibido: {data['timestamp_captura']!r}")

    # fecha_str
    if not campo_ausente("fecha_str"):
        if not isinstance(data["fecha_str"], str) or not FECHA_STR_RE.match(data["fecha_str"]):
            errores.append(f"[INVALID] {key} → 'fecha_str' debe tener formato YYYYMMDD_HHMMSS, recibido: {data['fecha_str']!r}")

    # s3_imagen_key
    if not campo_ausente("s3_imagen_key"):
        if not isinstance(data["s3_imagen_key"], str) or not data["s3_imagen_key"].endswith(".jpg"):
            errores.append(f"[INVALID] {key} → 's3_imagen_key' debe terminar en .jpg, recibido: {data['s3_imagen_key']!r}")

    # s3_bucket
    if not campo_ausente("s3_bucket"):
        if not isinstance(data["s3_bucket"], str) or not data["s3_bucket"].strip():
            errores.append(f"[INVALID] {key} → 's3_bucket' debe ser str no vacío, recibido: {data['s3_bucket']!r}")

    # bytes_originales
    if not campo_ausente("bytes_originales"):
        if not isinstance(data["bytes_originales"], int) or data["bytes_originales"] < 0:
            errores.append(f"[INVALID] {key} → 'bytes_originales' debe ser int >= 0, recibido: {data['bytes_originales']!r}")

    # bytes_comprimidos
    if not campo_ausente("bytes_comprimidos"):
        if not isinstance(data["bytes_comprimidos"], int) or data["bytes_comprimidos"] < 0:
            errores.append(f"[INVALID] {key} → 'bytes_comprimidos' debe ser int >= 0, recibido: {data['bytes_comprimidos']!r}")

    # ratio_compresion (puede ser null)
    if not campo_ausente("ratio_compresion"):
        val = data["ratio_compresion"]
        if val is not None:
            if not isinstance(val, (int, float)) or val <= 0:
                errores.append(f"[INVALID] {key} → 'ratio_compresion' debe ser float > 0 o null, recibido: {val!r}")

    # instancia_ec2 (puede ser null)
    if not campo_ausente("instancia_ec2"):
        val = data["instancia_ec2"]
        if val is not None:
            if not isinstance(val, dict):
                errores.append(f"[INVALID] {key} → 'instancia_ec2' debe ser dict o null, recibido: {type(val).__name__}")
            else:
                for sub in ("instance_id", "instance_type"):
                    if sub not in val:
                        errores.append(f"[INVALID] {key} → 'instancia_ec2.{sub}' ausente")

    # generado_en
    if not campo_ausente("generado_en"):
        if not isinstance(data["generado_en"], str) or not _iso_parseable(data["generado_en"]):
            errores.append(f"[INVALID] {key} → 'generado_en' no es ISO 8601 válido, recibido: {data['generado_en']!r}")

    return errores


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def listar_keys_hoy(s3, bucket: str, prefix: str) -> list[str]:
    hoy = datetime.now().strftime("%Y/%m/%d")
    full_prefix = f"{prefix}/{hoy}/"
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json"):
                keys.append(k)
    return keys


def descargar_json(s3, bucket: str, key: str) -> dict | None:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except (BotoCoreError, ClientError) as e:
        logger.warning(f"[S3-ERROR] No se pudo descargar {key}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"[INVALID] {key} → JSON malformado: {e}")
        return None


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

RUNNING = True


def _handle_signal(signum, frame):
    global RUNNING
    logger.info("Señal recibida, deteniendo validador...")
    RUNNING = False


def run_ciclo(s3, bucket: str, prefix: str) -> None:
    total = ok = errores_count = 0
    keys = listar_keys_hoy(s3, bucket, prefix)
    logger.info(f"Ciclo iniciado: {len(keys)} JSONs encontrados para hoy")

    for key in keys:
        data = descargar_json(s3, bucket, key)
        if data is None:
            errores_count += 1
            total += 1
            continue

        errores = validar_registro(key, data)
        total += 1
        if errores:
            errores_count += len(errores)
            for msg in errores:
                logger.warning(msg)
        else:
            ok += 1

    logger.info(f"Ciclo completado — Validados: {total} | OK: {ok} | Errores: {errores_count}")


def main():
    parser = argparse.ArgumentParser(description="Validador automático de metadata CCTV en S3")
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", "flujo-prt-imagenes"))
    parser.add_argument("--prefix", default=os.getenv("METADATA_PREFIX", "metadata/capturas"))
    parser.add_argument("--interval", type=int, default=600, help="Segundos entre ciclos (default: 600)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("=" * 60)
    logger.info("VALIDADOR METADATA S3 — INICIO")
    logger.info(f"Bucket: {args.bucket} | Prefix: {args.prefix} | Intervalo: {args.interval}s")
    logger.info("=" * 60)

    s3 = boto3.client("s3")

    while RUNNING:
        try:
            run_ciclo(s3, args.bucket, args.prefix)
        except Exception as e:
            logger.error(f"Error inesperado en ciclo: {e}", exc_info=True)

        if RUNNING:
            logger.info(f"Próximo ciclo en {args.interval}s...")
            time.sleep(args.interval)

    logger.info("Validador detenido.")


if __name__ == "__main__":
    main()
