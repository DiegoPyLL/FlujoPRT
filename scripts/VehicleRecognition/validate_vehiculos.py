"""
Validacion, registro vehicular y visualizacion de capturas en S3.

Recorre el prefijo S3 indicado, detecta vehiculos en cada imagen con YOLOv8,
escribe un log JSONL con metadata + conteos por tipo, y sube las imagenes
anotadas con bounding boxes directamente a S3 (sin escritura local).

Uso:
    python scripts/VehicleRecognition/validate_vehiculos.py
    python scripts/VehicleRecognition/validate_vehiculos.py --prefijo capturas/2026/04/25/
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

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image, ImageDraw, ImageFont
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(__file__))
from detector import cargar_modelo, detectar_vehiculos  # noqa: E402

# --- Configuracion por defecto ---
S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIJO = os.getenv("S3_PREFIJO", "capturas/2026/")
S3_PREFIJO_ANOTADAS = os.getenv("S3_PREFIJO_ANOTADAS", "capturas_anotadas")
S3_PREFIJO_LOGS     = os.getenv("S3_PREFIJO_LOGS", "metadata/validate_vehiculos")

MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB — minimo por parte intermedia en S3

PATRON_ARCHIVO = re.compile(r"^([A-Z]{2,3})_(\d{8})_(\d{6})\.jpg$", re.IGNORECASE)

# --- Constantes de dibujo ---
COLOR_TIPO = {
    "auto":   (0, 200, 0),
    "moto":   (0, 120, 255),
    "bus":    (255, 200, 0),
    "camion": (220, 40, 40),
}
COLOR_DEFAULT = (180, 0, 255)
GROSOR = 3
FONT_SIZE = 18


def _font():
    try:
        return ImageFont.truetype("arial.ttf", FONT_SIZE)
    except Exception:
        return ImageFont.load_default()


def configurar_logger() -> logging.Logger:
    logger = logging.getLogger("validate-vehiculos")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    if not logger.handlers:
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
    return {"planta_codigo": codigo.upper(), "fecha": fecha, "hora": hora, "timestamp_imagen": f"{fecha}T{hora}"}


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
    conteo["total"] = sum(conteo[t] for t in ("auto", "moto", "bus", "camion"))
    return conteo


def dibujar_boxes(datos_imagen: bytes, detecciones: list[dict], confianza_min: float) -> bytes:
    """Retorna los bytes JPEG de la imagen con bounding boxes dibujados."""
    with Image.open(io.BytesIO(datos_imagen)) as img:
        img = img.convert("RGB")
        draw = ImageDraw.Draw(img)
        font = _font()

        for det in detecciones:
            conf = det.get("confianza", 0)
            if conf < confianza_min:
                continue
            bbox = det.get("bbox", [])
            if len(bbox) != 4:
                continue
            x1, y1, x2, y2 = [float(v) for v in bbox]
            tipo = det.get("tipo", "?")
            color = COLOR_TIPO.get(tipo, COLOR_DEFAULT)

            for offset in range(GROSOR):
                draw.rectangle([x1 - offset, y1 - offset, x2 + offset, y2 + offset], outline=color)

            etiqueta = f"{tipo}  {conf * 100:.1f}%"
            bbox_texto = font.getbbox(etiqueta) if hasattr(font, "getbbox") else (0, 0, FONT_SIZE * len(etiqueta) // 2, FONT_SIZE)
            tw = bbox_texto[2] - bbox_texto[0]
            th = bbox_texto[3] - bbox_texto[1]
            tx, ty = int(x1), min(int(y2) + 2, img.size[1] - th - 4)
            draw.rectangle([tx, ty, tx + tw + 6, ty + th + 4], fill=color)
            draw.text((tx + 3, ty + 2), etiqueta, fill=(255, 255, 255), font=font)

        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=90)
        return buffer.getvalue()


def _clave_anotada(s3_key: str, prefijo_anotadas: str) -> str:
    """capturas/YYYY/MM/DD/Planta/img.jpg → <prefijo_anotadas>/YYYY/MM/DD/Planta/img.jpg"""
    _, resto = s3_key.split("/", 1)
    return f"{prefijo_anotadas}/{resto}"


def _subir_imagen_anotada(s3, bucket: str, datos_imagen: bytes, clave_s3: str, logger: logging.Logger) -> bool:
    try:
        s3.put_object(
            Bucket=bucket,
            Key=clave_s3,
            Body=datos_imagen,
            ContentType="image/jpeg",
            StorageClass="INTELLIGENT_TIERING",
        )
        return True
    except (BotoCoreError, ClientError) as exc:
        logger.warning("[S3] No se pudo subir imagen anotada '%s': %s", clave_s3, exc)
        return False


def _iniciar_multipart(s3, bucket: str, clave_s3: str) -> str:
    resp = s3.create_multipart_upload(
        Bucket=bucket,
        Key=clave_s3,
        ContentType="application/x-ndjson",
        StorageClass="INTELLIGENT_TIERING",
    )
    return resp["UploadId"]


def _subir_parte(s3, bucket: str, clave_s3: str, upload_id: str, numero: int, datos: bytes) -> dict:
    resp = s3.upload_part(
        Bucket=bucket,
        Key=clave_s3,
        UploadId=upload_id,
        PartNumber=numero,
        Body=datos,
    )
    return {"PartNumber": numero, "ETag": resp["ETag"]}


def _completar_multipart(s3, bucket: str, clave_s3: str, upload_id: str, partes: list[dict]) -> None:
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=clave_s3,
        UploadId=upload_id,
        MultipartUpload={"Parts": partes},
    )


def _abortar_multipart(s3, bucket: str, clave_s3: str, upload_id: str, logger: logging.Logger) -> None:
    try:
        s3.abort_multipart_upload(Bucket=bucket, Key=clave_s3, UploadId=upload_id)
    except Exception as exc:
        logger.warning("[S3] No se pudo abortar multipart '%s': %s", clave_s3, exc)


def procesar_objeto(
    modelo,
    s3_client,
    bucket: str,
    obj: dict,
    confianza_min: float,
    prefijo_anotadas: str,
    logger: logging.Logger,
) -> dict:
    """Descarga el objeto S3, detecta vehiculos, dibuja boxes y genera el registro JSONL."""
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
        "s3_key_anotada": None,
        "error": None,
    }

    try:
        respuesta = s3_client.get_object(Bucket=bucket, Key=clave)
        datos = respuesta["Body"].read()

        ancho, alto = dimensiones_desde_bytes(datos)
        registro["ancho_px"] = ancho
        registro["alto_px"] = alto

        # tempfile necesario: ultralytics requiere ruta en disco
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp.write(datos)
            ruta_tmp = tmp.name

        try:
            detecciones = detectar_vehiculos(modelo, ruta_tmp)
            registro["detecciones"] = detecciones
            registro["conteo"] = construir_conteo(detecciones)

            img_anotada_bytes = dibujar_boxes(datos, detecciones, confianza_min)
            clave_anot = _clave_anotada(clave, prefijo_anotadas)
            if _subir_imagen_anotada(s3_client, bucket, img_anotada_bytes, clave_anot, logger):
                registro["s3_key_anotada"] = clave_anot
        finally:
            os.unlink(ruta_tmp)

    except (BotoCoreError, ClientError) as exc:
        registro["error"] = f"S3: {exc}"
    except Exception as exc:
        registro["error"] = str(exc)

    return registro


def main():
    parser = argparse.ArgumentParser(description="Registro vehicular y visualizacion de capturas en S3")
    parser.add_argument("--bucket", default=S3_BUCKET, help="Nombre del bucket S3 (default: flujo-prt-imagenes)")
    parser.add_argument("--prefijo", default=S3_PREFIJO, help="Prefijo S3 a recorrer (default: capturas/2026/)")
    parser.add_argument(
        "--confianza-min",
        type=float,
        default=0.0,
        help="Umbral minimo de confianza para dibujar box (default: 0.0)",
    )
    args = parser.parse_args()

    logger = configurar_logger()
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
    logger.info("Subida S3: anotadas → %s/, JSONL → %s/", S3_PREFIJO_ANOTADAS, S3_PREFIJO_LOGS)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    clave_jsonl_s3 = f"{S3_PREFIJO_LOGS}/registro_vehicular_{ts}.jsonl"
    upload_id = _iniciar_multipart(s3, args.bucket, clave_jsonl_s3)
    logger.info("[S3] Multipart iniciado: %s", clave_jsonl_s3)

    acum = {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0, "errores": 0}
    partes: list[dict] = []
    numero_parte = 1
    buffer = io.BytesIO()

    try:
        for obj in tqdm(objetos, desc="Procesando", unit="img"):
            registro = procesar_objeto(
                modelo, s3, args.bucket, obj,
                args.confianza_min, S3_PREFIJO_ANOTADAS, logger,
            )
            buffer.write((json.dumps(registro, ensure_ascii=False) + "\n").encode("utf-8"))

            if registro["error"]:
                acum["errores"] += 1
                logger.warning("Error en %s: %s", registro["archivo"], registro["error"])
            else:
                for tipo in ("auto", "moto", "bus", "camion", "total"):
                    acum[tipo] += registro["conteo"][tipo]

            if buffer.tell() >= MULTIPART_CHUNK_SIZE:
                partes.append(_subir_parte(s3, args.bucket, clave_jsonl_s3, upload_id, numero_parte, buffer.getvalue()))
                logger.debug("[S3] Parte %d subida (%d bytes)", numero_parte, buffer.tell())
                numero_parte += 1
                buffer = io.BytesIO()

        datos_finales = buffer.getvalue()
        if datos_finales:
            partes.append(_subir_parte(s3, args.bucket, clave_jsonl_s3, upload_id, numero_parte, datos_finales))

        _completar_multipart(s3, args.bucket, clave_jsonl_s3, upload_id, partes)

    except Exception:
        _abortar_multipart(s3, args.bucket, clave_jsonl_s3, upload_id, logger)
        raise

    logger.info("=" * 50)
    logger.info("Procesamiento completado")
    logger.info("  Imagenes procesadas : %d", total)
    logger.info("  Errores             : %d", acum["errores"])
    logger.info("  Autos detectados    : %d", acum["auto"])
    logger.info("  Motos detectadas    : %d", acum["moto"])
    logger.info("  Buses detectados    : %d", acum["bus"])
    logger.info("  Camiones detectados : %d", acum["camion"])
    logger.info("  Total vehiculos     : %d", acum["total"])
    logger.info("  JSONL en S3         : s3://%s/%s (%d parte(s))", args.bucket, clave_jsonl_s3, len(partes))
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
