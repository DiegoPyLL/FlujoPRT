"""
Validacion y registro vehicular de capturas locales.

Recorre la carpeta de capturas, detecta vehiculos en cada imagen con YOLOv8
y escribe un log JSONL con metadata + conteos por tipo.

Uso:
    python scripts/VehicleRecognition/validate_vehiculos.py
    python scripts/VehicleRecognition/validate_vehiculos.py --carpeta /ruta/alternativa --salida /ruta/salida.jsonl
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

from PIL import Image
from tqdm import tqdm

# Permite importar detector.py desde el mismo directorio
sys.path.insert(0, os.path.dirname(__file__))
from detector import cargar_modelo, detectar_vehiculos  # noqa: E402

# --- Configuracion por defecto ---
CARPETA_CAPTURAS = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "Capturas"
)
# Base para rutas relativas: directorio padre de FlujoPRT_main
BASE_RELATIVA = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
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


def recolectar_imagenes(carpeta: str) -> list[str]:
    """Recorre recursivamente la carpeta y retorna rutas de imagenes validas."""
    rutas = []
    for raiz, _, archivos in os.walk(carpeta):
        for nombre in sorted(archivos):
            if PATRON_ARCHIVO.match(nombre):
                rutas.append(os.path.join(raiz, nombre))
    return rutas


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


def dimensiones_imagen(ruta: str) -> tuple[int, int]:
    """Retorna (ancho, alto) en pixeles. Devuelve (0, 0) si falla."""
    try:
        with Image.open(ruta) as img:
            return img.size  # (width, height)
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


def procesar_imagen(modelo, ruta: str) -> dict:
    """Genera el registro JSONL para una imagen."""
    nombre = os.path.basename(ruta)
    meta = parsear_nombre(nombre)
    ancho, alto = dimensiones_imagen(ruta)
    registro = {
        "archivo": nombre,
        "ruta_absoluta": os.path.relpath(os.path.abspath(ruta), BASE_RELATIVA).replace("\\", "/"),
        **meta,
        "bytes_archivo": os.path.getsize(ruta),
        "ancho_px": ancho,
        "alto_px": alto,
        "conteo": {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0},
        "detecciones": [],
        "procesado_en": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "error": None,
    }
    try:
        detecciones = detectar_vehiculos(modelo, ruta)
        registro["detecciones"] = detecciones
        registro["conteo"] = construir_conteo(detecciones)
    except Exception as exc:
        registro["error"] = str(exc)
    return registro


def main():
    parser = argparse.ArgumentParser(description="Registro vehicular de capturas locales")
    parser.add_argument(
        "--carpeta",
        default=CARPETA_CAPTURAS,
        help="Carpeta raiz de capturas a procesar",
    )
    parser.add_argument(
        "--salida",
        default=LOG_JSONL_DEFAULT,
        help="Ruta del archivo JSONL de salida",
    )
    args = parser.parse_args()

    logger = configurar_logger()
    carpeta = os.path.normpath(args.carpeta)
    salida = os.path.normpath(args.salida)

    if not os.path.isdir(carpeta):
        logger.error("Carpeta no encontrada: %s", carpeta)
        sys.exit(1)

    os.makedirs(os.path.dirname(salida), exist_ok=True)

    logger.info("Cargando modelo YOLOv8...")
    modelo = cargar_modelo()

    logger.info("Buscando imagenes en: %s", carpeta)
    imagenes = recolectar_imagenes(carpeta)
    total = len(imagenes)

    if total == 0:
        logger.warning("No se encontraron imagenes con el patron esperado.")
        sys.exit(0)

    logger.info("Imagenes encontradas: %d", total)
    logger.info("Escribiendo log en: %s", salida)

    acum = {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0, "errores": 0}

    with open(salida, "w", encoding="utf-8") as f_out:
        for ruta in tqdm(imagenes, desc="Procesando", unit="img"):
            registro = procesar_imagen(modelo, ruta)
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
