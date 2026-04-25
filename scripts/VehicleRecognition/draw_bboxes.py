"""
Dibuja bounding boxes sobre las imagenes usando los registros del JSONL.

- Guarda las imagenes anotadas en --destino, espejando la estructura de subcarpetas.
- Las imagenes originales NO se modifican.
- Elimina del JSONL las entradas cuya imagen no exista en disco.

Uso:
    python scripts/draw_bboxes.py
    python scripts/draw_bboxes.py --jsonl scripts/logs/registro_vehicular_2026_02.jsonl
    python scripts/draw_bboxes.py --jsonl ... --confianza-min 0.5 --destino /ruta/salida
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
# Base para resolver rutas relativas almacenadas en el JSONL
BASE_RELATIVA = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
LOG_JSONL_DEFAULT = os.path.join(LOG_DIR, "registro_vehicular_2026_02.jsonl")

DESTINO_DEFAULT = str(Path(__file__).parents[3] / "Resultados Captura")

# Color por tipo de vehiculo (RGB)
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
    """Carga fuente truetype si esta disponible, si no usa la por defecto."""
    try:
        return ImageFont.truetype("arial.ttf", FONT_SIZE)
    except Exception:
        return ImageFont.load_default()


def ruta_destino(ruta_origen: str, carpeta_destino: str) -> str:
    """
    Construye la ruta de salida espejando la subestructura a partir del
    segmento 'Capturas' (o el nombre de la carpeta raiz de origen).
    Ej.: .../Capturas/2026/02/04/Recoleta/img.jpg
         -> destino/2026/02/04/Recoleta/img.jpg
    """
    partes = Path(ruta_origen).parts
    try:
        idx = next(i for i, p in enumerate(partes) if p.lower() == "capturas")
        relativa = os.path.join(*partes[idx + 1:])
    except StopIteration:
        # Si no hay segmento 'Capturas', usar solo nombre de archivo
        relativa = os.path.basename(ruta_origen)
    return os.path.join(carpeta_destino, relativa)


def dibujar_boxes(ruta_origen: str, ruta_salida: str, detecciones: list[dict], confianza_min: float) -> None:
    """Guarda en ruta_salida la imagen con los bounding boxes dibujados."""
    with Image.open(ruta_origen) as img:
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

            # Marco
            for offset in range(GROSOR):
                draw.rectangle(
                    [x1 - offset, y1 - offset, x2 + offset, y2 + offset],
                    outline=color,
                )

            # Etiqueta con fondo debajo del box
            etiqueta = f"{tipo}  {conf * 100:.1f}%"
            bbox_texto = font.getbbox(etiqueta) if hasattr(font, "getbbox") else (0, 0, FONT_SIZE * len(etiqueta) // 2, FONT_SIZE)
            tw = bbox_texto[2] - bbox_texto[0]
            th = bbox_texto[3] - bbox_texto[1]
            img_h = img.size[1]
            tx = int(x1)
            ty = min(int(y2) + 2, img_h - th - 4)
            draw.rectangle([tx, ty, tx + tw + 6, ty + th + 4], fill=color)
            draw.text((tx + 3, ty + 2), etiqueta, fill=(255, 255, 255), font=font)

        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        img.save(ruta_salida, format="JPEG", quality=90)


def main():
    parser = argparse.ArgumentParser(description="Dibuja bboxes en imagenes y limpia JSONL")
    parser.add_argument("--jsonl", default=LOG_JSONL_DEFAULT, help="Ruta al archivo JSONL")
    parser.add_argument(
        "--confianza-min",
        type=float,
        default=0.0,
        help="Umbral minimo de confianza para dibujar un box (default: 0.0)",
    )
    parser.add_argument(
        "--destino",
        default=DESTINO_DEFAULT,
        help="Carpeta raiz donde guardar las imagenes anotadas",
    )
    args = parser.parse_args()

    jsonl_path = os.path.normpath(args.jsonl)
    if not os.path.isfile(jsonl_path):
        print(f"[ERROR] No se encontro el JSONL: {jsonl_path}")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("draw-bboxes")

    with open(jsonl_path, encoding="utf-8") as f:
        lineas = f.readlines()

    total = len(lineas)
    procesadas = 0
    omitidas = 0
    registros_validos = []

    for i, linea in enumerate(lineas, 1):
        linea = linea.strip()
        if not linea:
            continue

        try:
            reg = json.loads(linea)
        except json.JSONDecodeError as exc:
            log.warning("Linea %d: JSON invalido (%s), omitida.", i, exc)
            omitidas += 1
            continue

        ruta_raw = reg.get("ruta_absoluta", "")
        if not ruta_raw:
            log.warning("Imagen no disponible, eliminando del JSONL: (sin ruta)")
            omitidas += 1
            continue
        ruta = ruta_raw if os.path.isabs(ruta_raw) else os.path.join(BASE_RELATIVA, ruta_raw)
        ruta = os.path.normpath(ruta)
        if not os.path.isfile(ruta):
            log.warning("Imagen no disponible, eliminando del JSONL: %s", ruta or "(sin ruta)")
            omitidas += 1
            continue

        detecciones = reg.get("detecciones") or []
        try:
            salida = ruta_destino(ruta, args.destino)
            dibujar_boxes(ruta, salida, detecciones, args.confianza_min)
            registros_validos.append(reg)
            procesadas += 1
            if procesadas % 100 == 0:
                log.info("  %d / %d imagenes procesadas...", procesadas, total - omitidas)
        except Exception as exc:
            log.error("Error al dibujar en %s: %s", ruta, exc)
            # Mantener el registro aunque falle el dibujo
            registros_validos.append(reg)
            omitidas += 1

    # Reescribir el JSONL sin las entradas de imagenes faltantes
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for reg in registros_validos:
            f.write(json.dumps(reg, ensure_ascii=False) + "\n")

    os.makedirs(args.destino, exist_ok=True)

    log.info("=" * 50)
    log.info("Completado")
    log.info("  Total lineas leidas : %d", total)
    log.info("  Imagenes procesadas : %d", procesadas)
    log.info("  Entradas eliminadas : %d", omitidas)
    log.info("  Imagenes guardadas en: %s", args.destino)
    log.info("  JSONL actualizado   : %s", jsonl_path)
    log.info("=" * 50)


if __name__ == "__main__":
    main()
