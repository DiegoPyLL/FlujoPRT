from pathlib import Path
from PIL import Image
from datetime import datetime
import hashlib


#Usamos el BASE_DIR para comprarar los archivos locales
from ImageRecompilerLocal import BASE_DIR 


#En caso de querer comparar los archivos descargados de la MV redefinimos el BASE_DIR
#BASE_DIR = BASE_DIR / "downloaded_from_mv"



#vemos primero si hay archivos basura (0 KB) y los eliminamos
def es_basura(archivo: Path) -> bool:
    return archivo.is_file() and archivo.stat().st_size == 0


for archivo in BASE_DIR.rglob("*"):
    if es_basura(archivo):
        archivo.unlink()
        print(f"Archivo eliminado (0 KB): {archivo}")


def son_duplicados(archivo1: Path, archivo2: Path) -> bool:
    return archivo1.is_file() and archivo2.is_file() and archivo1.stat().st_size == archivo2.stat().st_size





#ahora vemos los archivos duplicados viendo su tamaño y aspectos de la metadata usando un hash binario,
# este compara y responde a la pregunta ¿es el mismo archivo?

def hash_binario(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for bloque in iter(lambda: f.read(8192), b""):
            h.update(bloque)
    return h.hexdigest()


def fecha_desde_nombre(path: Path) -> datetime | None:
    try:
        nombre = path.stem
        fecha_str = nombre.split("_", 1)[1]
        return datetime.strptime(fecha_str, "%Y%m%d_%H%M%S")
    except Exception:
        return None


for year_dir in BASE_DIR.iterdir():
    if not year_dir.is_dir():
        continue

    for month_dir in year_dir.iterdir():
        if not month_dir.is_dir():
            continue

        for day_dir in month_dir.iterdir():
            if not day_dir.is_dir():
                continue

            for planta_dir in day_dir.iterdir():
                if not planta_dir.is_dir():
                    continue

                imagenes = sorted(planta_dir.glob("*.jpg"))
                if len(imagenes) < 2:
                    continue

                prev_path = None
                prev_fecha = None
                prev_size = None
                prev_hash = None

                for path in imagenes:
                    fecha = fecha_desde_nombre(path)
                    if not fecha:
                        prev_path = None
                        prev_fecha = None
                        prev_size = None
                        prev_hash = None
                        continue

                    try:
                        with Image.open(path) as img:
                            size = img.size
                    except Exception:
                        continue

                    if prev_fecha and prev_size:
                        mismo_minuto = abs((fecha - prev_fecha).total_seconds()) < 60
                        mismo_tamano = size == prev_size

                        if mismo_minuto and mismo_tamano:
                            h = hash_binario(path)

                            if prev_hash is None:
                                prev_hash = hash_binario(prev_path)

                            if h == prev_hash:
                                path.unlink()
                                print(f"Duplicada eliminada (hash): {path}")
                                continue

                    prev_path = path
                    prev_fecha = fecha
                    prev_size = size
                    prev_hash = None
