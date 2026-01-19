from pathlib import Path
from PIL import Image


EXTENSIONES_VALIDAS = {".jpg"}


def es_archivo_valido(archivo: Path) -> bool:
    """
    Valida si un archivo es procesable como imagen válida.
    Reglas:
    - Debe ser un archivo (no carpeta)
    - No debe pesar 0 KB
    - Debe tener una extensión permitida
    """
    if not archivo.is_file():
        return False

    if archivo.stat().st_size == 0:
        return False

    if archivo.suffix.lower() not in EXTENSIONES_VALIDAS:
        return False

    return True


def obtener_resolucion(archivo: Path):
    """
    Obtiene la resolución de la imagen sin modificarla.
    Retorna una tupla (ancho, alto).
    """
    with Image.open(archivo) as img:
        return img.size  # (width, height)


# Las imágenes provenientes de la nube vienen comprimidas (75%).
# Esta función las reescribe sin compresión perceptual.
def descomprimir_imagen(archivo: Path):
    """
    Reescribe la imagen eliminando compresión:
    - quality=100
    - sin optimización
    - sin subsampling
    El archivo original se sobrescribe.
    """
    with Image.open(archivo) as img:
        img = img.convert("RGB")
        img.save(
            archivo,
            format="JPEG",
            quality=100,
            optimize=False,
            subsampling=0
        )


def limpiar_archivo(archivo: Path):
    """
    Elimina el archivo del sistema.
    missing_ok evita errores si el archivo ya no existe.
    """
    archivo.unlink(missing_ok=True)


def procesar_imagen(
    archivo: Path,
    resolucion_referencia
):
    """
    Procesa una imagen individual.
    Flujo:
    - Obtiene resolución
    - Si no hay referencia, la fija
    - Si la resolución no coincide, elimina la imagen
    - Si es válida, la descomprime
    Retorna siempre la resolución de referencia vigente.
    """
    try:
        resolucion = obtener_resolucion(archivo)

        if resolucion_referencia is None:
            # Primera imagen válida define la resolución del lote
            resolucion_referencia = resolucion

        elif resolucion != resolucion_referencia:
            # Resolución distinta → archivo descartado
            limpiar_archivo(archivo)
            return resolucion_referencia

        # Imagen válida y consistente → se descomprime
        descomprimir_imagen(archivo)
        return resolucion_referencia

    except Exception:
        # Cualquier error implica archivo corrupto o inválido
        limpiar_archivo(archivo)
        return resolucion_referencia


def ejecucion(carpeta: str):
    """
    Punto de entrada principal.
    Recorre una carpeta plana de imágenes y aplica:
    - limpieza
    - validación
    - homogeneización de resolución
    - descompresión
    """
    base = Path(carpeta)

    if not base.exists() or not base.is_dir():
        raise ValueError("La carpeta no existe o no es válida")

    resolucion_referencia = None

    for archivo in sorted(base.iterdir()):

        # Archivos inválidos se eliminan directamente
        if not es_archivo_valido(archivo):
            limpiar_archivo(archivo)
            continue

        # Procesamiento controlado de la imagen
        resolucion_referencia = procesar_imagen(
            archivo,
            resolucion_referencia
        )


if __name__ == "__main__":
    import sys

    # Se espera exactamente un argumento: la carpeta a procesar
    if len(sys.argv) != 2:
        print("Uso: python CloudDecompressor.py <carpeta>")
        exit(1)

    ejecucion(sys.argv[1])
