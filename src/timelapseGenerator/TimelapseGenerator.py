import os
import subprocess
from pathlib import Path

def crear_timelapse(
    input_dir: str,
    output_file: str,
    fps: int = 30,
    extension: str = ".jpg"
):
    """
    Genera un timelapse a partir de imágenes en una carpeta.
    - Ignora archivos de 0 KB
    - Ordena por nombre (timestamp en filename)
    - No recomprime imágenes
    - Usa ffmpeg
    """

    input_path = Path(input_dir)
    if not input_path.exists() or not input_path.is_dir():
        raise ValueError("La carpeta de entrada no existe o no es válida")

    imagenes = sorted(
        [
            f for f in input_path.iterdir()
            if f.is_file()
            and f.suffix.lower() == extension
            and f.stat().st_size > 0
        ]
    )

    if not imagenes:
        raise RuntimeError("No hay imágenes válidas para procesar")

    # Archivo temporal con la lista de imágenes (modo concat de ffmpeg)
    lista = input_path / "imagenes.txt"

    with open(lista, "w", encoding="utf-8") as f:
        for img in imagenes:
            f.write(f"file '{img.as_posix()}'\n")

    comando = [
        "ffmpeg",
        "-y",
        "-r", str(fps),
        "-f", "concat",
        "-safe", "0",
        "-i", str(lista),
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        output_file
    ]

    subprocess.run(comando, check=True)

    lista.unlink()  # limpiar

    print(f"Timelapse creado: {output_file}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Uso: python timelapse_simple.py <carpeta_imagenes> <salida.mp4> [fps]")
        sys.exit(1)

    carpeta = sys.argv[1]
    salida = sys.argv[2]
    fps = int(sys.argv[3]) if len(sys.argv) > 3 else 30

    crear_timelapse(carpeta, salida, fps)
