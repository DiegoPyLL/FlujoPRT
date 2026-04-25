"""
Demo visual de deteccion de vehiculos

Dibuja bounding boxes sobre la imagen original y guarda el resultado.
Util para validar que el modelo detecta correctamente en imagenes CCTV reales.

Uso:
    python demo_detector.py <imagen>
    python demo_detector.py                  # usa car_img/car_1.png por defecto
"""

import sys
import os
import cv2
import numpy as np
from scripts.VehicleRecognition.detector import cargar_modelo, detectar_vehiculos

# Colores por tipo de vehiculo (BGR)
COLORES = {
    "auto": (0, 255, 0),
    "moto": (255, 165, 0),
    "bus": (0, 165, 255),
    "camion": (0, 0, 255),
}


def dibujar_detecciones(imagen: np.ndarray, detecciones: list[dict]) -> np.ndarray:
    """
    Dibuja bounding boxes y etiquetas sobre la imagen.

    Args:
        imagen: Array BGR de OpenCV
        detecciones: Lista de detecciones de detectar_vehiculos()

    Returns:
        Imagen con anotaciones dibujadas
    """
    resultado = imagen.copy()
    for d in detecciones:
        x1, y1, x2, y2 = [int(v) for v in d["bbox"]]
        tipo = d["tipo"]
        confianza = d["confianza"]
        color = COLORES.get(tipo, (200, 200, 200))

        cv2.rectangle(resultado, (x1, y1), (x2, y2), color, 2)

        etiqueta = f"{tipo} {confianza:.2f}"
        (ancho_texto, alto_texto), _ = cv2.getTextSize(
            etiqueta, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 2
        )
        cv2.rectangle(resultado, (x1, y1 - alto_texto - 8), (x1 + ancho_texto + 4, y1), color, -1)
        cv2.putText(
            resultado, etiqueta, (x1 + 2, y1 - 4),
            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 0), 2
        )

    return resultado


def main():
    imagen_path = sys.argv[1] if len(sys.argv) > 1 else "./car_img/car_1.png"

    if not os.path.exists(imagen_path):
        print(f"Error: archivo no encontrado: {imagen_path}")
        sys.exit(1)

    modelo = cargar_modelo()
    detecciones = detectar_vehiculos(modelo, imagen_path)

    imagen = cv2.imread(imagen_path)
    if imagen is None:
        print(f"Error: no se pudo leer la imagen: {imagen_path}")
        sys.exit(1)

    print(f"Imagen: {imagen_path}")
    print(f"Vehiculos detectados: {len(detecciones)}")
    for i, d in enumerate(detecciones, 1):
        x1, y1, x2, y2 = [round(v) for v in d["bbox"]]
        print(f"  [{i}] {d['tipo']} | confianza: {d['confianza']:.2f} | bbox: ({x1},{y1})-({x2},{y2})")

    imagen_anotada = dibujar_detecciones(imagen, detecciones)

    nombre, ext = os.path.splitext(imagen_path)
    ruta_salida = f"{nombre}_deteccion{ext}"
    cv2.imwrite(ruta_salida, imagen_anotada)
    print(f"\nImagen guardada en: {ruta_salida}")

    cv2.imshow("Deteccion de Vehiculos", imagen_anotada)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
