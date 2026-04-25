"""
Deteccion de vehiculos en imagenes CCTV usando YOLOv8

Detecta multiples vehiculos en una imagen y retorna bounding boxes con tipo y confianza.
El modelo yolov8s.pt se descarga automaticamente la primera vez (~22 MB).
"""

from ultralytics import YOLO

# Subconjunto de clases COCO relevantes para vehiculos
CLASES_VEHICULO = {2: "auto", 3: "moto", 5: "bus", 7: "camion"}

UMBRAL_CONFIANZA = 0.4


def cargar_modelo(ruta_pesos="yolov8s.pt") -> YOLO:
    """
    Carga el modelo YOLO desde disco o lo descarga si no existe.

    Args:
        ruta_pesos (str): Ruta o nombre del archivo de pesos, por defecto 'yolov8s.pt'

    Returns:
        YOLO: Modelo listo para inferencia
    """
    return YOLO(ruta_pesos)


def detectar_vehiculos(modelo: YOLO, imagen_path: str) -> list[dict]:
    """
    Detecta todos los vehiculos en una imagen CCTV.

    Args:
        modelo: Modelo YOLO cargado
        imagen_path (str): Ruta local a la imagen

    Returns:
        Lista de detecciones, cada una con:
            - bbox (list[float]): coordenadas [x1, y1, x2, y2] en pixeles
            - tipo (str): "auto", "moto", "bus" o "camion"
            - confianza (float): score de confianza entre 0 y 1
    """
    resultados = modelo(imagen_path, conf=UMBRAL_CONFIANZA, verbose=False)
    detecciones = []
    for r in resultados:
        for box in r.boxes:
            clase_id = int(box.cls)
            if clase_id in CLASES_VEHICULO:
                detecciones.append({
                    "bbox": box.xyxy[0].tolist(),
                    "tipo": CLASES_VEHICULO[clase_id],
                    "confianza": float(box.conf),
                })
    return detecciones


def main():
    """Prueba rapida de deteccion sobre las imagenes de ejemplo."""
    import os

    modelo = cargar_modelo()

    imagenes_prueba = [
        "./car_img/car_1.png",
        "./car_img/car_2.png",
        "./car_img/car_3.png",
    ]

    for imagen_path in imagenes_prueba:
        if not os.path.exists(imagen_path):
            print(f"Imagen no encontrada: {imagen_path}")
            continue

        print(f"\nProcesando: {imagen_path}")
        detecciones = detectar_vehiculos(modelo, imagen_path)

        if not detecciones:
            print("  Sin vehiculos detectados.")
        else:
            print(f"  {len(detecciones)} vehiculo(s) detectado(s):")
            for i, d in enumerate(detecciones, 1):
                x1, y1, x2, y2 = [round(v) for v in d["bbox"]]
                print(f"  [{i}] {d['tipo']} | confianza: {d['confianza']:.2f} | bbox: ({x1},{y1})-({x2},{y2})")


if __name__ == "__main__":
    main()
