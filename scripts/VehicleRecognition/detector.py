"""
Deteccion de vehiculos en imagenes CCTV usando YOLOv8

Detecta multiples vehiculos en una imagen y retorna bounding boxes con tipo y confianza.
El modelo yolov8s.pt se descarga automaticamente la primera vez (~22 MB).
"""

from ultralytics import YOLO

# Subconjunto de clases COCO relevantes para vehiculos
CLASES_VEHICULO = {2: "auto", 3: "moto", 5: "bus", 7: "camion"}

UMBRAL_CONFIANZA = 0.55


def cargar_modelo(ruta_pesos="yolov8m.pt") -> YOLO:
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
