Proyecto: Análisis de Flujo Vehicular con Cámaras TÜV Rheinland
Descripción General

Este proyecto automatiza la captura, almacenamiento y análisis de imágenes provenientes de las cámaras públicas de las plantas de revisión técnica asociadas a TÜV Rheinland Chile. El objetivo es generar un timelapse diario y aplicar reconocimiento vehicular automatizado para estimar el flujo vehicular por planta, día y hora.

Componentes Principales

Captura de Imágenes en Vivo:
Se accede a las cámaras disponibles en el dominio pti-cameras.cl.tuv.com. Cada imagen es solicitada mediante una URL con un parámetro de tiempo (pitime o timestamp Unix) para evitar caché y reflejar la actualización más reciente.

Almacenamiento Local y Timelapse:
Las imágenes se guardan con su timestamp y se convierten en secuencias de video usando ffmpeg. Cada video representa un intervalo de tiempo determinado (por ejemplo, un turno completo o un día de operación).

Análisis de Flujo Vehicular:
Los videos generados se procesan con un modelo de reconocimiento vehicular basado en visión por computador (por ejemplo, YOLOv8, OpenCV o TensorFlow Object Detection). El sistema cuenta los vehículos detectados por cuadro y asocia la información a su timestamp, construyendo un registro de tráfico por franja horaria.

Flujo de Ejecución

Descarga periódica: se ejecuta un script Python que descarga imágenes cada cierto intervalo (por defecto, cada 30 segundos).

Generación del timelapse: al completar el ciclo de captura, se usa ffmpeg para unir las imágenes en un video MP4.

Procesamiento por IA: un módulo de reconocimiento vehicular detecta y clasifica los vehículos presentes en cada cuadro del video.

Almacenamiento y análisis: los resultados se almacenan en una base de datos o CSV para análisis posterior, permitiendo generar métricas como flujo promedio por hora, picos de tráfico y variaciones diarias.

Dependencias

Python 3.10+

Requests

OpenCV

ffmpeg

NumPy

(Opcional) PyTorch + YOLOv8 para detección avanzada

Instalación rápida:

pip install requests opencv-python numpy ultralytics
sudo apt install ffmpeg

Estructura del Proyecto
/tuv_flow_analysis
│
├── capturas/             # Imágenes descargadas
├── videos/               # Timelapses generados
├── data/                 # Resultados de análisis (CSV, logs)
├── capture.py            # Script de descarga automática
├── process.py            # Detección vehicular y conteo
└── README.md

Métricas Resultantes

Flujo vehicular por minuto, hora y día.

Densidad promedio de tráfico por planta.

Identificación de horarios punta y períodos de baja actividad.

Posibles Extensiones

Clasificación de tipo de vehículo (auto, camioneta, camión, moto).

Integración con panel web para visualización de datos.

Alertas en tiempo real ante congestión o interrupciones.

Consideraciones Legales y Éticas

Las cámaras utilizadas son públicas y pertenecen a instalaciones de TÜV Rheinland.

No se capturan datos personales ni se realiza identificación facial.

El propósito es únicamente estadístico y de optimización operativa.
