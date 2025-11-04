# Proyecto: Análisis de Flujo Vehicular con Cámaras TÜV Rheinland

## Descripción general
Este proyecto automatiza la captura, almacenamiento y análisis de imágenes provenientes de las cámaras públicas de las plantas de revisión técnica asociadas a TÜV Rheinland Chile. El objetivo es generar un timelapse diario y aplicar reconocimiento vehicular automatizado para estimar el flujo vehicular por planta, día y hora.

## Componentes principales
- **Captura de imágenes en vivo:** se accede a las cámaras disponibles en el dominio `pti-cameras.cl.tuv.com`. Cada imagen se solicita con un parámetro de tiempo (pitime o timestamp Unix) para evitar caché y reflejar la actualización más reciente.
- **Almacenamiento local y timelapse:** las imágenes se guardan con su timestamp y se convierten en secuencias de video usando ffmpeg. Cada video representa un intervalo de tiempo determinado (por ejemplo, un turno completo o un día de operación).
- **Análisis de flujo vehicular:** los videos se procesan con un modelo de reconocimiento vehicular basado en visión por computador (por ejemplo, YOLOv8, OpenCV o TensorFlow Object Detection). El sistema cuenta los vehículos detectados por cuadro y asocia la información a su timestamp, construyendo un registro de tráfico por franja horaria.

## Flujo de ejecución
1. **Descarga periódica:** un script Python descarga imágenes cada cierto intervalo (por defecto, cada 30 segundos).
2. **Generación del timelapse:** al completar el ciclo de captura, se usa ffmpeg para unir las imágenes en un video MP4.
3. **Procesamiento por IA:** un módulo de reconocimiento vehicular detecta y clasifica los vehículos presentes en cada cuadro del video.
4. **Almacenamiento y análisis:** los resultados se almacenan en una base de datos o CSV para análisis posterior, permitiendo generar métricas como flujo promedio por hora, picos de tráfico y variaciones diarias.

## Dependencias
- Python 3.10+
- requests
- OpenCV
- ffmpeg
- NumPy
- (Opcional) PyTorch + YOLOv8 para detección avanzada

Instalación rápida:
```bash
pip install requests opencv-python numpy ultralytics
sudo apt install ffmpeg
```

## Estructura del proyecto
```text
/tuv_flow_analysis
│
├── capturas/             # Imágenes descargadas
├── videos/               # Timelapses generados
├── data/                 # Resultados de análisis (CSV, logs)
├── capture.py            # Script de descarga automática
├── process.py            # Detección vehicular y conteo
└── README.md
```

## Métricas resultantes
- Flujo vehicular por minuto, hora y día.
- Densidad promedio de tráfico por planta.
- Identificación de horarios punta y períodos de baja actividad.

## Posibles extensiones
- Clasificación de tipo de vehículo (auto, camioneta, camión, moto).
- Integración con panel web para visualización de datos.
- Alertas en tiempo real ante congestión o interrupciones.

## Consideraciones legales y éticas
- Las cámaras utilizadas son públicas y pertenecen a instalaciones de TÜV Rheinland.
- No se capturan datos personales ni se realiza identificación facial.
- El propósito es únicamente estadístico y de optimización operativa.

## Licencias
- Por ver, quiero que sea de codigo abierto pero que no sea utilizado por las empresas de manera libre
