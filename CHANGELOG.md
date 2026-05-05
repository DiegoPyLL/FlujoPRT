# Changelog

Todos los cambios notables en este proyecto se documentan en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/),
y este proyecto adhiere a [Semantic Versioning](https://semver.org/lang/es/).

## [1.3.0] - 2026-05-03

### Added
- Dashboard Streamlit con actualización fragmentada (`@st.fragment`)
- Tres niveles de refresco independientes en dashboard:
  - `FAST_REFRESH`: Estado/KPIs (60 s)
  - `STATS_REFRESH`: Estadísticas del pipeline (120 s)
  - `DASHBOARD_REFRESH`: Gráficos de análisis (300 s)
- Variables de entorno para configurar refresh rates del dashboard
- Soporte para múltiples días de fallback en dashboard

### Changed
- Dashboard refactorizado con mejor rendimiento y separación de concerns

### Fixed
- Corrección en colecta de metadatos en `realtime_dashboard.py`

---

## [1.2.1] - 2026-04-30

### Added
- Política de versionado documentada en CLAUDE.md

### Changed
- Actualización general del sistema de realtime_dashboard

---

## [1.2.0] - 2026-04-28

### Added
- Sistema completo de reconocimiento vehicular integrado

### Changed
- Refactorización de dashboard realtime_dashboard

---

## [1.0.1] - 2026-04-27

### Added
- Validación vehicular idempotente por planta y día
- Detección con YOLOv8m (auto, moto, bus, camión)
- Generación de JSONL estructurado con metadata de detección
- Dibujo de bounding boxes sobre imágenes

### Changed
- `validate_vehiculos.py` ahora es idempotente (reanuda desde último checkpoint)
- Reescritura de rutas JSONL a `metadata/capturas/YYYY/MM/DD/{Planta}/`
- Integración de subida multipart con buffer de 5 MB

### Fixed
- Corrección de ubicación JSONL en S3

---

## [1.0.0] - 2025-12-15

### Added
- Sistema asíncrono de captura CCTV desde 14 cámaras IP
- Compresión JPEG + eliminación de EXIF en paralelo
- Deduplicación MD5 para evitar duplicados consecutivos
- Cola productor-consumidor para desacoplar captura/subida
- Subida a S3 con configuración `INTELLIGENT_TIERING`
- Metricas periódicas (cada 5 minutos) con estadísticas de compresión
- Control de horarios por planta (lun-sáb; domingos suspendido)
- Margen previo de 20 minutos antes de apertura de planta
- Shutdown limpio con drenaje de cola (hasta 5 minutos)
- Resiliencia: 5 reintentos con espera de 2.5s entre intentos
- Pausa de 10 minutos tras 10 errores consecutivos en una cámara
- Soporte para `uvloop` en Linux (fallback a `asyncio` en Windows)
- Logging con logger `flujo-prt` y prefijos informativos
- Verificación de credenciales AWS antes de iniciar
- Metadata EC2 vía IMDS v2 (best-effort)

### Architecture
- **asyncio + aiohttp**: Captura no-bloqueante
- **asyncio.Queue**: Productor-consumidor desacoplado
- **asyncio.Semaphore**: Control de descargas simultaneas
- **ThreadPoolExecutor**: Compresión JPEG CPU-bound
- **aioboto3**: Subida S3 asíncrona

### Configuration
- 14 cámaras monitoreadas en 3 regiones de Chile
- Horarios por planta (lunes-viernes y sábados diferenciados)
- Almacenamiento: `s3://flujo-prt-imagenes/capturas/YYYY/MM/DD/{Planta}/`

---

## Historial Completo (sin Semantic Versioning)

### [Pre_VehicleRecognition_AI]
- Agregada guía de despliegue AWS (DEPLOY_AWS.md)
- Remoción de permisos S3 legacy

### Cambios anteriores a versionado
- Implementación repository Vehicle Recognition AI
- Creación de tests unitarios
- Integración dashboard Streamlit
- Eliminación de módulo Timelapse (commits 4cc98fe, 5ac0c72)
- Consolidación de ImageRecompiler + MetadataIngestor
- Optimización S3 download con resilencia y throttling
- Implementación horarios por planta
- Optimizaciones de recursos en EC2

---

## Notas

- **Versionado centralizado**: Desde 1.3.0 todos los módulos deben usar la misma versión.
- **Cambios arquitectónicos mayores**: Eliminación de timelapse (commits 4cc98fe, 5ac0c72) fue deliberada.
- **Compatibilidad**: Sistema requiere Python 3.9+, aiohttp 3.9.2+, y ultralytics>=8.0 para reconocimiento vehicular.
