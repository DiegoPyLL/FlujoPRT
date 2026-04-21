# CLAUDE.md

Guía para Claude Code al trabajar en este repositorio. Lee esto antes de hacer cambios.

## Resumen del Proyecto

**FlujoPRT** es un pipeline Python asíncrono que captura imágenes desde 14 cámaras IP de plantas de revisión técnica TUV Rheinland (Chile) y las almacena en AWS S3 junto con metadata estructurada (JSON) derivada de un catálogo CSV de 116 plantas nacionales.

El sistema está pensado para correr 24/7 en una instancia EC2 respetando los horarios de operación por planta (lun–sáb; domingos suspendido).

Consultar [README.md](README.md) para la descripción completa, arquitectura y comandos operativos.

## Idioma

- **Código, logs, mensajes, commits y documentación en español** (sin tildes en identificadores de código: `catalogo`, `camara`, `captura`).
- Comentarios y docstrings en español.
- Mantener esta convención en cualquier archivo nuevo.

## Estructura Clave

```
src/imageRecopilator/
  Cloud/ImageRecompilerCloud.py   # Módulo principal (captura + metadata integrada)
  Local/ImageRecompilerLocal.py   # Variante local (desarrollo/offline)
data/
  plantas_revision_tecnica.csv    # Fuente de datos: 116 plantas
docs/
  ComandosEjecucionCloud.txt      # Guía de comandos operativos
  PERMISOS_S3.md                  # Políticas IAM mínimas
deploy/
  requirements.txt                # Dependencias Python del runtime
  run.sh                          # Script de ejecución en EC2
scripts/
  realtime_dashboard.py           # Dashboard Streamlit (read-only sobre S3)
  validate_metadata.py            # Validador de metadata
tests/
  imageRecopilatorTest/           # Tests pytest (sufijo *_test.py)
```

Nota: `MetadataIngestor.py` fue fusionado dentro de `ImageRecompilerCloud.py` (commit `7ea2577`). Todo lo relativo a ingesta de catálogo y metadata por captura vive ahora en ese archivo único.

## Stack Técnico

- **Python 3.9+** con `asyncio` como modelo de concurrencia principal.
- **aiohttp** para captura HTTP paralela de cámaras.
- **aioboto3 / boto3** para S3 asíncrono.
- **Pillow** para compresión JPEG + eliminación de EXIF.
- **uvloop** como event loop en Linux (fallback a asyncio en Windows).
- **Streamlit** solo para el dashboard (`scripts/`), aislado del runtime de captura.

## Convenciones de Código

- **Async por defecto** en el pipeline de captura/subida. No introducir código bloqueante en el event loop; usar `ThreadPoolExecutor` solo para CPU-bound (ej. compresión JPEG).
- **Configuración por variables de entorno** con `os.getenv("NOMBRE", default)`. No hardcodear valores que ya existen como variable (ver tabla en README).
- **Logging con el logger `flujo-prt`**, nivel INFO por defecto. Prefijos `[META]`, `[S3]`, etc. cuando ayudan a filtrar.
- **Best-effort para metadata por captura**: si falla, warning y seguir — nunca interrumpir la captura por un error de metadata.
- **Sin tildes en identificadores** (nombres de funciones, variables, claves JSON).

## Desarrollo y Testing

### Dependencias

```bash
pip install -r deploy/requirements.txt   # runtime
pip install -r scripts/requirements.txt  # dashboard
```

### Tests

```bash
pytest                              # corre todo
pytest tests/imageRecopilatorTest/  # módulo específico
pytest -m ImageRecopilator          # por marker
```

Configuración en [pytest.ini](pytest.ini): `pythonpath=src`, archivos `*_test.py`.

### Ejecución local

```bash
python3 src/imageRecopilator/Cloud/ImageRecompilerCloud.py
```

Requiere credenciales AWS configuradas (`aws configure` o variables de entorno) y acceso a la red interna TUV para las cámaras.

## AWS / S3

- Bucket: `flujo-prt-imagenes` (us-east-1).
- Prefijos: `capturas/YYYY/MM/DD/<Planta>/` para JPEGs; `metadata/capturas/...` espeja la ruta con `.json`; `metadata/plantas/catalogo_plantas.json` para el catálogo.
- StorageClass: `INTELLIGENT_TIERING`.
- Políticas IAM mínimas documentadas en [docs/PERMISOS_S3.md](docs/PERMISOS_S3.md).

**No tocar producción desde Claude sin confirmación explícita**: no ejecutar `aws s3 rm`, borrar objetos, modificar políticas IAM, ni lanzar capturas contra S3 real. Verificar bucket/prefijo antes de cualquier comando `aws s3`.

## Datos Sensibles

- IPs internas de cámaras (`10.57.x.x`) viven en el código — son red privada TUV, no secretos, pero no publicarlas fuera del repo.
- Credenciales AWS: nunca commitearlas. `.env` está en el repo pero debe contener solo plantillas/placeholders (ver línea 1: `TODO("Modificar el ENV")`).
- El `.env` actual tiene rutas locales de Windows y debe tratarse como ejemplo, no como fuente de verdad.

## Qué hacer y qué no

**Sí:**
- Mantener el estilo async existente al extender el pipeline.
- Respetar el patrón productor/consumidor con `asyncio.Queue` para desacoplar captura de subida.
- Añadir nuevas variables de configuración vía `os.getenv` y documentarlas en README.
- Escribir tests en `tests/<modulo>Test/` con sufijo `_test.py`.

**No:**
- No introducir frameworks nuevos (Django, FastAPI, etc.) — este es un servicio batch/daemon, no una web app.
- No reintroducir el módulo de timelapse ni el flujo cloud de timelapse; fueron eliminados deliberadamente (commits `4cc98fe`, `5ac0c72`).
- No separar `MetadataIngestor` de nuevo sin pedirlo — fue fusionado a propósito.
- No cambiar la estructura de rutas S3 (`capturas/YYYY/MM/DD/Planta/...`); hay consumers downstream (dashboard, validador) que dependen de ella.
- No agregar dependencias pesadas al runtime de captura; el dashboard y validador tienen su propio `requirements.txt` aparte.

## Entorno de Desarrollo

- SO principal del desarrollador: **Windows 11** (shell bash vía Git Bash/WSL).
- Producción: **Linux EC2** (Amazon Linux/Ubuntu) con `tmux` + `run.sh`.
- Zona horaria canónica: `America/Santiago` (se setea en el proceso con `TZ` env var).
- Cuando escribas rutas en scripts, usa barras `/` y rutas relativas al root del repo.

## Referencias rápidas

- Arquitectura del pipeline: [README.md](README.md#arquitectura-del-pipeline)
- Comandos de ejecución EC2: [docs/ComandosEjecucionCloud.txt](docs/ComandosEjecucionCloud.txt)
- Permisos IAM: [docs/PERMISOS_S3.md](docs/PERMISOS_S3.md)
- Dashboard en vivo: `streamlit run scripts/realtime_dashboard.py --server.address 127.0.0.1`
