# CLAUDE.md

Guía para Claude Code al trabajar en este repositorio. Lee esto antes de hacer cambios.

## Resumen del Proyecto

**FlujoPRT** es un pipeline Python asíncrono que captura imágenes desde 14 cámaras IP de plantas de revisión técnica TUV Rheinland (Chile) y las almacena en AWS S3 junto con metadata estructurada (JSONL)

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
  DEPLOY_AWS.md                   # Guia de despliegue en AWS e IAM
deploy/
  requirements.cloud.txt          # Dependencias Python para EC2/Linux (incluye uvloop)
  requirements.local.txt          # Dependencias Python para desarrollo local/Windows (sin uvloop)
  run.sh                          # Script de ejecución en EC2
scripts/
  realtime_dashboard.py           # Dashboard Streamlit (read-only sobre S3)
tests/
  imageRecopilatorTest/           # Tests pytest (sufijo *_test.py)
```

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
- **Logging con el logger `flujo-prt`**, nivel INFO por defecto. Prefijos `[S3]`, `[VAL]`, etc. cuando ayudan a filtrar.
- **Sin tildes en identificadores** (nombres de funciones, variables, claves JSON).

## Desarrollo y Testing

### Dependencias

```bash
pip install -r deploy/requirements.cloud.txt  # runtime en EC2/Linux
pip install -r deploy/requirements.local.txt  # runtime en desarrollo local/Windows
pip install -r scripts/requirements.txt       # dashboard
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
- Prefijos: `capturas/YYYY/MM/DD/<Planta>/` para JPEGs; `metadata/capturas/YYYY/MM/DD/{Planta}/{DENOM}_YYYYMMDD.jsonl` para detecciones vehiculares; `metadata/stats/YYYY/MM/DD/resumen.json` para estadísticas del pipeline.
- StorageClass: `INTELLIGENT_TIERING`.
- Políticas IAM y guía de despliegue documentadas en [docs/DEPLOY_AWS.md](docs/DEPLOY_AWS.md).

**No tocar producción desde Claude sin confirmación explícita**: no ejecutar `aws s3 rm`, borrar objetos, modificar políticas IAM, ni lanzar capturas contra S3 real. Verificar bucket/prefijo antes de cualquier comando `aws s3`.

## Datos Sensibles

- IPs internas de cámaras (`10.57.x.x`) viven en el código — son red privada TUV, no secretos, pero no publicarlas fuera del repo.
- Credenciales AWS: nunca commitearlas. `.env` está en el repo pero debe contener solo plantillas/placeholders (ver línea 1: `TODO("Modificar el ENV")`).
- El `.env` actual tiene rutas locales de Windows y debe tratarse como ejemplo, no como fuente de verdad.



# Política de Versionado **OBLIGATORIO**

## Versión Base
- `1.0.0` define la primera versión estable en producción.

## Formato de Versión
- Se utiliza el esquema: `MAJOR.MINOR.PATCH`

### MAJOR
- Se incrementa cuando:
  - Se rompe compatibilidad hacia atrás
  - Cambian APIs, contratos o estructuras críticas
  - Se modifica la arquitectura de forma significativa

### MINOR
- Se incrementa cuando:
  - Se agregan nuevas funcionalidades
  - No se rompe compatibilidad existente

### PATCH
- Se incrementa cuando:
  - Se corrigen errores
  - Se realizan mejoras menores
  - No se agregan funcionalidades nuevas

---

## Reglas de Evaluación de Cambios

Antes de subir versión, cada cambio debe evaluarse con las siguientes preguntas:

- ¿Rompe compatibilidad hacia atrás?
- ¿Agrega funcionalidad nueva?
- ¿Solo corrige errores?
- ¿Impacta rendimiento o arquitectura?
- ¿Requiere cambios en despliegue o infraestructura?

### Decisión de Versión

- Si **rompe compatibilidad** → incrementar `MAJOR`
- Si **agrega funcionalidad sin romper** → incrementar `MINOR`
- Si **solo corrige o ajusta** → incrementar `PATCH`

---

## Flujo de Versionado

1. Desarrollo en ramas (`feature/*`, `fix/*`, etc.)

2. Antes de merge a `main`:
   - Documentar el cambio
   - Clasificar el impacto

3. Determinar nueva versión según reglas

4. Actualizar versión en el proyecto

5. Generar `CHANGELOG.md`

6. Crear tag en git:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z


## **Qué hacer y qué no**

**Sí:**

- Mantener el estilo async existente al extender el pipeline.
- Respetar el patrón productor/consumidor con `asyncio.Queue` para desacoplar captura de subida.
- Añadir nuevas variables de configuración vía `os.getenv` y documentarlas en README.
- Escribir tests en `tests/<modulo>Test/` con sufijo `_test.py`.
- Tener en consideración el consumo de créditos de aws y recursos de la instancia.

**No:**

- No introducir frameworks nuevos (Django, FastAPI, etc.) — este es un servicio batch/daemon, no una web app.
- No reintroducir el módulo de timelapse ni el flujo cloud de timelapse; fueron eliminados deliberadamente (commits `4cc98fe`, `5ac0c72`).
- No cambiar la estructura de rutas S3 (`capturas/YYYY/MM/DD/Planta/...`); hay consumers downstream (dashboard, validador) que dependen de ella.
- No agregar dependencias pesadas al runtime de captura; el dashboard y validador tienen su propio `requirements.txt` aparte.
- No asumir que el CSV de plantas es el dataset a procesar. El **dataset real** son los registros JSONL de detecciones vehiculares en S3 (`metadata/capturas/YYYY/MM/DD/{Planta}/`). El archivo `data/plantas_revision_tecnica.csv` es catálogo de referencia, no fuente ETL.

## Entorno de Desarrollo

- SO principal del desarrollador: **Windows 11** (shell bash vía Git Bash/WSL).
- Producción: **Linux EC2** (Amazon Linux/Ubuntu) con `tmux` + `run.sh`.
- Zona horaria canónica: `America/Santiago` (se setea en el proceso con `TZ` env var).
- Cuando escribas rutas en scripts, usa barras `/` y rutas relativas al root del repo.

## Referencias rápidas

- Arquitectura del pipeline: [README.md](README.md#arquitectura-del-pipeline)
- Comandos de ejecución EC2: [docs/ComandosEjecucionCloud.txt](docs/ComandosEjecucionCloud.txt)
- Despliegue AWS e IAM: [docs/DEPLOY_AWS.md](docs/DEPLOY_AWS.md)
- Dashboard en vivo: `streamlit run scripts/realtime_dashboard.py --server.address 127.0.0.1`
