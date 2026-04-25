# FlujoPRT

Sistema automatizado de captura de imágenes CCTV desde plantas de revisión técnica vehicular en Chile, con ingesta de datos estructurados y almacenamiento en AWS S3.

## Descripcion del Proyecto

FlujoPRT captura imagenes en tiempo real desde 14 camaras IP de plantas TUV Rheinland distribuidas en 3 regiones de Chile. Las imagenes se comprimen, deduplican y almacenan automaticamente en AWS S3, organizadas por fecha y planta. Adicionalmente, el sistema genera metadata estructurada (JSON) a partir de un catalogo CSV de 116 plantas nacionales, almacenandola en un prefijo separado del mismo bucket.

El sistema esta disenado para operar de forma continua en una instancia EC2, respetando los horarios de operacion de cada planta (lunes a sabado) y suspendiendo la actividad los domingos.

## Arquitectura del Pipeline

```
                    FUENTE DE DATOS
                    ===============
    Camaras IP (14)                CSV Plantas (116)
         |                                |
         v                                v
  +---------------+             +-------------------+
  | Captura HTTP  |             | Lectura CSV       |
  | (aiohttp)     |             | (csv.DictReader)  |
  +-------+-------+             +--------+----------+
          |                              |
          v                              v
  +---------------+             +-------------------+
  | Compresion    |             | Enriquecimiento   |
  | JPEG + MD5    |             | (cruce con        |
  | (Pillow)      |             |  camaras activas) |
  +-------+-------+             +--------+----------+
          |                              |
          v                              v
  +---------------+             +-------------------+
  | Cola Async    |             | catalogo_plantas   |
  | (Queue)       |             | .json             |
  +-------+-------+             +--------+----------+
          |                              |
          v                              v
  +---------------+    +------+-------------------+
  | Workers S3    |--->| AWS S3                    |
  | (aioboto3)    |    |  capturas/YYYY/MM/DD/... |
  |               |    |  metadata/plantas/...     |
  | + metadata    |    |  metadata/capturas/...    |
  |   por captura |    +---------------------------+
  +---------------+
```

## Estructura del Proyecto

```
FlujoPRT/
├── README.md
├── pytest.ini
├── data/
│   ├── plantas_revision_tecnica.csv      # Fuente de datos: 116 plantas nacionales
│   └── plantas_revision_tecnica.xlsx
├── docs/
│   ├── ComandosEjecucionCloud.txt        # Guia de comandos
│   └── DEPLOY_AWS.md                     # Guia de despliegue en AWS e IAM
├── deploy/
│   ├── requirements.txt                  # Dependencias Python del runtime
│   └── run.sh                            # Script de ejecucion en EC2
├── src/
│   └── imageRecopilator/
│       └── Cloud/
│           └── ImageRecompilerCloud.py   # Modulo principal (captura + metadata)
└── tests/
    └── imageRecopilatorTest/
        ├── imageRecopilatorCloud_test.py
        └── imageRecopilatorLocal_test.py
```

## Estructura de Almacenamiento en S3

```
s3://flujo-prt-imagenes/
│
├── capturas/                                  # Imagenes JPEG capturadas
│   └── YYYY/
│       └── MM/
│           └── DD/
│               └── NombrePlanta/
│                   └── DEN_YYYYMMDD_HHMMSS.jpg
│
└── metadata/                                  # Datos estructurados (JSON)
    ├── plantas/
    │   └── catalogo_plantas.json              # Catalogo completo de 116 plantas
    └── capturas/
        └── YYYY/
            └── MM/
                └── DD/
                    └── NombrePlanta/
                        └── DEN_YYYYMMDD_HHMMSS.json
```

Cada archivo de metadata espeja exactamente la ruta de su imagen correspondiente, reemplazando el prefijo `capturas/` por `metadata/capturas/` y la extension `.jpg` por `.json`.

## Requisitos Previos

- Python 3.9 o superior
- Cuenta AWS con acceso a S3 y EC2
- Credenciales AWS configuradas (`aws configure` o variables de entorno)

## Instalacion

### En servidor EC2 (produccion)

```bash
sudo apt update && sudo apt install python3-pip awscli
git clone https://github.com/DiegoPyLL/FlujoPRT
cd FlujoPRT
pip install --user -r deploy/requirements.txt
```

### En entorno local (desarrollo)

```bash
git clone https://github.com/DiegoPyLL/FlujoPRT
cd FlujoPRT
pip install -r deploy/requirements.txt
```

### Dependencias

| Paquete | Version | Proposito |
|---------|---------|-----------|
| aiohttp | 3.9.2 | Cliente HTTP asincrono para captura de imagenes |
| aioboto3 | 12.3.0 | Cliente AWS S3 asincrono |
| aiobotocore | 2.11.2 | Core de aioboto3 |
| boto3 | 1.34.34 | SDK AWS para Python |
| botocore | 1.34.34 | Core de boto3 |
| s3transfer | 0.10.0 | Transferencias S3 optimizadas |
| Pillow | 10.2.0 | Compresion y procesamiento JPEG |
| uvloop | 0.19.0 | Event loop optimizado (solo Linux) |

El modulo `MetadataIngestor.py` no requiere dependencias adicionales: utiliza unicamente la biblioteca estandar de Python (`csv`, `json`, `datetime`).

## Configuracion

Todas las variables se configuran mediante variables de entorno. Si no se definen, se usan los valores por defecto.

### Captura de Imagenes

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `S3_BUCKET` | `flujo-prt-imagenes` | Nombre del bucket S3 |
| `S3_PREFIX` | `capturas` | Prefijo S3 para imagenes |
| `INTERVALO` | `60` | Segundos entre capturas por camara |
| `TZ` | `America/Santiago` | Zona horaria del sistema |
| `JPEG_QUALITY` | `80` | Calidad de compresion JPEG (0-100) |
| `MAX_DESCARGAS` | `10` | Descargas HTTP simultaneas maximas |
| `QUEUE_SIZE` | `40` | Tamanio maximo de la cola de subida |
| `NUM_UPLOADERS` | `2` | Workers paralelos de subida a S3 |
| `METRICAS_INTERVALO` | `300` | Segundos entre reportes de metricas |
| `MARGEN_PREVIO` | `1200` | Segundos antes de apertura para despertar (20 min) |

### Ingesta de Metadata

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `METADATA_PREFIX` | `metadata` | Prefijo S3 para archivos de metadata |
| `PLANTAS_CSV_PATH` | `data/plantas_revision_tecnica.csv` | Ruta al CSV fuente |
| `METADATA_SNAPSHOT` | `false` | Si es `true`, guarda copia fechada del catalogo |
| `CATALOGO_REFRESH_HORAS` | `24` | Horas entre actualizaciones del catalogo |

## Ejecucion

### Modo principal (captura + metadata integrada)

```bash
# Desde la raiz del proyecto
python3 src/imageRecopilator/Cloud/ImageRecompilerCloud.py
```

Al ejecutarse:
1. Verifica credenciales AWS (via STS)
2. Ingesta el catalogo de plantas desde el CSV y lo sube a S3
3. Lanza 14 tareas de captura en paralelo (una por camara)
4. Lanza 2 workers S3 que suben imagenes y metadata simultaneamente
5. Opera continuamente respetando los horarios de cada planta

### Modo standalone (solo ingesta de metadata)

```bash
# Ejecutar solo la ingesta del catalogo CSV a S3
cd src
python3 -m imageRecopilator.Cloud.MetadataIngestor
```

Util para actualizar el catalogo de plantas sin necesidad de iniciar la captura de imagenes.

### Ejecucion en EC2 con tmux (produccion)

```bash
# Crear sesion persistente
tmux new -s FlujoPRT_CCTV

# Ejecutar el sistema
python3 src/imageRecopilator/Cloud/ImageRecompilerCloud.py

# Desconectarse sin detener el proceso: Ctrl+B, luego D

# Reconectarse a la sesion
tmux attach -t FlujoPRT_CCTV
```

### Ejecucion con script de servicio

```bash
chmod +x deploy/run.sh

./deploy/run.sh start    # Iniciar en background
./deploy/run.sh stop     # Detener
./deploy/run.sh restart  # Reiniciar
./deploy/run.sh status   # Ver estado
./deploy/run.sh logs     # Ver logs en tiempo real
```

## Camaras Monitoreadas

### Region Metropolitana (7 plantas)

| Planta | Denominador | Horario Lun-Vie | Horario Sabado |
|--------|-------------|------------------|----------------|
| Huechuraba | HCH | 07:10 - 16:50 | 07:10 - 16:50 |
| La Florida | LFL | 07:40 - 17:20 | 07:10 - 16:50 |
| La Pintana | LPT | 07:40 - 17:20 | 07:10 - 16:50 |
| Pudahuel | PUD | 07:40 - 17:20 | 07:10 - 16:50 |
| Quilicura | QLC | 07:10 - 16:50 | 07:10 - 16:50 |
| Recoleta | RCL | 07:40 - 17:20 | 07:10 - 16:50 |
| San Joaquin | SJQ | 07:40 - 17:20 | 07:10 - 16:50 |

### Region de La Araucania (2 plantas)

| Planta | Denominador | Horario Lun-Vie | Horario Sabado |
|--------|-------------|------------------|----------------|
| Temuco | TMU | 08:10 - 18:20 | 08:10 - 13:50 |
| Villarica | VLL | 07:10 - 17:50 | 07:40 - 13:50 |

### Region del Biobio y Nuble (5 plantas)

| Planta | Denominador | Horario Lun-Vie | Horario Sabado |
|--------|-------------|------------------|----------------|
| Chillan | CHL | 06:40 - 17:20 | 07:10 - 13:50 |
| Yungay | YGY | 07:40 - 17:20 | 08:10 - 13:50 |
| Concepcion | CCP | 07:40 - 20:20 | 08:10 - 16:50 |
| San Pedro de la Paz | SPP | 07:40 - 17:20 | 08:10 - 13:50 |
| Yumbel | YMB | 07:40 - 17:20 | 08:10 - 13:50 |

Los domingos no se realiza captura en ninguna planta.

## Ingesta de Metadata Estructurada

### Que es y por que existe

El sistema no solo captura imagenes, tambien genera **datos estructurados** que permiten analizar y consultar las capturas sin necesidad de procesar los archivos JPEG. La metadata se almacena como archivos JSON en S3, separada de las imagenes pero vinculada a ellas.

### Fuente de datos: CSV de plantas

El archivo `data/plantas_revision_tecnica.csv` contiene informacion de **116 plantas de revision tecnica** de 15 plataformas nacionales:

| Columna | Descripcion | Ejemplo |
|---------|-------------|---------|
| Plataforma | Empresa operadora | TUV Rheinland, San Damaso, Applus |
| Region | Region de Chile | Region Metropolitana |
| Comuna | Comuna donde esta la planta | Huechuraba |
| Direccion | Direccion fisica | Av. Santa Clara N523 |
| URL_Reserva | Sitio web de reservas | https://www.prt.tuv.com/horario-prt |

De estas 116 plantas, **14 corresponden a TUV Rheinland** y tienen camara activa en el sistema.

### Tipo 1: Catalogo de plantas

Se genera al iniciar el sistema. Lee el CSV completo, cruza cada planta con los diccionarios de camaras activas del modulo de captura, y sube el resultado como un unico JSON a S3.

**Ubicacion:** `s3://flujo-prt-imagenes/metadata/plantas/catalogo_plantas.json`

**Ejemplo de registro (planta con camara activa):**

```json
{
  "planta_id": "HCH",
  "nombre": "Huechuraba",
  "plataforma": "TUV Rheinland",
  "region": "Region Metropolitana",
  "comuna": "Huechuraba",
  "direccion": "Av. Santa Clara N523",
  "url_reserva": "https://www.prt.tuv.com/horario-prt",
  "tiene_camara_activa": true,
  "cam_id": "10.57.6.222_Cam08",
  "horarios": {
    "semana": { "apertura": "07:10", "cierre": "16:50" },
    "sabado": { "apertura": "07:10", "cierre": "16:50" }
  },
  "generado_en": "2026-04-19T10:00:00"
}
```

**Ejemplo de registro (planta sin camara):**

```json
{
  "planta_id": "QUI",
  "nombre": "Quilpue",
  "plataforma": "San Damaso",
  "region": "Valparaiso",
  "comuna": "Quilpue",
  "direccion": "Av. del Trabajador 627",
  "url_reserva": "https://www.sandamaso.cl/",
  "tiene_camara_activa": false,
  "cam_id": null,
  "horarios": null,
  "generado_en": "2026-04-19T10:00:00"
}
```

### Tipo 2: Metadata por captura

Cada vez que una imagen se sube exitosamente a S3, el worker genera automaticamente un archivo JSON con informacion de esa captura. La ruta del JSON espeja la de la imagen:

- Imagen: `capturas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg`
- Metadata: `metadata/capturas/2026/04/19/Huechuraba/HCH_20260419_100523.json`

**Ejemplo de metadata por captura:**

```json
{
  "version": "1",
  "planta_id": "HCH",
  "planta_nombre": "Huechuraba",
  "plataforma": "TUV Rheinland",
  "timestamp_captura": "2026-04-19T10:05:23",
  "fecha_str": "20260419_100523",
  "s3_imagen_key": "capturas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
  "s3_bucket": "flujo-prt-imagenes",
  "bytes_originales": 85432,
  "bytes_comprimidos": 52000,
  "ratio_compresion": 0.6086,
  "generado_en": "2026-04-19T10:05:24"
}
```

### Trazabilidad y Logging

El modulo de metadata registra cada etapa del proceso:

| Evento | Nivel | Mensaje de ejemplo |
|--------|-------|--------------------|
| Inicio de ingesta | INFO | `=== INICIO INGESTA CATALOGO PLANTAS ===` |
| CSV leido | INFO | `CSV leido: 116 plantas de 15 plataformas` |
| Catalogo construido | INFO | `Catalogo construido: 14 con camara activa, 102 sin camara` |
| Catalogo subido a S3 | INFO | `Catalogo subido: 116 registros -> s3://flujo-prt-imagenes/...` |
| CSV no encontrado | ERROR | `No se encontro CSV: data/plantas_revision_tecnica.csv` |
| Metadata de captura subida | DEBUG | `[META] Huechuraba -> s3://flujo-prt-imagenes/metadata/capturas/...` |
| Error subiendo metadata | WARNING | `[META] No se pudo generar metadata para Huechuraba: ...` |

La metadata por captura opera en modo **best-effort**: si falla, se registra un warning pero no interrumpe la captura ni la subida de la imagen.

## Caracteristicas Tecnicas

### Concurrencia

- **asyncio + aiohttp**: Captura no-bloqueante de multiples camaras en paralelo
- **asyncio.Queue**: Cola productor-consumidor desacopla captura de subida
- **asyncio.Semaphore**: Limita descargas simultaneas para no saturar la red
- **ThreadPoolExecutor**: Compresion JPEG en hilos separados (CPU-bound)
- **uvloop**: Event loop optimizado en Linux (fallback a asyncio en Windows)

### Optimizaciones

- **Deduplicacion MD5**: No sube imagenes identicas consecutivas (ahorra ancho de banda y almacenamiento)
- **Compresion JPEG configurable**: Reduce tamanio de imagen antes de subir
- **Eliminacion de EXIF**: Remueve metadata innecesaria de las imagenes
- **StorageClass INTELLIGENT_TIERING**: Optimiza costos de almacenamiento en S3
- **DNS cache**: TTL de 5 minutos en el connector HTTP
- **Jitter por planta**: Evita que todas las camaras consulten al mismo instante

### Resiliencia

- **5 reintentos por captura** con espera de 2.5s entre intentos
- **Pausa de 10 minutos** tras 10 errores consecutivos en una camara
- **Cola con timeout**: Si la cola esta llena, no bloquea la captura indefinidamente
- **Shutdown limpio**: Drena la cola de subida (hasta 5 minutos) antes de cerrar
- **Metricas periodicas**: Cada 5 minutos reporta estado del sistema

### Control de Horarios

El sistema solo captura durante los horarios de operacion de cada planta:
- **Lunes a viernes**: Horario completo por planta
- **Sabados**: Horario reducido por planta
- **Domingos**: Sin captura, el sistema se suspende hasta el lunes
- **Margen previo**: Se despierta 20 minutos antes de la apertura

## Monitoreo y Verificacion

### Dashboard en tiempo real (Streamlit)

Visualiza el estado del pipeline (plantas OK / caidas, gaps, volumen subido a S3, ratio de compresion, latencia) leyendo los JSONs de metadata del dia actual.

```bash
# Instalar dependencias del dashboard (aisladas del runtime de captura)
pip install -r scripts/requirements.txt

# Levantar el dashboard (bind a loopback, no exponer a internet)
streamlit run scripts/realtime_dashboard.py \
    --server.port 8501 --server.address 127.0.0.1
```

Para acceder desde la maquina local cuando corre en EC2:

```bash
ssh -L 8501:localhost:8501 ec2-user@<ip-ec2>
# luego abrir http://localhost:8501 en el browser
```

Variables de entorno relevantes:

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `DASHBOARD_REFRESH` | `300` | Segundos entre refresh automatico |
| `GAP_THRESHOLD` | `180` | Segundos entre capturas para marcar gap |
| `DOWN_THRESHOLD` | `900` | Segundos sin captura para marcar planta caida |

El dashboard es read-only sobre S3, no interfiere con el capturador.

### Verificar que el sistema esta capturando

```bash
# Ver fotos de hoy
aws s3 ls s3://flujo-prt-imagenes/capturas/2026/04/19/

# Ver metadata de hoy
aws s3 ls s3://flujo-prt-imagenes/metadata/capturas/2026/04/19/

# Ver el catalogo de plantas
aws s3 cp s3://flujo-prt-imagenes/metadata/plantas/catalogo_plantas.json -
```

### Descargar capturas

```bash
# Todas las capturas
aws s3 sync s3://flujo-prt-imagenes/capturas/ ./descargas/

# Un dia especifico
aws s3 sync s3://flujo-prt-imagenes/capturas/2026/04/19/ ./fotos_hoy/

# Una planta especifica
aws s3 sync s3://flujo-prt-imagenes/capturas/2026/04/19/Temuco/ ./fotos_temuco/

# Solo metadata
aws s3 sync s3://flujo-prt-imagenes/metadata/ ./metadata/
```

### Ver tamanio de almacenamiento

```bash
aws s3 ls s3://flujo-prt-imagenes/capturas/ --recursive --human-readable --summarize
aws s3 ls s3://flujo-prt-imagenes/metadata/ --recursive --human-readable --summarize
```

## Modulos del Sistema

### ImageRecompilerCloud.py

Modulo principal. Orquesta la captura, compresion, deduplicacion y subida de imagenes. Al iniciar, tambien dispara la ingesta del catalogo de metadata y en cada subida exitosa genera la metadata por captura.

**Funciones principales:**
- `capturar_camara()` - Tarea async por camara, captura cada 60 segundos
- `worker_subida_s3()` - Consumer de la cola, sube imagen + metadata a S3
- `verificar_credenciales_aws()` - Valida acceso AWS antes de iniciar
- `dentro_horario()` - Determina si una planta esta en horario de operacion
- `esperar_hasta_apertura()` - Suspende el sistema fuera de horario

### MetadataIngestor.py

Modulo de ingesta de datos estructurados. Puede ejecutarse de forma integrada (importado por ImageRecompilerCloud) o standalone.

**Funciones principales:**
- `leer_csv_plantas()` - Lee y parsea el CSV fuente con csv.DictReader
- `construir_catalogo_plantas()` - Enriquece datos CSV con informacion de camaras activas
- `generar_metadata_captura()` - Crea diccionario de metadata para una imagen capturada
- `ingestar_catalogo_plantas()` - Orquesta lectura, construccion y subida del catalogo a S3
- `subir_metadata_captura()` - Genera y sube metadata JSON por cada imagen (best-effort)

## Links de Referencia

- Listado de plantas PRT: https://www.prt.tuv.com/red-de-plantas-revision-tecnicas?page=1
- Estadisticas de flujo vehicular: https://www.prt.cl/Paginas/estadisticas.aspx
- Buscador de plantas: https://www.prt.cl/Paginas/Buscador.aspx
- Base URL de camaras: `https://pti-cameras.cl.tuv.com/camaras/`
- Formato de URL: `https://pti-cameras.cl.tuv.com/camaras/{IP_CamXX}/imagen.jpg`
