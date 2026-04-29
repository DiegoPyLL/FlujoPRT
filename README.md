# FlujoPRT

Sistema automatizado de captura de imágenes CCTV desde plantas de revisión técnica vehicular en Chile, con ingesta de datos estructurados y almacenamiento en AWS S3.

## Descripcion del Proyecto

FlujoPRT captura imagenes en tiempo real a 14 camaras IP de plantas TUV Rheinland distribuidas en 3 regiones de Chile. Las imagenes se comprimen, deduplican y almacenan automaticamente en AWS S3, organizadas por fecha y planta. Luego de esto, se pasa por un proceso de reconocimiento vehicular en donde las imagenes de los vehiculos detectados se les guarda su box dibujada y su JSONL para mejorar la trazabilidad del dato.

El sistema esta disenado para operar de forma continua en una instancia EC2, respetando los horarios de operacion de cada planta (lunes a sabado) y suspendiendo la actividad los domingos.

## Arquitectura del Pipeline

```
                   FUENTE DE DATOS
                   ===============
                   Camaras IP (14)       
                         |                
                         v                
                  +---------------+       
                  | Captura HTTP  |       
                  | (aiohttp)     |       
                  +-------+-------+       
                          |               
                          v               
                  +---------------+       
                  | Compresion    |       
                  | JPEG + MD5    |       
                  | (Pillow)      |       
                  +-------+-------+       
                          |               
                          v               
                  +---------------+       
                  | Cola Async    |       
                  | (Queue)       |       
                  +-------+-------+       
                          |               
                          v               
                  +---------------+
                  | Workers S3    |
                  | (aioboto3)    |
                  +---------------+
                          |               
                          v         
         +---------------------------------+                  
         | validate_vehiculo.py            |       
         | (Reconocimiento vehicular       |       
         |   utilizando YOLOv8m)           |         
         |                                 | 
         | + JSONL guardando la metadata   |   
         |   del Reconocimiento vehicular  |   
         |   separadolo por YYYY/MM/DD     | 
         +---------------------------------+      
                          |               
                          v  
            +---------------------------+
            | AWS S3                    |
            |  capturas/YYYY/MM/DD/...  |
            |  metadata/capturas/...    |
            |  metadata/stats/...       |
            +---------------------------+
```

## Estructura del Proyecto

```
FlujoPRT/
├── README.md
├── SUPPORT.md                            # Guia de despliegue AWS paso a paso
├── pytest.ini
├── data/
│   ├── plantas_revision_tecnica.csv      # Fuente de datos: 116 plantas nacionales
│   └── plantas_revision_tecnica.xlsx
├── docs/
│   ├── ComandosEjecucionCloud.txt        # Guia de comandos operativos en EC2
│   └── DEPLOY_AWS.md                     # Referencia IAM y S3
├── deploy/
│   ├── requirements.cloud.txt            # Dependencias Python para EC2/Linux
│   ├── requirements.local.txt            # Dependencias Python para desarrollo local
│   └── run.sh                            # Script de ejecucion en EC2
├── scripts/
│   ├── realtime_dashboard.py             # Dashboard Streamlit (lectura S3)
│   └── VehicleRecognition/              # Subsistema de reconocimiento vehicular
│       ├── detector.py                   # Wrapper YOLOv8 para deteccion de vehiculos
│       ├── validate_vehiculos.py         # Validador con bbox y log JSONL
│       └── analisis_historico.py         # Procesamiento en lote del historico S3
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
├── capturas/                                  # Imagenes JPEG originales capturadas
│   └── YYYY/MM/DD/NombrePlanta/
│       └── DEN_YYYYMMDD_HHMMSS.jpg
│
├── capturas_anotadas/                         # Imagenes con bounding boxes dibujados
│   └── YYYY/MM/DD/NombrePlanta/
│       └── DEN_YYYYMMDD_HHMMSS.jpg
│
└── metadata/                                  # Datos estructurados (JSON / JSONL)
    ├── capturas/                              # JSONL vehicular diario por planta
    │   └── YYYY/MM/DD/NombrePlanta/
    │       └── DEN_YYYYMMDD.jsonl             # Validacion vehicular diaria por planta
    ├── capturas_anotadas/                     # Imagenes con bounding boxes dibujados
    │   └── YYYY/MM/DD/NombrePlanta/
    │       └── DEN_YYYYMMDD_HHMMSS.jpg
    ├── detecciones/                           # Resultados YOLOv8 por imagen (analisis_historico)
    │   └── YYYY/MM/DD/NombrePlanta/
    │       └── DEN_YYYYMMDD_HHMMSS.json
    └── stats/                                 # Metricas periodicas del proceso de captura
        └── YYYY/MM/DD/
            └── resumen.json
```

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
pip install --user -r deploy/requirements.cloud.txt
```

### En entorno local (desarrollo)

```bash
git clone https://github.com/DiegoPyLL/FlujoPRT
cd FlujoPRT
pip install -r deploy/requirements.local.txt
```

### Dependencias del runtime de captura

| Paquete     | Version | Proposito                                       |
| ----------- | ------- | ----------------------------------------------- |
| aiohttp     | 3.9.2   | Cliente HTTP asincrono para captura de imagenes |
| aioboto3    | 12.3.0  | Cliente AWS S3 asincrono                        |
| aiobotocore | 2.11.2  | Core de aioboto3                                |
| boto3       | 1.34.34 | SDK AWS para Python                             |
| botocore    | 1.34.34 | Core de boto3                                   |
| s3transfer  | 0.10.0  | Transferencias S3 optimizadas                   |
| Pillow      | 10.2.0  | Compresion y procesamiento JPEG                 |
| uvloop      | 0.19.0  | Event loop optimizado (solo Linux)              |

### Dependencias del subsistema vehicular

| Paquete     | Proposito                                                  |
| ----------- | ---------------------------------------------------------- |
| ultralytics | YOLOv8 para deteccion de vehiculos en imagenes CCTV        |
| boto3       | Acceso a S3 (modo sincrono, suficiente para scripts batch) |
| Pillow      | Dibujo de bounding boxes sobre imagenes                    |
| tqdm        | Barra de progreso durante el procesamiento en lote         |

```bash
pip install ultralytics tqdm Pillow boto3
```

El modelo `yolov8m.pt` (~50 MB) se descarga automaticamente por `ultralytics` la primera vez.

## Configuracion

Todas las variables se configuran mediante variables de entorno. Si no se definen, se usan los valores por defecto.

### Captura de Imagenes

| Variable               | Default                | Descripcion                                        |
| ---------------------- | ---------------------- | -------------------------------------------------- |
| `S3_BUCKET`          | `flujo-prt-imagenes` | Nombre del bucket S3                               |
| `S3_PREFIX`          | `capturas`           | Prefijo S3 para imagenes                           |
| `INTERVALO`          | `60`                 | Segundos entre capturas por camara                 |
| `TZ`                 | `America/Santiago`   | Zona horaria del sistema                           |
| `JPEG_QUALITY`       | `80`                 | Calidad de compresion JPEG (0-100)                 |
| `MAX_DESCARGAS`      | `10`                 | Descargas HTTP simultaneas maximas                 |
| `QUEUE_SIZE`         | `40`                 | Tamanio maximo de la cola de subida                |
| `NUM_UPLOADERS`      | `2`                  | Workers paralelos de subida a S3                   |
| `METRICAS_INTERVALO` | `300`                | Segundos entre reportes de metricas                |
| `MARGEN_PREVIO`      | `1200`               | Segundos antes de apertura para despertar (20 min) |

## Ejecucion

### Modo principal (captura)

```bash
# Desde la raiz del proyecto
python3 src/imageRecopilator/Cloud/ImageRecompilerCloud.py
```

Al ejecutarse:

1. Verifica credenciales AWS (via STS) y consulta metadata de la instancia EC2 (IMDS v2)
2. Lanza 14 tareas de captura en paralelo (una por camara)
3. Lanza 2 workers S3 que suben imagenes simultaneamente
4. Lanza la tarea de validacion vehicular diaria (se dispara automaticamente 5 min tras el cierre de la ultima planta, lun-sab)
6. Opera continuamente respetando los horarios de cada planta

### Subsistema de reconocimiento vehicular

Los scripts de `scripts/VehicleRecognition/` son independientes del capturador y se ejecutan por separado (ver seccion [Reconocimiento Vehicular](#reconocimiento-vehicular-yolov8) mas abajo).

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

| Planta      | Denominador | Horario Lun-Vie | Horario Sabado |
| ----------- | ----------- | --------------- | -------------- |
| Huechuraba  | HCH         | 07:10 - 16:50   | 07:10 - 16:50  |
| La Florida  | LFL         | 07:40 - 17:20   | 07:10 - 16:50  |
| La Pintana  | LPT         | 07:40 - 17:20   | 07:10 - 16:50  |
| Pudahuel    | PUD         | 07:40 - 17:20   | 07:10 - 16:50  |
| Quilicura   | QLC         | 07:10 - 16:50   | 07:10 - 16:50  |
| Recoleta    | RCL         | 07:40 - 17:20   | 07:10 - 16:50  |
| San Joaquin | SJQ         | 07:40 - 17:20   | 07:10 - 16:50  |

### Region de La Araucania (2 plantas)

| Planta    | Denominador | Horario Lun-Vie | Horario Sabado |
| --------- | ----------- | --------------- | -------------- |
| Temuco    | TMU         | 08:10 - 18:20   | 08:10 - 13:50  |
| Villarica | VLL         | 07:10 - 17:50   | 07:40 - 13:50  |

### Region del Biobio y Nuble (5 plantas)

| Planta              | Denominador | Horario Lun-Vie | Horario Sabado |
| ------------------- | ----------- | --------------- | -------------- |
| Chillan             | CHL         | 06:40 - 17:20   | 07:10 - 13:50  |
| Yungay              | YGY         | 07:40 - 17:20   | 08:10 - 13:50  |
| Concepcion          | CCP         | 07:40 - 20:20   | 08:10 - 16:50  |
| San Pedro de la Paz | SPP         | 07:40 - 17:20   | 08:10 - 13:50  |
| Yumbel              | YMB         | 07:40 - 17:20   | 08:10 - 13:50  |

Los domingos no se realiza captura en ninguna planta.

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

Visualiza el estado del pipeline (stats de subida, metricas periodicas) leyendo los JSONs de stats del dia actual.

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

| Variable              | Default | Descripcion                                   |
| --------------------- | ------- | --------------------------------------------- |
| `DASHBOARD_REFRESH` | `300` | Segundos entre refresh automatico             |
| `GAP_THRESHOLD`     | `180` | Segundos entre capturas para marcar gap       |
| `DOWN_THRESHOLD`    | `900` | Segundos sin captura para marcar planta caida |

El dashboard es read-only sobre S3, no interfiere con el capturador.

### Verificar que el sistema esta capturando

```bash
# Ver fotos de hoy
aws s3 ls s3://flujo-prt-imagenes/capturas/2026/04/19/

# Ver metadata de hoy
aws s3 ls s3://flujo-prt-imagenes/metadata/capturas/2026/04/19/

# Ver JSONL de validacion vehicular de hoy (por planta)
aws s3 ls s3://flujo-prt-imagenes/metadata/capturas/2026/04/19/ --recursive | grep ".jsonl"

# Ver stats del proceso capturador de hoy
aws s3 cp s3://flujo-prt-imagenes/metadata/stats/2026/04/19/resumen.json -
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

## Reconocimiento Vehicular (YOLOv8)

El subsistema de reconocimiento vehicular detecta automaticamente autos, motos, buses y camiones en las imagenes CCTV capturadas, generando metadata enriquecida y visualizaciones con bounding boxes.

### Modulos

| Archivo                   | Descripcion                                                                                                          |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `detector.py`           | Wrapper sobre YOLOv8 (`ultralytics`). Carga el modelo y retorna detecciones `{bbox, tipo, confianza}` por imagen |
| `validate_vehiculos.py` | Recorre un prefijo S3, detecta vehiculos en cada JPG, dibuja bbox sobre las imagenes y escribe un log JSONL          |
| `analisis_historico.py` | Procesamiento en lote del historico: detecta vehiculos en imagenes que aun no tienen deteccion y sube JSONs a S3     |

### Clases detectadas

| Clase COCO | Etiqueta | Color bbox |
| ---------- | -------- | ---------- |
| 2          | auto     | verde      |
| 3          | moto     | azul       |
| 5          | bus      | amarillo   |
| 7          | camion   | rojo       |

Umbral de confianza por defecto: `0.55` (configurable via `UMBRAL_CONFIANZA`).

### validate_vehiculos.py

Procesa todas las imagenes bajo un prefijo S3, genera un JSONL por planta por dia con conteos y detecciones, dibuja bounding boxes y sube imagenes anotadas directamente a S3 (sin escritura local).

La ejecucion es **idempotente**: si ya existe un JSONL parcial de una planta (ejecucion previa interrumpida), carga los registros existentes y solo procesa las imagenes faltantes.

Se dispara automaticamente desde el proceso principal a traves de `tarea_validacion_diaria()`, pero tambien puede ejecutarse manualmente:

```bash
# Procesar capturas de 2026 (configuracion por defecto)
python scripts/VehicleRecognition/validate_vehiculos.py

# Prefijo especifico (dia o mes)
python scripts/VehicleRecognition/validate_vehiculos.py --prefijo capturas/2026/04/25/

# Umbral de confianza para dibujar boxes (0.0 dibuja todos, 0.55 filtra ruido)
python scripts/VehicleRecognition/validate_vehiculos.py --confianza-min 0.55

# Otro bucket
python scripts/VehicleRecognition/validate_vehiculos.py --bucket mi-bucket-pruebas
```

**Variables de entorno:**

| Variable                | Default                | Descripcion                       |
| ----------------------- | ---------------------- | --------------------------------- |
| `S3_BUCKET`           | `flujo-prt-imagenes` | Bucket S3                         |
| `S3_PREFIJO`          | `capturas/2026/`     | Prefijo a procesar                |
| `S3_PREFIJO_ANOTADAS` | `capturas_anotadas`  | Prefijo destino imagenes con bbox |
| `S3_PREFIJO_JSONL`    | `metadata/capturas`  | Prefijo destino JSONL en S3       |

**Ruta del JSONL generado:** `metadata/capturas/YYYY/MM/DD/{Planta}/{DENOM}_YYYYMMDD.jsonl`

**Formato del registro JSONL por imagen:**

```json
{
  "archivo": "HCH_20260419_100523.jpg",
  "s3_key": "capturas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
  "planta_codigo": "HCH",
  "fecha": "2026-04-19",
  "hora": "10:05:23",
  "bytes_archivo": 85432,
  "ancho_px": 1920,
  "alto_px": 1080,
  "conteo": {"auto": 3, "moto": 1, "bus": 0, "camion": 0, "total": 4},
  "detecciones": [{"bbox": [120, 200, 400, 480], "tipo": "auto", "confianza": 0.87}],
  "s3_key_anotada": "capturas_anotadas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
  "procesado_en": "2026-04-19T11:00:00",
  "error": null
}
```

### analisis_historico.py

Procesa el historico de imagenes S3 de forma incremental: omite imagenes que ya tienen un JSON de deteccion en `metadata/detecciones/`, evitando reprocesamiento innecesario.

```bash
# Procesar todas las imagenes pendientes del bucket
python scripts/VehicleRecognition/analisis_historico.py

# Rango de fechas especifico
python scripts/VehicleRecognition/analisis_historico.py \
    --fecha-inicio 2026-04-01 --fecha-fin 2026-04-25

# Solo plantas especificas
python scripts/VehicleRecognition/analisis_historico.py --planta HCH LFL TMU

# Ver que se procesaria sin ejecutar nada
python scripts/VehicleRecognition/analisis_historico.py --dry-run

# Reprocesar aunque ya tengan deteccion
python scripts/VehicleRecognition/analisis_historico.py --forzar
```

**Variables de entorno:**

| Variable             | Default                | Descripcion                     |
| -------------------- | ---------------------- | ------------------------------- |
| `S3_BUCKET`        | `flujo-prt-imagenes` | Bucket S3                       |
| `S3_PREFIX`        | `capturas`           | Prefijo imagenes originales     |
| `METADATA_PREFIX`  | `metadata`           | Prefijo para JSONs de deteccion |
| `MODELO_YOLO`      | `yolov8m.pt`         | Archivo de pesos YOLO           |
| `UMBRAL_CONFIANZA` | `0.55`               | Umbral minimo de confianza      |

Los resultados de deteccion se almacenan en `metadata/detecciones/YYYY/MM/DD/Planta/img.json`.

## Modulos del Sistema

### ImageRecompilerCloud.py

Modulo principal. Orquesta la captura, compresion, deduplicacion y subida de imagenes a S3.

**Funciones principales:**

- `capturar_camara()` - Tarea async por camara, captura cada 60 segundos
- `worker_subida_s3()` - Consumer de la cola, sube imagen + metadata a S3
- `tarea_validacion_diaria()` - Lanza `validate_vehiculos.py` 5 min tras el cierre de la ultima planta cada dia laboral
- `verificar_credenciales_aws()` - Valida acceso AWS antes de iniciar
- `obtener_metadata_ec2()` - Consulta IMDS v2 para obtener info de la instancia (best-effort)
- `dentro_horario()` - Determina si una planta esta en horario de operacion
- `esperar_hasta_apertura()` - Suspende el sistema fuera de horario

### Subsistema de reconocimiento vehicular

| Modulo                                               | Descripcion                                                             |
| ---------------------------------------------------- | ----------------------------------------------------------------------- |
| `scripts/VehicleRecognition/detector.py`           | Carga YOLOv8 y retorna lista de detecciones `{bbox, tipo, confianza}` |
| `scripts/VehicleRecognition/validate_vehiculos.py` | Valida, dibuja bbox y genera log JSONL desde S3                         |
| `scripts/VehicleRecognition/analisis_historico.py` | Procesamiento en lote incremental del historico S3                      |

Ver seccion [Reconocimiento Vehicular](#reconocimiento-vehicular-yolov8) para uso detallado.

## Links de Referencia

- Listado de plantas PRT: https://www.prt.tuv.com/red-de-plantas-revision-tecnicas?page=1
- Estadisticas de flujo vehicular: https://www.prt.cl/Paginas/estadisticas.aspx
- Buscador de plantas: https://www.prt.cl/Paginas/Buscador.aspx
- Base URL de camaras: `https://pti-cameras.cl.tuv.com/camaras/`
- Formato de URL: `https://pti-cameras.cl.tuv.com/camaras/{IP_CamXX}/imagen.jpg`
