# VehicleRecognition — Deteccion Vehicular en Imagenes CCTV

Subsistema de deteccion de vehiculos basado en **YOLOv8** para el pipeline FlujoPRT. Detecta autos, motos, buses y camiones en imagenes CCTV almacenadas en S3, genera metadata enriquecida y visualizaciones con bounding boxes.

## Descripcion

Este subsistema se ejecuta de forma independiente al capturador principal. Consume las imagenes ya almacenadas en `s3://flujo-prt-imagenes/capturas/` y produce:

- **Imagenes anotadas** con bounding boxes dibujados → `capturas_anotadas/`
- **JSONs de deteccion** por imagen → `metadata/detecciones/`
- **Logs JSONL** con conteos agregados → `metadata/validate_vehiculos/` y `metadata/analisis_historico/`

## Archivos

| Archivo | Descripcion |
|---------|-------------|
| `detector.py` | Wrapper YOLOv8: carga el modelo y retorna detecciones por imagen |
| `validate_vehiculos.py` | Procesamiento con bbox + log JSONL desde un prefijo S3 |
| `analisis_historico.py` | Procesamiento en lote incremental del historico S3 |
| `logs/` | Logs de texto y JSONL generados localmente |

## Instalacion de Dependencias

```bash
pip install ultralytics tqdm Pillow boto3
```

El modelo `yolov8m.pt` (~50 MB) se descarga automaticamente la primera vez que se invoca `cargar_modelo()`.

## Clases Detectadas

| Clase COCO | Etiqueta | Color bbox |
|------------|----------|------------|
| 2 | auto | verde |
| 3 | moto | azul |
| 5 | bus | amarillo |
| 7 | camion | rojo |

Umbral de confianza por defecto: `0.55`.

---

## detector.py

Modulo base de deteccion. Los demas scripts lo importan directamente.

```python
from detector import cargar_modelo, detectar_vehiculos

modelo = cargar_modelo()                     # carga yolov8m.pt
dets = detectar_vehiculos(modelo, "img.jpg") # lista de {bbox, tipo, confianza}
```

**Variables de entorno:** ninguna. El archivo de pesos se configura al llamar a `cargar_modelo(ruta_pesos)`.

---

## validate_vehiculos.py

Recorre un prefijo S3, descarga cada imagen, detecta vehiculos, dibuja bounding boxes y escribe un log JSONL. Opcionalmente sube las imagenes anotadas y el JSONL a S3.

### Uso

```bash
# Configuracion por defecto (bucket=flujo-prt-imagenes, prefijo=capturas/2026/)
python scripts/VehicleRecognition/validate_vehiculos.py

# Prefijo especifico
python scripts/VehicleRecognition/validate_vehiculos.py \
    --prefijo capturas/2026/04/ \
    --salida scripts/VehicleRecognition/logs/abril.jsonl

# Solo JSONL, sin dibujar ni subir imagenes anotadas
python scripts/VehicleRecognition/validate_vehiculos.py --sin-dibujo

# Sin subida a S3 (modo completamente local)
python scripts/VehicleRecognition/validate_vehiculos.py --sin-s3

# Otro bucket
python scripts/VehicleRecognition/validate_vehiculos.py --bucket mi-bucket-pruebas
```

### Argumentos CLI

| Argumento | Default | Descripcion |
|-----------|---------|-------------|
| `--bucket` | `flujo-prt-imagenes` | Bucket S3 |
| `--prefijo` | `capturas/2026/` | Prefijo S3 a recorrer |
| `--salida` | `logs/registro_vehicular.jsonl` | Ruta del JSONL de salida |
| `--destino` | `../../Resultados Captura` | Carpeta raiz para imagenes anotadas locales |
| `--confianza-min` | `0.0` | Umbral minimo de confianza para dibujar box |
| `--sin-dibujo` | false | Omite el dibujado de boxes |
| `--sin-s3` | false | Omite toda subida a S3 |

### Variables de Entorno

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `S3_BUCKET` | `flujo-prt-imagenes` | Bucket S3 |
| `S3_PREFIJO` | `capturas/2026/` | Prefijo de imagenes a procesar |
| `S3_PREFIJO_ANOTADAS` | `capturas_anotadas` | Destino S3 de imagenes con bbox |
| `S3_PREFIJO_LOGS` | `metadata/validate_vehiculos` | Destino S3 del JSONL |

### Formato del registro JSONL

Cada linea del archivo de salida corresponde a una imagen procesada:

```json
{
  "archivo": "HCH_20260419_100523.jpg",
  "s3_key": "capturas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
  "planta_codigo": "HCH",
  "fecha": "2026-04-19",
  "hora": "10:05:23",
  "timestamp_imagen": "2026-04-19T10:05:23",
  "bytes_archivo": 85432,
  "ancho_px": 1920,
  "alto_px": 1080,
  "conteo": {"auto": 3, "moto": 1, "bus": 0, "camion": 0, "total": 4},
  "detecciones": [
    {"bbox": [120.0, 200.0, 400.0, 480.0], "tipo": "auto", "confianza": 0.87}
  ],
  "procesado_en": "2026-04-19T11:00:00",
  "s3_key_anotada": "capturas_anotadas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
  "error": null
}
```

---

## analisis_historico.py

Procesamiento incremental del historico en S3. Detecta vehiculos en las imagenes que aun no tienen un JSON de deteccion en `metadata/detecciones/`, evitando reprocesar lo ya analizado.

Ademas de crear el JSON de deteccion, enriquece el JSON de metadata de captura existente agregando un campo `detecciones` con el conteo y la referencia al JSON de deteccion.

### Uso

```bash
# Procesar todo el bucket (solo lo pendiente)
python scripts/VehicleRecognition/analisis_historico.py

# Rango de fechas
python scripts/VehicleRecognition/analisis_historico.py \
    --fecha-inicio 2026-04-01 --fecha-fin 2026-04-25

# Solo plantas especificas
python scripts/VehicleRecognition/analisis_historico.py --planta HCH LFL TMU

# Ver que se procesaria sin ejecutar nada
python scripts/VehicleRecognition/analisis_historico.py --dry-run

# Reprocesar aunque ya tengan JSON de deteccion
python scripts/VehicleRecognition/analisis_historico.py --forzar

# Usar un modelo distinto
python scripts/VehicleRecognition/analisis_historico.py --modelo yolov8l.pt
```

### Argumentos CLI

| Argumento | Default | Descripcion |
|-----------|---------|-------------|
| `--fecha-inicio` | (todo el bucket) | Fecha inicio del rango `YYYY-MM-DD` |
| `--fecha-fin` | hoy | Fecha fin del rango `YYYY-MM-DD` |
| `--planta` | todas | Uno o mas codigos de planta (ej: `HCH LFL`) |
| `--forzar` | false | Reprocesar imagenes que ya tienen deteccion |
| `--dry-run` | false | Listar pendientes sin procesar ni subir |
| `--modelo` | `yolov8m.pt` | Archivo de pesos YOLO |

### Variables de Entorno

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `S3_BUCKET` | `flujo-prt-imagenes` | Bucket S3 |
| `S3_PREFIX` | `capturas` | Prefijo de imagenes originales |
| `METADATA_PREFIX` | `metadata` | Prefijo para JSONs de deteccion y resumenes |
| `MODELO_YOLO` | `yolov8m.pt` | Archivo de pesos YOLO |
| `UMBRAL_CONFIANZA` | `0.55` | Umbral minimo de confianza |
| `TZ` | `America/Santiago` | Zona horaria para timestamps de procesamiento |

### Salidas en S3

| Prefijo | Contenido |
|---------|-----------|
| `metadata/detecciones/YYYY/MM/DD/Planta/img.json` | Detecciones YOLOv8 por imagen |
| `metadata/analisis_historico/resumen_TS.json` | Resumen agregado de la ejecucion |
| `metadata/analisis_historico/acciones_TS.jsonl` | Log de acciones por imagen |

### Formato JSON de deteccion

```json
{
  "version": "1",
  "planta_id": "HCH",
  "planta_nombre": "Huechuraba",
  "s3_imagen_key": "capturas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
  "timestamp_imagen": "2026-04-19T10:05:23",
  "conteo": {"auto": 2, "moto": 0, "bus": 0, "camion": 1, "total": 3},
  "detecciones": [
    {"bbox": [120.0, 200.0, 400.0, 480.0], "tipo": "auto", "confianza": 0.91},
    {"bbox": [600.0, 150.0, 950.0, 510.0], "tipo": "camion", "confianza": 0.78}
  ],
  "modelo_yolo": "yolov8m.pt",
  "umbral_confianza": 0.55,
  "procesado_en": "2026-04-19T11:00:00-04:00"
}
```

---

## Ejecucion en EC2

Para procesar el historico completo en una sesion tmux:

```bash
tmux new -s VehicleRecognition
cd ~/FlujoPRT

# Analisis historico (modo incremental, solo lo pendiente)
python3 scripts/VehicleRecognition/analisis_historico.py \
    --fecha-inicio 2026-04-01

# Desconectarse sin matar el proceso: Ctrl+B, luego D
tmux attach -t VehicleRecognition
```

---

## Logs Locales

Los logs de texto y JSONL se generan en `scripts/VehicleRecognition/logs/`:

| Archivo | Descripcion |
|---------|-------------|
| `validate_vehiculos.log` | Log de texto con rotacion (5 MB x 3 archivos) |
| `registro_vehicular.jsonl` | Salida JSONL del ultimo `validate_vehiculos.py` |
| `analisis_historico.log` | Log de texto del analisis historico |
| `analisis_historico.jsonl` | Log de acciones del analisis historico |
