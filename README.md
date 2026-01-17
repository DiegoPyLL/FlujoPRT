# FlujoPRT

    Sistema automatizado para capturar imágenes desde cámaras públicas de plantas de revisión técnica, generar timelapses diarios y aplicar análisis visual con reconocimiento vehicular.

**Tópicos:** `computer-vision` `image-capture` `timelapse` `vehicle-recognition` `aws` `cloud-processing` `automation` `python` `opencv`

## Características

    Captura paralela de múltiples plantas con`asyncio` y `aiohttp`. Compresión JPEG optimizada con calidad configurable y eliminación de metadata. Detección de duplicados por hash MD5 antes de subir a S3. Almacenamiento particionado por fecha en AWS S3. Respeta horarios de operación (lunes-sábado, omite domingos). Backoff exponencial en errores y métricas en memoria. Compatible con `uvloop` en Linux. Control de concurrencia con semáforos y colas.

## Instalación

```bash
git clone https://github.com/tu_usuario/FlujoPRT.git
cd FlujoPRT
pip install -r requirements.txt
```

## Configuración

Variables de entorno:

* `S3_BUCKET`: Bucket S3 (default: `flujo-prt-imagenes`)
* `S3_PREFIX`: Prefijo de almacenamiento (default: `capturas`)
* `INTERVALO`: Segundos entre capturas (default: `60`)
* `TZ`: Zona horaria (default: `America/Santiago`)
* `JPEG_QUALITY`: Calidad JPEG 0-100 (default: `75`)
* `MAX_DESCARGAS`: Descargas simultáneas (default: `10`)
* `QUEUE_SIZE`: Tamaño de cola de subida (default: `100`)
* `NUM_UPLOADERS`: Workers de subida S3 (default: `3`)
* `METRICAS_INTERVALO`: Intervalo de métricas en segundos (default: `900`)

## Cámaras y horarios

### Región Metropolitana

| Planta       | URL                                                                 | Lun-Vie     | Sábado     |
| ------------ | ------------------------------------------------------------------- | ----------- | ----------- |
| Huechuraba   | https://pti-cameras.cl.tuv.com/camaras/10.57.6.222_Cam08/imagen.jpg | 07:30-16:30 | 07:30-16:30 |
| La Florida   | https://pti-cameras.cl.tuv.com/camaras/10.57.0.222_Cam03/imagen.jpg | 08:00-17:00 | 07:30-16:30 |
| La Pintana   | https://pti-cameras.cl.tuv.com/camaras/10.57.5.222_Cam09/imagen.jpg | 08:00-17:00 | 07:30-16:30 |
| Pudahuel     | https://pti-cameras.cl.tuv.com/camaras/10.57.4.222_Cam07/imagen.jpg | 08:00-17:00 | 07:30-16:30 |
| Quilicura    | https://pti-cameras.cl.tuv.com/camaras/10.57.2.222_Cam06/imagen.jpg | 07:30-16:30 | 07:30-16:30 |
| Recoleta     | https://pti-cameras.cl.tuv.com/camaras/10.57.7.222_Cam09/imagen.jpg | 08:00-17:00 | 07:30-16:30 |
| San Joaquín | https://pti-cameras.cl.tuv.com/camaras/10.57.3.222_Cam07/imagen.jpg | 08:00-17:00 | 07:30-16:30 |

### Región de La Araucanía

| Planta    | URL                                                                  | Lun-Vie     | Sábado     |
| --------- | -------------------------------------------------------------------- | ----------- | ----------- |
| Temuco    | https://pti-cameras.cl.tuv.com/camaras/10.57.32.222_Cam01/imagen.jpg | 08:30-18:00 | 08:30-13:30 |
| Villarica | https://pti-cameras.cl.tuv.com/camaras/10.57.33.222_Cam04/imagen.jpg | 07:30-17:30 | 08:00-13:30 |

### Región del Biobío

| Planta              | URL                                                           | Lun-Vie                    | Sábado                    | Notas                    |
| ------------------- | ------------------------------------------------------------- | -------------------------- | -------------------------- | ------------------------ |
| Chillán            | https://pti-cameras.cl.tuv.com/camaras/10.57.12.70/imagen.jpg | 07:00-17:00                | 07:30-13:30                | 07:00-08:00 solo clase B |
| Yungay              | https://pti-cameras.cl.tuv.com/camaras/10.57.20.70/imagen.jpg | 08:00-17:00                | 08:30-13:30                | -                        |
| Concepción         | https://pti-cameras.cl.tuv.com/camaras/10.57.19.70/imagen.jpg | 08:00-20:00 / 08:00-16:30* | 08:30-16:30 / 08:30-13:00* | *Camiones, taxis, buses  |
| San Pedro de la Paz | https://pti-cameras.cl.tuv.com/camaras/10.57.16.70/imagen.jpg | 08:00-17:00                | 08:30-13:30                | -                        |
| Yumbel              | https://pti-cameras.cl.tuv.com/camaras/10.57.17.70/imagen.jpg | 08:00-17:00                | 08:30-13:30                | -                        |

### No funcionales

Castro y Osorno no tienen cámaras disponibles.


Listado de plantas general
https://www.prt.tuv.com/red-de-plantas-revision-tecnicas?page=1

Link base cámaras
Base: https://pti-cameras.cl.tuv.com/camaras/

Identificador IP: 10.57.x.xxx_CamXX
Nombre imagen cacheada: imagen.jpg
