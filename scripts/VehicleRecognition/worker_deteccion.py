"""
Worker paralelo de deteccion vehicular para EC2.

Monitorea las imagenes capturadas hoy en S3, ejecuta YOLOv8 sobre las nuevas,
y sube resultados a S3 enriqueciendo la metadata existente.
Corre como proceso independiente en sesion tmux FlujoPRT_Deteccion.

Variables de entorno:
  S3_BUCKET            Bucket S3 (default: flujo-prt-imagenes)
  S3_PREFIX            Prefijo imagenes (default: capturas)
  METADATA_PREFIX      Prefijo metadata (default: metadata)
  INTERVALO_DETECCION  Segundos entre ciclos (default: 120)
  UMBRAL_CONFIANZA     Umbral YOLOv8 (default: 0.55)
  MODELO_YOLO          Archivo de pesos (default: yolov8m.pt)
  TZ                   Zona horaria (default: America/Santiago)
"""

import json
import logging
import os
import signal
import sys
import tempfile
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

# Permite importar detector.py desde el mismo directorio
sys.path.insert(0, str(Path(__file__).resolve().parent))
from detector import cargar_modelo, detectar_vehiculos as _yolo_detectar  # noqa: E402

# ── Configuracion ─────────────────────────────────────────────────────────────
S3_BUCKET           = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIX           = os.getenv("S3_PREFIX", "capturas")
METADATA_PREFIX     = os.getenv("METADATA_PREFIX", "metadata")
INTERVALO_DETECCION = int(os.getenv("INTERVALO_DETECCION", "120"))
UMBRAL_CONFIANZA    = float(os.getenv("UMBRAL_CONFIANZA", "0.55"))
MODELO_YOLO         = os.getenv("MODELO_YOLO", "yolov8m.pt")
_TZ                 = ZoneInfo(os.getenv("TZ", "America/Santiago"))

VERSION = "1"

# ── Logging ───────────────────────────────────────────────────────────────────
_DIR_LOGS = Path(__file__).resolve().parent / "logs"
_DIR_LOGS.mkdir(exist_ok=True)

_LOG_TEXTO = _DIR_LOGS / "acciones_deteccion.log"
_LOG_JSONL = _DIR_LOGS / "acciones_deteccion.jsonl"

logger = logging.getLogger("flujo-prt-deteccion")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

_hdl_consola = logging.StreamHandler()
_hdl_consola.setFormatter(_fmt)

_hdl_archivo = RotatingFileHandler(
    _LOG_TEXTO, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
)
_hdl_archivo.setFormatter(_fmt)

logger.addHandler(_hdl_consola)
logger.addHandler(_hdl_archivo)

# ── Control de apagado ────────────────────────────────────────────────────────
_detener = False


def _manejar_senal(sig, frame):
    global _detener
    _detener = True
    logger.info("Senal %s recibida — terminando al fin del ciclo actual...", sig)


signal.signal(signal.SIGTERM, _manejar_senal)
signal.signal(signal.SIGINT, _manejar_senal)


# ── Log de acciones JSONL ─────────────────────────────────────────────────────
def _registrar_accion(evento: str, **campos):
    """Escribe un evento al JSONL de acciones y al log de texto."""
    entrada = {"timestamp": _ahora_iso(), "evento": evento, **campos}
    with _LOG_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")
    detalle = " | ".join(f"{k}={v}" for k, v in campos.items())
    logger.info("[%s] %s", evento, detalle)


# ── Utilidades de tiempo ──────────────────────────────────────────────────────
def _ahora() -> datetime:
    return datetime.now(_TZ)


def _ahora_iso() -> str:
    return _ahora().isoformat(timespec="seconds")


def _fecha_hoy() -> str:
    """Retorna fecha de hoy en formato YYYY/MM/DD para prefijos S3."""
    return _ahora().strftime("%Y/%m/%d")


# ── Derivacion de claves S3 ───────────────────────────────────────────────────
def _clave_deteccion(clave_imagen: str) -> str:
    """
    capturas/2026/04/26/Huechuraba/HCH_20260426_100523.jpg
    → metadata/detecciones/2026/04/26/Huechuraba/HCH_20260426_100523.json
    """
    p = PurePosixPath(clave_imagen)
    carpeta = str(p.parent.relative_to(S3_PREFIX))
    return f"{METADATA_PREFIX}/detecciones/{carpeta}/{p.stem}.json"


def _clave_metadata_captura(clave_imagen: str) -> str:
    """
    capturas/2026/04/26/Huechuraba/HCH_20260426_100523.jpg
    → metadata/capturas/2026/04/26/Huechuraba/HCH_20260426_100523.json
    """
    p = PurePosixPath(clave_imagen)
    carpeta = str(p.parent.relative_to(S3_PREFIX))
    return f"{METADATA_PREFIX}/capturas/{carpeta}/{p.stem}.json"


def _clave_log_s3(fecha: str) -> str:
    return f"{METADATA_PREFIX}/logs_deteccion/{fecha}/acciones.jsonl"


def _clave_imagen_desde_deteccion(clave_det: str) -> str:
    """
    metadata/detecciones/2026/04/26/Huechuraba/HCH_20260426_100523.json
    → capturas/2026/04/26/Huechuraba/HCH_20260426_100523.jpg
    """
    p = PurePosixPath(clave_det)
    base_detecciones = f"{METADATA_PREFIX}/detecciones"
    carpeta = str(p.parent.relative_to(base_detecciones))
    return f"{S3_PREFIX}/{carpeta}/{p.stem}.jpg"


def _planta_desde_clave(clave: str) -> str:
    """Extrae el nombre de la planta (penultimo segmento de la clave S3)."""
    partes = clave.split("/")
    return partes[-2] if len(partes) >= 2 else ""


# ── Operaciones S3 ────────────────────────────────────────────────────────────
def _listar_claves(s3, prefijo: str) -> set[str]:
    """Lista todas las claves bajo un prefijo. Retorna set vacio si falla."""
    claves = set()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for pagina in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefijo):
            for obj in pagina.get("Contents", []):
                claves.add(obj["Key"])
    except ClientError as e:
        logger.error("Error listando S3 '%s': %s", prefijo, e)
    return claves


def _descargar_a_temp(s3, clave: str) -> str | None:
    """Descarga objeto S3 a un archivo temporal. Retorna ruta o None si falla."""
    try:
        sufijo = PurePosixPath(clave).suffix or ".jpg"
        fd, ruta = tempfile.mkstemp(suffix=sufijo)
        os.close(fd)
        s3.download_file(S3_BUCKET, clave, ruta)
        return ruta
    except ClientError as e:
        logger.error("Error descargando '%s': %s", clave, e)
        return None


def _subir_json(s3, clave: str, datos: dict) -> bool:
    """Sube un dict como JSON a S3. Retorna True si exitoso."""
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=clave,
            Body=json.dumps(datos, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
            StorageClass="INTELLIGENT_TIERING",
        )
        return True
    except ClientError as e:
        logger.error("Error subiendo JSON a '%s': %s", clave, e)
        return False


def _descargar_json(s3, clave: str) -> dict | None:
    """Descarga y parsea un JSON de S3. Retorna None si falla."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=clave)
        return json.loads(resp["Body"].read())
    except (ClientError, json.JSONDecodeError) as e:
        logger.warning("No se pudo descargar JSON '%s': %s", clave, e)
        return None


def _subir_log_s3(s3, fecha: str):
    """Sube el JSONL de acciones local a S3."""
    if not _LOG_JSONL.exists():
        return
    clave = _clave_log_s3(fecha)
    try:
        s3.upload_file(
            str(_LOG_JSONL),
            S3_BUCKET,
            clave,
            ExtraArgs={
                "ContentType": "application/x-ndjson",
                "StorageClass": "INTELLIGENT_TIERING",
            },
        )
        logger.debug("[LOG] Log subido a s3://%s/%s", S3_BUCKET, clave)
    except ClientError as e:
        logger.warning("No se pudo subir log de acciones a S3: %s", e)


# ── Procesamiento de imagen ───────────────────────────────────────────────────
def _procesar_imagen(s3, modelo, clave_imagen: str) -> dict | None:
    """
    Descarga la imagen, corre YOLOv8, sube JSON de deteccion y enriquece
    la metadata de captura existente. Retorna dict con conteo y duracion
    si fue exitoso, None si hubo algun error.
    """
    archivo   = PurePosixPath(clave_imagen).name
    clave_det = _clave_deteccion(clave_imagen)
    clave_meta = _clave_metadata_captura(clave_imagen)
    t_inicio  = time.time()

    ruta_temp = _descargar_a_temp(s3, clave_imagen)
    if ruta_temp is None:
        return None

    try:
        detecciones = _yolo_detectar(modelo, ruta_temp)
    except Exception as e:
        logger.error("Error YOLOv8 en '%s': %s", archivo, e)
        return None
    finally:
        try:
            os.unlink(ruta_temp)
        except OSError:
            pass

    # Conteo por tipo
    conteo = {"auto": 0, "moto": 0, "bus": 0, "camion": 0}
    for d in detecciones:
        if d["tipo"] in conteo:
            conteo[d["tipo"]] += 1
    conteo["total"] = sum(conteo[t] for t in ("auto", "moto", "bus", "camion"))

    # Inferir planta e id del nombre de archivo (ej: HCH_20260426_100523)
    nombre_base = PurePosixPath(clave_imagen).stem
    partes_nombre = nombre_base.split("_", 1)
    planta_id = partes_nombre[0]
    planta_nombre = _planta_desde_clave(clave_imagen)

    timestamp_img = ""
    partes_ts = nombre_base.split("_")
    if len(partes_ts) >= 3:
        try:
            timestamp_img = datetime.strptime(
                f"{partes_ts[1]}_{partes_ts[2]}", "%Y%m%d_%H%M%S"
            ).isoformat()
        except ValueError:
            timestamp_img = nombre_base

    json_deteccion = {
        "version": VERSION,
        "planta_id": planta_id,
        "planta_nombre": planta_nombre,
        "s3_imagen_key": clave_imagen,
        "timestamp_imagen": timestamp_img,
        "conteo": conteo,
        "detecciones": detecciones,
        "modelo_yolo": MODELO_YOLO,
        "umbral_confianza": UMBRAL_CONFIANZA,
        "procesado_en": _ahora_iso(),
    }

    if not _subir_json(s3, clave_det, json_deteccion):
        return None

    # Enriquecer metadata de captura existente (best-effort: si falla, continua)
    meta = _descargar_json(s3, clave_meta)
    if meta is not None:
        meta["detecciones"] = {
            "conteo": conteo,
            "s3_deteccion_key": clave_det,
            "procesado_en": _ahora_iso(),
        }
        if not _subir_json(s3, clave_meta, meta):
            logger.warning("[META] No se pudo enriquecer metadata de '%s'", archivo)

    duracion_ms = int((time.time() - t_inicio) * 1000)
    return {"conteo": conteo, "duracion_ms": duracion_ms}


# ── Ciclo principal ───────────────────────────────────────────────────────────
def ejecutar_ciclo(s3, modelo, fecha: str) -> tuple[int, int, int]:
    """
    Un ciclo completo: detecta imagenes nuevas y verifica eliminaciones.
    Retorna (procesadas, errores, eliminaciones).
    """
    prefijo_imgs = f"{S3_PREFIX}/{fecha}/"
    prefijo_dets = f"{METADATA_PREFIX}/detecciones/{fecha}/"

    claves_imgs = {k for k in _listar_claves(s3, prefijo_imgs) if k.endswith(".jpg")}
    claves_dets = {k for k in _listar_claves(s3, prefijo_dets) if k.endswith(".json")}

    # Imagenes que todavia no tienen deteccion
    pendientes = [
        img for img in claves_imgs
        if _clave_deteccion(img) not in claves_dets
    ]

    _registrar_accion(
        "CICLO_INICIO",
        imagenes_hoy=len(claves_imgs),
        detecciones_existentes=len(claves_dets),
        pendientes=len(pendientes),
    )

    procesadas = 0
    errores = 0

    for clave_img in pendientes:
        if _detener:
            break
        resultado = _procesar_imagen(s3, modelo, clave_img)
        if resultado is not None:
            procesadas += 1
            _registrar_accion(
                "IMAGE_PROCESADA",
                planta=_planta_desde_clave(clave_img),
                archivo=PurePosixPath(clave_img).name,
                s3_key=clave_img,
                conteo=resultado["conteo"],
                duracion_ms=resultado["duracion_ms"],
            )
        else:
            errores += 1
            _registrar_accion(
                "ERROR_DETECCION",
                planta=_planta_desde_clave(clave_img),
                archivo=PurePosixPath(clave_img).name,
                s3_key=clave_img,
            )

    # Detectar imagenes que fueron eliminadas de S3 despues de ser procesadas
    eliminaciones = 0
    for clave_det in claves_dets:
        clave_img_esperada = _clave_imagen_desde_deteccion(clave_det)
        if clave_img_esperada not in claves_imgs:
            eliminaciones += 1
            _registrar_accion(
                "IMAGE_ELIMINADA",
                planta=_planta_desde_clave(clave_det),
                archivo=PurePosixPath(clave_det).stem + ".jpg",
                s3_key_imagen=clave_img_esperada,
                s3_key_deteccion=clave_det,
            )

    _registrar_accion(
        "CICLO_FIN",
        procesadas=procesadas,
        errores=errores,
        eliminaciones=eliminaciones,
    )

    return procesadas, errores, eliminaciones


def main():
    logger.info("=" * 60)
    logger.info("INICIANDO WORKER DETECCION VEHICULAR")
    logger.info("  Bucket   : %s", S3_BUCKET)
    logger.info("  Modelo   : %s", MODELO_YOLO)
    logger.info("  Intervalo: %ds", INTERVALO_DETECCION)
    logger.info("  Umbral   : %.2f", UMBRAL_CONFIANZA)
    logger.info("=" * 60)

    s3 = boto3.client("s3")

    logger.info("Cargando modelo YOLOv8 (%s)...", MODELO_YOLO)
    try:
        modelo = cargar_modelo(MODELO_YOLO)
    except Exception as e:
        logger.error("No se pudo cargar el modelo: %s", e)
        sys.exit(1)
    logger.info("Modelo listo.")

    while not _detener:
        fecha = _fecha_hoy()
        try:
            procesadas, errores, eliminaciones = ejecutar_ciclo(s3, modelo, fecha)
            _subir_log_s3(s3, fecha)
            logger.info(
                "Ciclo completado — procesadas=%d errores=%d eliminaciones=%d"
                " — proxima ejecucion en %ds",
                procesadas,
                errores,
                eliminaciones,
                INTERVALO_DETECCION,
            )
        except Exception as e:
            logger.error("Error inesperado en ciclo: %s", e, exc_info=True)

        # Espera interrumpible segundo a segundo para responder a SIGTERM rapido
        for _ in range(INTERVALO_DETECCION):
            if _detener:
                break
            time.sleep(1)

    _subir_log_s3(s3, _fecha_hoy())
    logger.info("Worker detenido correctamente.")


if __name__ == "__main__":
    main()
