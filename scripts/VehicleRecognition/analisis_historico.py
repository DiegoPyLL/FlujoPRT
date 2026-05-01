"""
Analisis historico de imagenes en S3.

Procesa las imagenes del bucket que aun no tienen deteccion vehicular,
generando los JSONs de deteccion y enriqueciendo la metadata existente.
Ideal para poner al dia el backlog historico antes de activar el worker
continuo (worker_deteccion.py).

Uso:
  python3 scripts/VehicleRecognition/analisis_historico.py
  python3 scripts/VehicleRecognition/analisis_historico.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-25
  python3 scripts/VehicleRecognition/analisis_historico.py --planta HCH LFL
  python3 scripts/VehicleRecognition/analisis_historico.py --dry-run
  python3 scripts/VehicleRecognition/analisis_historico.py --forzar

Variables de entorno:
  S3_BUCKET        Bucket S3 (default: flujo-prt-imagenes)
  S3_PREFIX        Prefijo imagenes (default: capturas)
  METADATA_PREFIX  Prefijo metadata (default: metadata)
  MODELO_YOLO      Archivo de pesos (default: yolov8m.pt)
  UMBRAL_CONFIANZA Umbral de confianza YOLOv8 (default: 0.55)
  TZ               Zona horaria (default: America/Santiago)
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import date, timedelta, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from detector import cargar_modelo, detectar_vehiculos as _yolo_detectar  # noqa: E402

# ── Configuracion ─────────────────────────────────────────────────────────────
S3_BUCKET       = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIX       = os.getenv("S3_PREFIX", "capturas")
METADATA_PREFIX = os.getenv("METADATA_PREFIX", "metadata")
MODELO_YOLO     = os.getenv("MODELO_YOLO", "yolov8m.pt")
UMBRAL_CONF     = float(os.getenv("UMBRAL_CONFIANZA", "0.55"))
_TZ             = ZoneInfo(os.getenv("TZ", "America/Santiago"))

VERSION = "1.0.0"

# ── Logging ───────────────────────────────────────────────────────────────────
_DIR_LOGS = Path(__file__).resolve().parent / "logs"
_DIR_LOGS.mkdir(exist_ok=True)

_LOG_TEXTO = _DIR_LOGS / "analisis_historico.log"
_LOG_JSONL = _DIR_LOGS / "analisis_historico.jsonl"

logger = logging.getLogger("flujo-prt-historico")
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


# ── Utilidades ────────────────────────────────────────────────────────────────
def _ahora() -> datetime:
    return datetime.now(_TZ)


def _ahora_iso() -> str:
    return _ahora().isoformat(timespec="seconds")


def _registrar_evento(evento: str, **campos):
    entrada = {"timestamp": _ahora_iso(), "evento": evento, **campos}
    with _LOG_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")


def _fechas_en_rango(inicio: date, fin: date) -> list[date]:
    dias = []
    actual = inicio
    while actual <= fin:
        dias.append(actual)
        actual += timedelta(days=1)
    return dias


# ── Derivacion de claves S3 ───────────────────────────────────────────────────
def _clave_deteccion(clave_imagen: str) -> str:
    p = PurePosixPath(clave_imagen)
    carpeta = str(p.parent.relative_to(S3_PREFIX))
    return f"{METADATA_PREFIX}/detecciones/{carpeta}/{p.stem}.json"


def _planta_id_desde_clave(clave: str) -> str:
    return PurePosixPath(clave).stem.split("_")[0]


def _planta_nombre_desde_clave(clave: str) -> str:
    partes = clave.split("/")
    return partes[-2] if len(partes) >= 2 else ""


def _fecha_desde_clave(clave: str) -> str:
    """Extrae YYYY-MM-DD de capturas/YYYY/MM/DD/... → 'YYYY-MM-DD'."""
    partes = clave.split("/")
    if len(partes) >= 4:
        return f"{partes[1]}-{partes[2]}-{partes[3]}"
    return ""


# ── Operaciones S3 ────────────────────────────────────────────────────────────
def _listar_imagenes(s3, prefijos: list[str]) -> list[str]:
    """Lista todas las claves .jpg bajo los prefijos dados."""
    claves = []
    for prefijo in prefijos:
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for pagina in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefijo):
                for obj in pagina.get("Contents", []):
                    if obj["Key"].endswith(".jpg"):
                        claves.append(obj["Key"])
        except ClientError as e:
            logger.error("Error listando '%s': %s", prefijo, e)
    return claves


def _existe_clave(s3, clave: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=clave)
        return True
    except ClientError:
        return False


def _descargar_a_temp(s3, clave: str) -> str | None:
    try:
        fd, ruta = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        s3.download_file(S3_BUCKET, clave, ruta)
        return ruta
    except ClientError as e:
        logger.error("Error descargando '%s': %s", PurePosixPath(clave).name, e)
        return None


def _subir_json(s3, clave: str, datos: dict) -> bool:
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
        logger.error("Error subiendo '%s': %s", clave, e)
        return False


def _descargar_json(s3, clave: str) -> dict | None:
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=clave)
        return json.loads(resp["Body"].read())
    except (ClientError, json.JSONDecodeError):
        return None


# ── Procesamiento de una imagen ───────────────────────────────────────────────
def _procesar_imagen(s3, modelo, clave_imagen: str) -> dict | None:
    """Detecta vehiculos en una imagen S3. Sube JSON de deteccion. Retorna dict con resultados o None si hubo error."""
    archivo   = PurePosixPath(clave_imagen).name
    clave_det = _clave_deteccion(clave_imagen)
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

    conteo = {"auto": 0, "moto": 0, "bus": 0, "camion": 0}
    for d in detecciones:
        if d["tipo"] in conteo:
            conteo[d["tipo"]] += 1
    conteo["total"] = sum(conteo[t] for t in ("auto", "moto", "bus", "camion"))

    nombre_base = PurePosixPath(clave_imagen).stem
    planta_id   = _planta_id_desde_clave(clave_imagen)
    planta_nombre = _planta_nombre_desde_clave(clave_imagen)

    timestamp_img = ""
    partes_ts = nombre_base.split("_")
    if len(partes_ts) >= 3:
        try:
            timestamp_img = datetime.strptime(
                f"{partes_ts[1]}_{partes_ts[2]}", "%Y%m%d_%H%M%S"
            ).isoformat()
        except ValueError:
            timestamp_img = nombre_base

    json_det = {
        "version": VERSION,
        "planta_id": planta_id,
        "planta_nombre": planta_nombre,
        "s3_imagen_key": clave_imagen,
        "timestamp_imagen": timestamp_img,
        "conteo": conteo,
        "detecciones": detecciones,
        "modelo_yolo": MODELO_YOLO,
        "umbral_confianza": UMBRAL_CONF,
        "procesado_en": _ahora_iso(),
    }

    if not _subir_json(s3, clave_det, json_det):
        return None

    return {
        "conteo": conteo,
        "detecciones": len(detecciones),
        "duracion_ms": int((time.time() - t_inicio) * 1000),
        "planta_id": planta_id,
        "planta_nombre": planta_nombre,
        "fecha": _fecha_desde_clave(clave_imagen),
    }


# ── Resumen ────────────────────────────────────────────────────────────────────
class Acumulador:
    def __init__(self):
        self.procesadas  = 0
        self.omitidas    = 0
        self.errores     = 0
        self.por_planta: dict[str, dict] = {}

    def registrar(self, planta_id: str, planta_nombre: str, conteo: dict, duracion_ms: int):
        self.procesadas += 1
        if planta_id not in self.por_planta:
            self.por_planta[planta_id] = {
                "nombre": planta_nombre,
                "imagenes": 0,
                "auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0,
            }
        p = self.por_planta[planta_id]
        p["imagenes"] += 1
        for tipo in ("auto", "moto", "bus", "camion", "total"):
            p[tipo] += conteo.get(tipo, 0)

    def totales_vehiculos(self) -> dict:
        tot = {"auto": 0, "moto": 0, "bus": 0, "camion": 0, "total": 0}
        for p in self.por_planta.values():
            for tipo in tot:
                tot[tipo] += p[tipo]
        return tot

    def imprimir(self):
        tot = self.totales_vehiculos()
        logger.info("=" * 60)
        logger.info("RESUMEN FINAL")
        logger.info("  Imagenes procesadas : %d", self.procesadas)
        logger.info("  Omitidas (ya tenian): %d", self.omitidas)
        logger.info("  Errores             : %d", self.errores)
        logger.info("  Autos detectados    : %d", tot["auto"])
        logger.info("  Motos detectadas    : %d", tot["moto"])
        logger.info("  Buses detectados    : %d", tot["bus"])
        logger.info("  Camiones detectados : %d", tot["camion"])
        logger.info("  Total vehiculos     : %d", tot["total"])
        if self.por_planta:
            logger.info("  Por planta:")
            for pid, datos in sorted(self.por_planta.items()):
                logger.info(
                    "    %-6s %-20s  imgs=%d  veh=%d",
                    pid, datos["nombre"], datos["imagenes"], datos["total"],
                )
        logger.info("=" * 60)


# ── Subida de resumen a S3 ────────────────────────────────────────────────────
def _subir_resumen(s3, acum: Acumulador, args, t_total_s: float):
    ts = _ahora().strftime("%Y%m%d_%H%M%S")
    clave_resumen = f"{METADATA_PREFIX}/analisis_historico/resumen_{ts}.json"
    clave_log     = f"{METADATA_PREFIX}/analisis_historico/acciones_{ts}.jsonl"

    resumen = {
        "version": VERSION,
        "ejecutado_en": _ahora_iso(),
        "parametros": {
            "fecha_inicio": str(args.fecha_inicio) if args.fecha_inicio else None,
            "fecha_fin":    str(args.fecha_fin)    if args.fecha_fin    else None,
            "plantas":      args.planta            if args.planta       else None,
            "forzar":       args.forzar,
            "dry_run":      args.dry_run,
        },
        "resultados": {
            "procesadas": acum.procesadas,
            "omitidas":   acum.omitidas,
            "errores":    acum.errores,
            "duracion_total_s": round(t_total_s, 1),
            "vehiculos": acum.totales_vehiculos(),
        },
        "por_planta": acum.por_planta,
    }

    if _subir_json(s3, clave_resumen, resumen):
        logger.info("[S3] Resumen subido → s3://%s/%s", S3_BUCKET, clave_resumen)

    if _LOG_JSONL.exists():
        try:
            s3.upload_file(
                str(_LOG_JSONL), S3_BUCKET, clave_log,
                ExtraArgs={"ContentType": "application/x-ndjson",
                           "StorageClass": "INTELLIGENT_TIERING"},
            )
            logger.info("[S3] Log de acciones subido → s3://%s/%s", S3_BUCKET, clave_log)
        except ClientError as e:
            logger.warning("No se pudo subir log a S3: %s", e)


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args():
    parser = argparse.ArgumentParser(
        description="Analisis historico de imagenes CCTV en S3 con YOLOv8."
    )
    parser.add_argument(
        "--fecha-inicio",
        metavar="YYYY-MM-DD",
        type=date.fromisoformat,
        help="Fecha de inicio del rango (inclusive). Sin valor: desde el principio del bucket.",
    )
    parser.add_argument(
        "--fecha-fin",
        metavar="YYYY-MM-DD",
        type=date.fromisoformat,
        default=date.today(),
        help="Fecha de fin del rango (inclusive). Default: hoy.",
    )
    parser.add_argument(
        "--planta",
        nargs="+",
        metavar="CODIGO",
        help="Filtrar por uno o mas codigos de planta (ej: HCH LFL TMU).",
    )
    parser.add_argument(
        "--forzar",
        action="store_true",
        help="Reprocesar imagenes que ya tienen deteccion en S3.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Listar imagenes pendientes sin procesar ni subir nada.",
    )
    parser.add_argument(
        "--modelo",
        default=MODELO_YOLO,
        metavar="ARCHIVO",
        help=f"Archivo de pesos YOLO (default: {MODELO_YOLO}).",
    )
    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    args = _parse_args()

    logger.info("=" * 60)
    logger.info("ANALISIS HISTORICO DETECCION VEHICULAR  v%s", VERSION)
    logger.info("  Bucket        : %s", S3_BUCKET)
    logger.info("  Modelo        : %s", args.modelo)
    logger.info("  Fecha inicio  : %s", args.fecha_inicio or "(todo el bucket)")
    logger.info("  Fecha fin     : %s", args.fecha_fin)
    logger.info("  Plantas       : %s", ", ".join(args.planta) if args.planta else "todas")
    logger.info("  Forzar        : %s", args.forzar)
    logger.info("  Dry-run       : %s", args.dry_run)
    logger.info("=" * 60)

    s3 = boto3.client("s3")

    # Construir prefijos a listar segun rango de fechas
    if args.fecha_inicio:
        dias = _fechas_en_rango(args.fecha_inicio, args.fecha_fin)
        prefijos = [f"{S3_PREFIX}/{d.strftime('%Y/%m/%d')}/" for d in dias]
        logger.info("Listando %d dia(s) en S3...", len(dias))
    else:
        prefijos = [f"{S3_PREFIX}/"]
        logger.info("Listando todo el bucket bajo '%s/'...", S3_PREFIX)

    logger.info("Buscando imagenes en S3...")
    todas_las_imagenes = _listar_imagenes(s3, prefijos)

    # Filtrar por planta si se especifico
    if args.planta:
        codigos = {c.upper() for c in args.planta}
        todas_las_imagenes = [
            k for k in todas_las_imagenes
            if _planta_id_desde_clave(k) in codigos
        ]

    logger.info("Imagenes encontradas: %d", len(todas_las_imagenes))

    if not todas_las_imagenes:
        logger.info("Nada que procesar.")
        return

    # Filtrar las que ya tienen deteccion (a menos que --forzar)
    if args.forzar:
        pendientes = todas_las_imagenes
        logger.info("Modo --forzar: procesando todas (%d)", len(pendientes))
    else:
        logger.info("Verificando cuales ya tienen deteccion en S3...")
        pendientes = []
        omitidas   = 0
        # Barra de progreso para la verificacion (puede ser lenta con muchas imagenes)
        for clave in tqdm(todas_las_imagenes, desc="Verificando", unit="img", ncols=80):
            if not _existe_clave(s3, _clave_deteccion(clave)):
                pendientes.append(clave)
            else:
                omitidas += 1
        logger.info(
            "Pendientes: %d | Ya procesadas (omitidas): %d",
            len(pendientes), omitidas,
        )

    if args.dry_run:
        logger.info("--- DRY RUN: las siguientes imagenes serian procesadas ---")
        for k in pendientes:
            logger.info("  %s", k)
        logger.info("Total: %d imagenes", len(pendientes))
        return

    if not pendientes:
        logger.info("Todas las imagenes ya tienen deteccion. Nada que hacer.")
        return

    # Cargar modelo
    logger.info("Cargando modelo YOLOv8 (%s)...", args.modelo)
    try:
        modelo = cargar_modelo(args.modelo)
    except Exception as e:
        logger.error("No se pudo cargar el modelo: %s", e)
        sys.exit(1)
    logger.info("Modelo listo. Iniciando procesamiento...")

    acum = Acumulador()
    if not args.forzar:
        acum.omitidas = omitidas

    t_inicio_total = time.time()

    _registrar_evento("INICIO", total_pendientes=len(pendientes), forzar=args.forzar)

    barra = tqdm(pendientes, desc="Procesando", unit="img", ncols=80)
    for clave_img in barra:
        archivo = PurePosixPath(clave_img).name
        barra.set_postfix({"img": archivo[:30]})

        resultado = _procesar_imagen(s3, modelo, clave_img)

        if resultado is not None:
            acum.registrar(
                resultado["planta_id"],
                resultado["planta_nombre"],
                resultado["conteo"],
                resultado["duracion_ms"],
            )
            _registrar_evento(
                "IMAGE_PROCESADA",
                planta=resultado["planta_id"],
                archivo=archivo,
                s3_key=clave_img,
                conteo=resultado["conteo"],
                duracion_ms=resultado["duracion_ms"],
            )
        else:
            acum.errores += 1
            _registrar_evento(
                "ERROR_DETECCION",
                planta=_planta_id_desde_clave(clave_img),
                archivo=archivo,
                s3_key=clave_img,
            )

    t_total = time.time() - t_inicio_total

    _registrar_evento(
        "FIN",
        procesadas=acum.procesadas,
        omitidas=acum.omitidas,
        errores=acum.errores,
        duracion_total_s=round(t_total, 1),
        vehiculos=acum.totales_vehiculos(),
    )

    acum.imprimir()
    logger.info("Tiempo total: %.1f segundos (%.1f img/min)", t_total, acum.procesadas / (t_total / 60) if t_total > 0 else 0)

    _subir_resumen(s3, acum, args, t_total)


if __name__ == "__main__":
    main()
