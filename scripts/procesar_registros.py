"""
Procesamiento y validacion de registros vehiculares JSONL desde S3.

Descarga los JSONL de detecciones (metadata/capturas/YYYY/MM/DD/{Planta}/)
hacia data/raw/, limpia, valida y transforma cada registro, y guarda:
  - Validos    : data/processed/validos/registros_YYYYMMDD.jsonl
  - Rechazados : data/processed/rechazados/rechazados_YYYYMMDD.jsonl
  - Reporte    : data/reports/reporte_YYYYMMDD_HHMMSS.json
  - Log        : data/reports/proceso_YYYYMMDD.log

Uso:
    python scripts/procesar_registros.py
    python scripts/procesar_registros.py --prefijo capturas/2026/04/25/
    python scripts/procesar_registros.py --solo-local
    python scripts/procesar_registros.py --bucket otro-bucket --prefijo capturas/2026/
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------------------------------------------------------
# Rutas base
# ---------------------------------------------------------------------------

DIR_BASE      = Path(__file__).resolve().parent.parent / "data"
DIR_RAW       = DIR_BASE / "raw"
DIR_PROCESSED = DIR_BASE / "processed"
DIR_REPORTS   = DIR_BASE / "reports"

S3_BUCKET       = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
S3_PREFIJO_META = os.getenv("S3_PREFIJO_META", "metadata/capturas")

# Copia de DENOMINADORES de validate_vehiculos.py — no se importa ese módulo
# para evitar cargar YOLOv8 y sus dependencias pesadas al iniciar este script.
DENOMINADORES: dict[str, str] = {
    "Huechuraba":          "HCH",
    "La Florida":          "LFL",
    "La Pintana":          "LPT",
    "Pudahuel":            "PUD",
    "Quilicura":           "QLC",
    "Recoleta":            "RCL",
    "San Joaquin":         "SJQ",
    "Temuco":              "TMU",
    "Villarica":           "VLL",
    "Chillan":             "CHL",
    "Yungay":              "YGY",
    "Concepcion":          "CCP",
    "San Pedro de la Paz": "SPP",
    "Yumbel":              "YMB",
}

TIPOS_VEHICULO     = {"auto", "moto", "bus", "camion"}
CAMPOS_OBLIGATORIOS = ("s3_key", "planta", "fecha", "hora", "conteo", "detecciones")

RE_FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RE_HORA  = re.compile(r"^\d{2}:\d{2}:\d{2}$")


# =============================================================================
# Logger
# =============================================================================

def configurar_logger(ruta_log: Path) -> logging.Logger:
    logger = logging.getLogger("flujo-prt-procesamiento")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        fh = logging.FileHandler(ruta_log, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# =============================================================================
# Carga desde S3 o disco
# =============================================================================

def descargar_jsonl_desde_s3(
    s3_client,
    bucket: str,
    prefijo_capturas: str,
    dir_raw: Path,
    logger: logging.Logger,
) -> list[Path]:
    """Lista objetos en metadata/capturas/{sufijo}/ y descarga los .jsonl a data/raw/."""
    sufijo = prefijo_capturas.removeprefix("capturas/").strip("/")
    prefijo_meta = f"metadata/capturas/{sufijo}/" if sufijo else "metadata/capturas/"

    logger.info("[S3] Listando JSONL en s3://%s/%s ...", bucket, prefijo_meta)
    rutas_descargadas: list[Path] = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for pagina in paginator.paginate(Bucket=bucket, Prefix=prefijo_meta):
            for obj in pagina.get("Contents", []):
                clave = obj["Key"]
                if not clave.endswith(".jsonl"):
                    continue
                ruta_local = dir_raw / clave
                ruta_local.parent.mkdir(parents=True, exist_ok=True)
                if ruta_local.exists():
                    logger.info("[S3] Ya existe localmente, omitiendo: %s", ruta_local.name)
                    rutas_descargadas.append(ruta_local)
                    continue
                try:
                    resp = s3_client.get_object(Bucket=bucket, Key=clave)
                    ruta_local.write_bytes(resp["Body"].read())
                    logger.info("[S3] Descargado: %s", clave)
                    rutas_descargadas.append(ruta_local)
                except (BotoCoreError, ClientError) as exc:
                    logger.warning("[S3] No se pudo descargar '%s': %s", clave, exc)
    except (BotoCoreError, ClientError) as exc:
        logger.error("[S3] Error al listar objetos: %s", exc)

    return rutas_descargadas


def cargar_registros_jsonl(rutas: list[Path], logger: logging.Logger) -> list[dict]:
    """Lee cada archivo JSONL línea por línea y retorna lista de registros."""
    registros: list[dict] = []
    for ruta in rutas:
        try:
            lineas = ruta.read_text(encoding="utf-8").splitlines()
            cargados = 0
            for i, linea in enumerate(lineas, 1):
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    registros.append(json.loads(linea))
                    cargados += 1
                except json.JSONDecodeError as exc:
                    logger.warning("[CARGA] Línea %d malformada en %s: %s", i, ruta.name, exc)
            logger.info("[CARGA] %s → %d registros", ruta.name, cargados)
        except OSError as exc:
            logger.warning("[CARGA] No se pudo leer %s: %s", ruta, exc)
    logger.info("[CARGA] Total: %d registros de %d archivos", len(registros), len(rutas))
    return registros


# =============================================================================
# Limpieza
# =============================================================================

def _rechazar(registro: dict, motivo: str) -> dict:
    r = dict(registro)
    r["motivo_rechazo"] = motivo
    return r


def limpiar_errores(registros: list[dict], logger: logging.Logger) -> tuple[list[dict], list[dict]]:
    """Rechaza registros cuyo campo 'error' no sea None."""
    validos, rechazados = [], []
    for r in registros:
        if r.get("error") is not None:
            rechazados.append(_rechazar(r, f"error_deteccion: {r['error']}"))
        else:
            validos.append(r)
    logger.info("[LIMPIEZA] Errores de detección: %d rechazados", len(rechazados))
    return validos, rechazados


def limpiar_campos_nulos(registros: list[dict], logger: logging.Logger) -> tuple[list[dict], list[dict]]:
    """Rechaza registros con algún campo obligatorio ausente o vacío."""
    validos, rechazados = [], []
    for r in registros:
        campo_nulo = next((c for c in CAMPOS_OBLIGATORIOS if not r.get(c) and r.get(c) != 0), None)
        if campo_nulo:
            rechazados.append(_rechazar(r, f"campo_nulo: {campo_nulo}"))
        else:
            validos.append(r)
    logger.info("[LIMPIEZA] Campos nulos: %d rechazados", len(rechazados))
    return validos, rechazados


def limpiar_duplicados(registros: list[dict], logger: logging.Logger) -> tuple[list[dict], list[dict]]:
    """Rechaza registros con s3_key duplicada; conserva la primera ocurrencia."""
    validos, rechazados = [], []
    vistos: set[str] = set()
    for r in registros:
        clave = r.get("s3_key", "")
        if clave in vistos:
            rechazados.append(_rechazar(r, "duplicado_s3_key"))
        else:
            vistos.add(clave)
            validos.append(r)
    logger.info("[LIMPIEZA] Duplicados: %d rechazados", len(rechazados))
    return validos, rechazados


# =============================================================================
# Transformaciones
# =============================================================================

def _hora_categoria(hora_str: str) -> str:
    try:
        h = int(hora_str[:2])
    except (ValueError, TypeError):
        return "fuera_horario"
    if 7 <= h < 12:
        return "manana"
    if 12 <= h < 18:
        return "tarde"
    if 18 <= h <= 21:
        return "cierre"
    return "fuera_horario"


def agregar_hora_categoria(registros: list[dict], logger: logging.Logger) -> list[dict]:
    for r in registros:
        r["hora_categoria"] = _hora_categoria(r.get("hora", ""))
    logger.info("[TRANSFORM] hora_categoria agregada a %d registros", len(registros))
    return registros


def agregar_metricas_deteccion(registros: list[dict], logger: logging.Logger) -> list[dict]:
    for r in registros:
        dets = r.get("detecciones") or []
        confianzas = [d["confianza"] for d in dets if isinstance(d.get("confianza"), (int, float))]
        r["confianza_media"] = round(sum(confianzas) / len(confianzas), 4) if confianzas else 0.0
        conteo = r.get("conteo") or {}
        r["tiene_vehiculos"] = conteo.get("total", 0) > 0
        suma = sum(conteo.get(t, 0) for t in ("auto", "moto", "bus", "camion"))
        r["conteo_consistente"] = (suma == conteo.get("total", -1))
    logger.info("[TRANSFORM] Métricas agregadas a %d registros", len(registros))
    return registros


# =============================================================================
# Validación
# =============================================================================

def validar_tipos_y_estructura(registros: list[dict], logger: logging.Logger) -> list[dict]:
    """Verifica formatos de fecha, hora y que bytes_archivo > 0."""
    warnings: list[dict] = []
    for r in registros:
        clave = r.get("s3_key", "?")
        if not RE_FECHA.fullmatch(r.get("fecha", "")):
            warnings.append({"s3_key": clave, "tipo": "formato_fecha_invalido",
                             "detalle": f"fecha='{r.get('fecha')}'"})
        if not RE_HORA.fullmatch(r.get("hora", "")):
            warnings.append({"s3_key": clave, "tipo": "formato_hora_invalido",
                             "detalle": f"hora='{r.get('hora')}'"})
        if r.get("bytes_archivo", 1) <= 0:
            warnings.append({"s3_key": clave, "tipo": "archivo_vacio",
                             "detalle": f"bytes_archivo={r.get('bytes_archivo')}"})
    if warnings:
        logger.warning("[VALIDACION] %d problemas de estructura detectados", len(warnings))
    return warnings


def validar_semantica(registros: list[dict], logger: logging.Logger) -> list[dict]:
    """Detecta fechas futuras, confianzas fuera de rango, tipos inválidos y conteos inconsistentes."""
    hoy = date.today().isoformat()
    warnings: list[dict] = []
    for r in registros:
        clave = r.get("s3_key", "?")

        fecha = r.get("fecha", "")
        if fecha and fecha > hoy:
            warnings.append({"s3_key": clave, "tipo": "fecha_futura",
                             "detalle": f"fecha={fecha} > hoy={hoy}"})

        if r.get("ancho_px", -1) == 0 and r.get("alto_px", -1) == 0:
            warnings.append({"s3_key": clave, "tipo": "dimensiones_cero",
                             "detalle": "imagen no leíble"})

        if not r.get("conteo_consistente", True):
            warnings.append({"s3_key": clave, "tipo": "conteo_inconsistente",
                             "detalle": str(r.get("conteo"))})

        for i, det in enumerate(r.get("detecciones") or []):
            conf = det.get("confianza")
            if conf is not None and not (0.0 <= conf <= 1.0):
                warnings.append({"s3_key": clave, "tipo": "confianza_invalida",
                                 "detalle": f"confianza={conf} en deteccion {i}"})
            tipo = det.get("tipo")
            if tipo not in TIPOS_VEHICULO:
                warnings.append({"s3_key": clave, "tipo": "tipo_vehiculo_invalido",
                                 "detalle": f"tipo='{tipo}' en deteccion {i}"})

    if warnings:
        logger.warning("[VALIDACION] %d warnings semánticos detectados", len(warnings))
    return warnings


def validar_integridad_referencial(registros: list[dict], logger: logging.Logger) -> list[dict]:
    """Verifica que planta y planta_codigo correspondan a un par conocido en DENOMINADORES."""
    errores: list[dict] = []
    for r in registros:
        clave  = r.get("s3_key", "?")
        planta = r.get("planta", "")
        codigo = r.get("planta_codigo", "")
        if planta not in DENOMINADORES:
            errores.append({"s3_key": clave, "tipo": "planta_desconocida",
                           "detalle": f"planta='{planta}' no está en DENOMINADORES"})
        elif codigo and codigo != DENOMINADORES[planta]:
            errores.append({"s3_key": clave, "tipo": "codigo_incorrecto",
                           "detalle": (f"planta='{planta}': esperaba '{DENOMINADORES[planta]}'"
                                       f", obtuvo '{codigo}'")})
    if errores:
        logger.warning("[INTEGRIDAD] %d errores de integridad referencial", len(errores))
    return errores


# =============================================================================
# Guardado
# =============================================================================

def guardar_procesados(
    validos: list[dict],
    rechazados: list[dict],
    dir_processed: Path,
    fecha_proceso: str,
    logger: logging.Logger,
) -> tuple[Path, Path | None]:
    """Guarda válidos y rechazados como JSONL separados."""
    dir_validos = dir_processed / "validos"
    dir_validos.mkdir(parents=True, exist_ok=True)

    ruta_validos = dir_validos / f"registros_{fecha_proceso}.jsonl"
    contenido = "\n".join(json.dumps(r, ensure_ascii=False) for r in validos) + "\n"
    ruta_validos.write_text(contenido, encoding="utf-8")
    logger.info("[GUARDADO] Válidos: %s (%d registros)", ruta_validos, len(validos))

    ruta_rechazados: Path | None = None
    if rechazados:
        dir_rechazados = dir_processed / "rechazados"
        dir_rechazados.mkdir(parents=True, exist_ok=True)
        ruta_rechazados = dir_rechazados / f"rechazados_{fecha_proceso}.jsonl"
        try:
            contenido_r = "\n".join(json.dumps(r, ensure_ascii=False) for r in rechazados) + "\n"
            ruta_rechazados.write_text(contenido_r, encoding="utf-8")
            logger.info("[GUARDADO] Rechazados: %s (%d registros)", ruta_rechazados, len(rechazados))
        except OSError as exc:
            logger.warning("[GUARDADO] No se pudo guardar rechazados: %s", exc)
            ruta_rechazados = None

    return ruta_validos, ruta_rechazados


# =============================================================================
# Reporte
# =============================================================================

def generar_reporte(
    stats: dict,
    warnings_estructura: list[dict],
    warnings_semantica: list[dict],
    errores_integridad: list[dict],
    rutas: dict,
    prefijo: str,
) -> dict:
    return {
        "version": "1",
        "timestamp_ejecucion": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "prefijo_s3_procesado": prefijo,
        "resumen": {k: v for k, v in stats.items() if k != "por_planta"},
        "distribucion_por_planta": stats.get("por_planta", {}),
        "validacion_estructura": {
            "warnings_totales": len(warnings_estructura),
            "warnings": warnings_estructura,
        },
        "validacion_semantica": {
            "warnings_totales": len(warnings_semantica),
            "warnings": warnings_semantica,
        },
        "integridad_referencial": {
            "errores_totales": len(errores_integridad),
            "errores": errores_integridad,
        },
        "rutas_salida": rutas,
    }


def escribir_reporte(reporte: dict, dir_reports: Path, logger: logging.Logger) -> Path:
    sello = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta = dir_reports / f"reporte_{sello}.json"
    ruta.write_text(json.dumps(reporte, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[REPORTE] Escrito: %s", ruta)
    return ruta


# =============================================================================
# Orquestador
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Procesamiento y validacion de registros vehiculares JSONL"
    )
    parser.add_argument("--bucket", default=S3_BUCKET,
                        help="Bucket S3 (default: flujo-prt-imagenes)")
    parser.add_argument("--prefijo", default=f"capturas/{date.today().strftime('%Y/%m/%d')}/",
                        help="Prefijo de capturas a procesar (default: hoy)")
    parser.add_argument("--solo-local", action="store_true",
                        help="Procesar solo archivos ya en data/raw/ sin descargar de S3")
    args = parser.parse_args()

    for d in (DIR_RAW, DIR_PROCESSED / "validos", DIR_PROCESSED / "rechazados", DIR_REPORTS):
        d.mkdir(parents=True, exist_ok=True)

    fecha_proceso = date.today().strftime("%Y%m%d")
    ruta_log = DIR_REPORTS / f"proceso_{fecha_proceso}.log"
    logger = configurar_logger(ruta_log)
    logger.info("=== Inicio procesamiento | prefijo=%s | solo_local=%s ===",
                args.prefijo, args.solo_local)

    # Carga
    if args.solo_local:
        rutas = sorted(DIR_RAW.rglob("*.jsonl"))
        logger.info("[CARGA] Modo local: %d archivos en %s", len(rutas), DIR_RAW)
    else:
        s3 = boto3.client("s3")
        rutas = descargar_jsonl_desde_s3(s3, args.bucket, args.prefijo, DIR_RAW, logger)

    if not rutas:
        logger.warning("No se encontraron archivos JSONL para procesar. Saliendo.")
        return 0

    registros = cargar_registros_jsonl(rutas, logger)
    total_cargados = len(registros)

    # Limpieza
    registros, rechazados_err   = limpiar_errores(registros, logger)
    registros, rechazados_nulos = limpiar_campos_nulos(registros, logger)
    registros, rechazados_dupes = limpiar_duplicados(registros, logger)
    rechazados = rechazados_err + rechazados_nulos + rechazados_dupes

    # Transformaciones
    registros = agregar_hora_categoria(registros, logger)
    registros = agregar_metricas_deteccion(registros, logger)

    # Validaciones
    warns_estructura = validar_tipos_y_estructura(registros, logger)
    warns_semantica  = validar_semantica(registros, logger)
    errores_integrid = validar_integridad_referencial(registros, logger)

    # Distribución por planta
    por_planta: dict[str, dict] = {}
    for r in registros:
        p = r.get("planta", "desconocida")
        if p not in por_planta:
            por_planta[p] = {"total": 0, "con_vehiculos": 0, "sin_vehiculos": 0}
        por_planta[p]["total"] += 1
        if r.get("tiene_vehiculos"):
            por_planta[p]["con_vehiculos"] += 1
        else:
            por_planta[p]["sin_vehiculos"] += 1

    stats = {
        "registros_cargados":             total_cargados,
        "registros_validos":              len(registros),
        "registros_rechazados":           len(rechazados),
        "rechazados_por_error_deteccion": len(rechazados_err),
        "rechazados_por_campo_nulo":      len(rechazados_nulos),
        "rechazados_por_duplicado":       len(rechazados_dupes),
        "por_planta":                     por_planta,
    }

    # Guardado y reporte
    ruta_validos, ruta_rechazados = guardar_procesados(
        registros, rechazados, DIR_PROCESSED, fecha_proceso, logger
    )
    rutas_salida = {
        "jsonl_validos":    str(ruta_validos),
        "jsonl_rechazados": str(ruta_rechazados) if ruta_rechazados else None,
        "log":              str(ruta_log),
    }
    reporte = generar_reporte(
        stats, warns_estructura, warns_semantica, errores_integrid, rutas_salida, args.prefijo
    )
    escribir_reporte(reporte, DIR_REPORTS, logger)

    hay_problemas = bool(warns_estructura or warns_semantica or errores_integrid)
    logger.info(
        "=== Completado | válidos=%d | rechazados=%d | warnings=%d | errores_integridad=%d ===",
        len(registros), len(rechazados),
        len(warns_estructura) + len(warns_semantica), len(errores_integrid),
    )
    return 2 if hay_problemas else 0


if __name__ == "__main__":
    sys.exit(main())
