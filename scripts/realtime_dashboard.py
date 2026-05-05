"""
Dashboard de monitoreo en tiempo real para FlujoPRT.

Lee los JSONs de metadata del día actual desde S3 y los presenta en tres
secciones independientes con @st.fragment. Cada sección se actualiza a su
propia frecuencia sin recargar la página completa:
  - Estado/KPIs/tabla operacional: FAST_REFRESH  (default 60 s)
  - Estadísticas del pipeline:     STATS_REFRESH (default 120 s)
  - Gráficos de análisis:          DASHBOARD_REFRESH (default 300 s)

Ejecución:
    streamlit run scripts/realtime_dashboard.py \
        --server.port 8501 --server.address 127.0.0.1

Acceso desde la máquina local (cuando el dashboard corre en EC2):
    ssh -L 8501:localhost:8501 ec2-user@<ip-ec2>
    open http://localhost:8501
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")
METADATA_PREFIX = os.getenv("METADATA_PREFIX", "metadata/capturas")
STATS_PREFIX = os.getenv("STATS_PREFIX", "metadata/stats")
DASHBOARD_REFRESH = int(os.getenv("DASHBOARD_REFRESH", "300"))
FAST_REFRESH      = int(os.getenv("FAST_REFRESH", "60"))
STATS_REFRESH     = int(os.getenv("STATS_REFRESH", "120"))
GAP_THRESHOLD = int(os.getenv("GAP_THRESHOLD", "180"))
DOWN_THRESHOLD = int(os.getenv("DOWN_THRESHOLD", "900"))
VENTANA_RECIENTE_MIN = int(os.getenv("VENTANA_RECIENTE_MIN", "5"))
DIAS_FALLBACK = int(os.getenv("DIAS_FALLBACK", "7"))
TASA_BIN_MIN = int(os.getenv("TASA_BIN_MIN", "15"))

VERSION = "1.3.0"

CHILE_TZ = ZoneInfo("America/Santiago")


def _ahora_chile() -> datetime:
    """Hora actual en America/Santiago como datetime naive (mismo formato que los JSONs de S3)."""
    return datetime.now(CHILE_TZ).replace(tzinfo=None)


# Duplicado desde ImageRecompilerCloud.py: importar ese módulo arranca
# aioboto3, signal handlers y un ThreadPoolExecutor al top-level.
HORARIOS = {
    "Huechuraba": {"semana": ("07:10", "16:50"), "sabado": ("07:10", "16:50")},
    "La Florida": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "La Pintana": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Pudahuel": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Quilicura": {"semana": ("07:10", "16:50"), "sabado": ("07:10", "16:50")},
    "Recoleta": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "San Joaquin": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Temuco": {"semana": ("08:10", "18:20"), "sabado": ("08:10", "13:50")},
    "Villarica": {"semana": ("07:10", "17:50"), "sabado": ("07:40", "13:50")},
    "Chillan": {"semana": ("06:40", "17:20"), "sabado": ("07:10", "13:50")},
    "Yungay": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Concepcion": {"semana": ("07:40", "20:20"), "sabado": ("08:10", "16:50")},
    "San Pedro de la Paz": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Yumbel": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
}

DENOMINADORES = {
    "Huechuraba": "HCH", "La Florida": "LFL", "La Pintana": "LPT",
    "Pudahuel": "PUD", "Quilicura": "QLC", "Recoleta": "RCL",
    "San Joaquin": "SJQ", "Temuco": "TMU", "Villarica": "VLL",
    "Chillan": "CHL", "Yungay": "YGY", "Concepcion": "CCP",
    "San Pedro de la Paz": "SPP", "Yumbel": "YMB",
}

NOMBRE_POR_ID = {v: k for k, v in DENOMINADORES.items()}

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


@st.cache_resource
def s3_client():
    cfg = Config(retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client("s3", config=cfg)


def listar_keys_fecha(s3, bucket: str, prefix: str, fecha_str: str) -> list[dict]:
    """Retorna lista de dicts con 'key' y 'last_modified' para cada JSONL de la fecha dada."""
    full_prefix = f"{prefix}/{fecha_str}/"
    objetos: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".jsonl"):
                objetos.append({"key": k, "last_modified": obj["LastModified"]})
    return objetos


def descargar_jsonl(s3, bucket: str, key: str) -> list[dict]:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        lineas = resp["Body"].read().decode("utf-8").splitlines()
        return [json.loads(ln) for ln in lineas if ln.strip()]
    except (BotoCoreError, ClientError, json.JSONDecodeError):
        return []


# ---------------------------------------------------------------------------
# Carga de dataset
# ---------------------------------------------------------------------------


@st.cache_data(ttl=DASHBOARD_REFRESH, show_spinner=False)
def encontrar_fecha_con_datos() -> str:
    """Retorna la fecha más reciente (hasta DIAS_FALLBACK días atrás) con JSONL en S3.
    Si ninguna fecha tiene datos, retorna hoy (el dashboard mostrará vacío)."""
    s3 = s3_client()
    for dias in range(DIAS_FALLBACK):
        fecha_str = (_ahora_chile() - timedelta(days=dias)).strftime("%Y/%m/%d")
        full_prefix = f"{METADATA_PREFIX}/{fecha_str}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=full_prefix, MaxKeys=1):
            if page.get("Contents"):
                return fecha_str
    return _ahora_chile().strftime("%Y/%m/%d")


@st.cache_data(ttl=STATS_REFRESH, show_spinner=False)
def cargar_stats_hoy() -> dict | None:
    """Lee el JSON de stats acumuladas que el pipeline escribe periódicamente a S3."""
    s3 = s3_client()
    hoy = _ahora_chile().strftime("%Y/%m/%d")
    key = f"{STATS_PREFIX}/{hoy}/resumen.json"
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except (BotoCoreError, ClientError):
        return None


@st.cache_data(ttl=FAST_REFRESH, show_spinner=False)
def estado_validacion_fecha(fecha_str: str) -> dict:
    """Infiere el estado de validate_vehiculos desde los JSONL disponibles en S3 para la fecha dada."""
    s3 = s3_client()
    objetos = listar_keys_fecha(s3, S3_BUCKET, METADATA_PREFIX, fecha_str)
    if not objetos:
        return {"estado": "sin_datos", "plantas_con_jsonl": [], "ultima_modificacion": None, "n_jsonl": 0}

    ahora_utc = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)
    ultima = max(
        (o["last_modified"].replace(tzinfo=None) if o["last_modified"].tzinfo else o["last_modified"])
        for o in objetos
    )
    minutos_desde_actualizacion = (ahora_utc - ultima).total_seconds() / 60

    # Ruta: metadata/capturas/YYYY/MM/DD/Planta/XXX.jsonl
    #        [0]     [1]      [2]  [3] [4] [5]   [6]
    plantas = sorted({o["key"].split("/")[5] for o in objetos if len(o["key"].split("/")) > 5})

    estado = "en_progreso" if minutos_desde_actualizacion < 10 else "completado"
    return {
        "estado": estado,
        "plantas_con_jsonl": plantas,
        "ultima_modificacion": ultima,
        "n_jsonl": len(objetos),
    }


@st.cache_data(ttl=FAST_REFRESH, show_spinner=False)
def cargar_dataset_fecha(fecha_str: str) -> pd.DataFrame:
    s3 = s3_client()
    objetos = listar_keys_fecha(s3, S3_BUCKET, METADATA_PREFIX, fecha_str)
    if not objetos:
        return pd.DataFrame()

    registros: list[dict] = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        futuros = {pool.submit(descargar_jsonl, s3, S3_BUCKET, o["key"]): o["key"] for o in objetos}
        for fut in as_completed(futuros):
            registros.extend(fut.result())

    if not registros:
        return pd.DataFrame()

    df = pd.DataFrame(registros)

    # Mapeo de campos validate_vehiculos → dashboard
    df["timestamp_captura"] = pd.to_datetime(df["timestamp_imagen"], errors="coerce")
    df["generado_en"] = pd.to_datetime(df["procesado_en"], errors="coerce")
    df["planta_id"] = df["planta_codigo"].str.upper()
    df["planta_nombre"] = df["planta"]
    df["mb_archivo"] = df["bytes_archivo"] / 1024 / 1024
    df["latencia_s"] = (df["generado_en"] - df["timestamp_captura"]).dt.total_seconds()
    df["hora"] = df["timestamp_captura"].dt.hour

    df["vehiculos_total"] = df["conteo"].apply(
        lambda c: c.get("total", 0) if isinstance(c, dict) else 0
    )
    df["bboxes_total"] = df["detecciones"].apply(
        lambda d: len(d) if isinstance(d, list) else 0
    )
    for tipo in ("auto", "moto", "bus", "camion"):
        df[f"v_{tipo}"] = df["conteo"].apply(
            lambda c, t=tipo: c.get(t, 0) if isinstance(c, dict) else 0
        )

    return df.sort_values("timestamp_captura").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Derivaciones
# ---------------------------------------------------------------------------


def _status_planta(delta_seg: float | None, dentro_horario_flag: bool) -> str:
    if delta_seg is None:
        return "SIN DATOS"
    if not dentro_horario_flag:
        return "FUERA DE HORARIO"
    if delta_seg > DOWN_THRESHOLD:
        return "DOWN"
    if delta_seg > GAP_THRESHOLD:
        return "GAP"
    return "OK"


def _dentro_horario(planta: str, ahora: datetime) -> bool:
    dia = ahora.weekday()
    if dia == 6:
        return False
    if planta not in HORARIOS:
        return False
    tipo = "sabado" if dia == 5 else "semana"
    inicio, fin = HORARIOS[planta][tipo]
    h_ini = datetime.strptime(inicio, "%H:%M").time()
    h_fin = datetime.strptime(fin, "%H:%M").time()
    return h_ini <= ahora.time() <= h_fin


def tabla_estado_plantas(df: pd.DataFrame, fecha_str: str | None = None) -> pd.DataFrame:
    hoy_str = _ahora_chile().strftime("%Y/%m/%d")
    es_historico = fecha_str is not None and fecha_str != hoy_str
    if es_historico and not df.empty:
        ahora = df["timestamp_captura"].max().to_pydatetime()
    else:
        ahora = _ahora_chile()
    corte = ahora - timedelta(minutes=VENTANA_RECIENTE_MIN)
    filas: list[dict] = []

    for nombre, pid in DENOMINADORES.items():
        sub = df[df["planta_id"] == pid] if not df.empty else df
        en_horario = _dentro_horario(nombre, ahora)

        if sub.empty:
            filas.append({
                "Planta": nombre,
                "ID": pid,
                "Última captura": "—",
                "Hace (min)": None,
                "Capturas hoy": 0,
                "Vehículos (bbox)": 0,
                f"Últ. {VENTANA_RECIENTE_MIN} min": 0,
                "Estado": _status_planta(None, en_horario),
            })
            continue

        ultima = sub["timestamp_captura"].max()
        delta = (ahora - ultima.to_pydatetime()).total_seconds()
        recientes = int((sub["timestamp_captura"] >= corte).sum())
        bboxes = int(sub["bboxes_total"].sum())
        filas.append({
            "Planta": nombre,
            "ID": pid,
            "Última captura": ultima.strftime("%H:%M:%S"),
            "Hace (min)": round(delta / 60, 1),
            "Capturas hoy": int(len(sub)),
            "Vehículos (bbox)": bboxes,
            f"Últ. {VENTANA_RECIENTE_MIN} min": recientes,
            "Estado": _status_planta(delta, en_horario),
        })

    return pd.DataFrame(filas)


def gaps_por_planta(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["planta_id", "gap_inicio", "gap_fin", "duracion_s"])

    partes: list[pd.DataFrame] = []
    for pid, sub in df.groupby("planta_id"):
        sub = sub.sort_values("timestamp_captura").copy()
        sub["delta_prev_s"] = sub["timestamp_captura"].diff().dt.total_seconds()
        incidencias = sub[sub["delta_prev_s"] > GAP_THRESHOLD].copy()
        if incidencias.empty:
            continue
        incidencias["gap_fin"] = incidencias["timestamp_captura"]
        incidencias["gap_inicio"] = incidencias["timestamp_captura"] - pd.to_timedelta(
            incidencias["delta_prev_s"], unit="s"
        )
        incidencias["duracion_s"] = incidencias["delta_prev_s"]
        partes.append(incidencias[["planta_id", "gap_inicio", "gap_fin", "duracion_s"]])

    if not partes:
        return pd.DataFrame(columns=["planta_id", "gap_inicio", "gap_fin", "duracion_s"])
    return pd.concat(partes, ignore_index=True).sort_values("gap_inicio", ascending=False)


def _intervalo_captura(stats: dict | None) -> int:
    """Retorna el intervalo de captura en segundos, priorizando stats de S3."""
    if stats and "intervalo_s" in stats:
        return int(stats["intervalo_s"])
    return int(os.getenv("INTERVALO", "60"))


def tasa_exito_capturas(df: pd.DataFrame, intervalo_s: int, bin_min: int = 15) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    capturas_esperadas = (bin_min * 60) / intervalo_s
    return (
        df.assign(bin=df["timestamp_captura"].dt.floor(f"{bin_min}min"))
          .groupby(["bin", "planta_nombre"])
          .size()
          .reset_index(name="capturas_reales")
          .assign(
              capturas_esperadas=capturas_esperadas,
              tasa_exito=lambda d: (d["capturas_reales"] / capturas_esperadas * 100).clip(upper=100),
          )
    )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="FlujoPRT · Live", layout="wide", page_icon="📸")

# -- Sidebar -----------------------------------------------------------------

hoy_str = _ahora_chile().strftime("%Y/%m/%d")

with st.sidebar:
    st.header("Controles")

    # Buscar la fecha más reciente con datos (puede ser hoy u otro día anterior)
    fecha_disponible_str = encontrar_fecha_con_datos()
    fecha_disponible_dt = datetime.strptime(fecha_disponible_str, "%Y/%m/%d")

    fecha_sel = st.date_input(
        "Fecha a visualizar",
        value=fecha_disponible_dt,
        max_value=datetime.strptime(hoy_str, "%Y/%m/%d"),
        format="YYYY/MM/DD",
    )
    fecha_str = fecha_sel.strftime("%Y/%m/%d")

    if st.button("Forzar recarga", use_container_width=True):
        cargar_dataset_fecha.clear()
        estado_validacion_fecha.clear()
        encontrar_fecha_con_datos.clear()
        st.rerun()

    st.divider()
    st.caption(f"Hora Santiago: **{_ahora_chile().strftime('%H:%M:%S')}**")
    st.caption(f"Estado/KPIs: {FAST_REFRESH}s · Stats: {STATS_REFRESH}s · Gráficos: {DASHBOARD_REFRESH}s")
    st.caption(f"Umbral GAP: {GAP_THRESHOLD}s · DOWN: {DOWN_THRESHOLD}s · Bin tasa: {TASA_BIN_MIN} min")

# -- Título ------------------------------------------------------------------

st.title("FlujoPRT — Monitoreo en tiempo real")
st.caption(
    f"Bucket: `{S3_BUCKET}` · Prefix: `{METADATA_PREFIX}` · "
    f"KPIs: {FAST_REFRESH}s · Stats: {STATS_REFRESH}s · Gráficos: {DASHBOARD_REFRESH}s · "
    f"Santiago: {_ahora_chile().strftime('%H:%M:%S')}"
)
st.caption(f"v{VERSION}")

# Banner cuando se muestran datos históricos
if fecha_str != hoy_str:
    st.warning(
        f"Mostrando datos de **{fecha_str}** · "
        f"No hay JSONL de detección para hoy ({hoy_str}). "
        "Ejecuta `validate_vehiculos.py` para generar los datos del día."
    )

# ---------------------------------------------------------------------------
# Fragments de auto-actualización independiente
# ---------------------------------------------------------------------------


@st.fragment(run_every=FAST_REFRESH)
def seccion_estado_y_kpis() -> None:
    """Estado de detección vehicular, KPIs globales y tabla operacional."""
    fecha_str = st.session_state.get("fecha_str", _ahora_chile().strftime("%Y/%m/%d"))
    hoy_str_frag = _ahora_chile().strftime("%Y/%m/%d")

    df = cargar_dataset_fecha(fecha_str)
    info_validacion = estado_validacion_fecha(fecha_str)

    # -- Estado de detección vehicular -----------------------------------------
    st.subheader("Estado del proceso de detección vehicular (validate_vehiculos)")

    _color_estado_val = {"sin_datos": "gray", "en_progreso": "orange", "completado": "green"}
    _label_estado_val = {"sin_datos": "SIN DATOS", "en_progreso": "EN PROGRESO", "completado": "COMPLETADO"}
    _est = info_validacion["estado"]
    st.markdown(
        f"**:{_color_estado_val[_est]}[{_label_estado_val[_est]}]**  "
        f"· {info_validacion['n_jsonl']} JSONL · "
        f"{len(info_validacion['plantas_con_jsonl'])}/{len(DENOMINADORES)} plantas"
    )

    if info_validacion["plantas_con_jsonl"]:
        st.caption("Plantas con JSONL: " + ", ".join(info_validacion["plantas_con_jsonl"]))

    if info_validacion["ultima_modificacion"]:
        ult = info_validacion["ultima_modificacion"]
        # ult viene de S3 LastModified (UTC naive); convertir a Santiago antes de restar
        ult_santiago = ult.replace(tzinfo=ZoneInfo("UTC")).astimezone(CHILE_TZ).replace(tzinfo=None)
        hace_min = (_ahora_chile() - ult_santiago).total_seconds() / 60
        st.caption(f"Última actualización JSONL: {ult_santiago.strftime('%H:%M:%S')} (hace {hace_min:.0f} min)")

    if not df.empty:
        st.caption(f"Total registros cargados: {len(df):,}")
    elif fecha_str == hoy_str_frag:
        st.info(
            "validate_vehiculos.py aún no ha procesado imágenes para hoy. "
            "El script es incremental: los datos aparecen a medida que se procesa cada planta."
        )
    else:
        st.info(f"No hay registros de detección para {fecha_str}.")

    st.divider()

    # -- KPIs globales ---------------------------------------------------------
    total_plantas = len(DENOMINADORES)
    col_recientes = f"Últ. {VENTANA_RECIENTE_MIN} min"
    if not df.empty:
        tabla = tabla_estado_plantas(df, fecha_str=fecha_str)
        activas = int((tabla["Estado"] == "OK").sum())
        mb_total = df["mb_archivo"].sum()
        bboxes_hoy = int(df["bboxes_total"].sum())
        capturas_recientes = int(tabla[col_recientes].sum())
    else:
        tabla = pd.DataFrame()
        activas = 0
        mb_total = 0.0
        bboxes_hoy = 0
        capturas_recientes = 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Plantas OK", f"{activas} / {total_plantas}")
    c2.metric("Capturas procesadas", f"{len(df):,}" if not df.empty else "—")
    c3.metric("Volumen analizado", f"{mb_total:.1f} MB")
    c4.metric("Vehículos (bbox)", f"{bboxes_hoy:,}")
    c5.metric(f"Capturas últ. {VENTANA_RECIENTE_MIN} min", capturas_recientes)

    if not df.empty:
        def _color_estado(val: object) -> str:
            colores = {
                "OK": "background-color: #1f7a3a; color: white",
                "GAP": "background-color: #b8860b; color: white",
                "DOWN": "background-color: #a02020; color: white",
                "FUERA DE HORARIO": "background-color: #444; color: #ccc",
                "SIN DATOS": "background-color: #222; color: #888",
            }
            return colores.get(str(val), "")

        def _color_recientes(val: object) -> str:
            try:
                return "background-color: #1f7a3a; color: white" if int(val) > 0 else "background-color: #333; color: #888"  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return ""

        st.divider()
        st.subheader("1 · Estado operacional en tiempo real por planta")
        st.dataframe(
            tabla.style
                .map(_color_estado, subset=["Estado"])
                .map(_color_recientes, subset=[col_recientes])
                .map(_color_recientes, subset=["Vehículos (bbox)"]),
            use_container_width=True,
            hide_index=True,
        )


@st.fragment(run_every=STATS_REFRESH)
def seccion_stats_pipeline() -> None:
    """Estadísticas acumuladas del pipeline (uptime, subidas, errores)."""
    stats_pipeline = cargar_stats_hoy()

    st.subheader("Estadísticas del pipeline de captura (acumuladas desde inicio del proceso)")

    if stats_pipeline:
        periodo_inicio = stats_pipeline.get("periodo_inicio", "—")
        periodo_fin = stats_pipeline.get("periodo_fin", "—")
        ts_act = stats_pipeline.get("timestamp_actualizacion", "")
        uptime_pct = stats_pipeline.get("uptime_pct")
        err_desc = stats_pipeline.get("errores_descarga", 0)
        err_s3 = stats_pipeline.get("errores_s3", 0)
        duplicadas = stats_pipeline.get("duplicadas_descartadas", 0)
        total_subidas = stats_pipeline.get("total_subidas", 0)
        version = stats_pipeline.get("version", "")

        caption_parts = [f"Período: **{periodo_inicio}** — **{periodo_fin}**"]
        if ts_act:
            caption_parts.append(f"Actualizado: **{ts_act}**")
        if version:
            caption_parts.append(f"v{version}")
        st.caption("  ·  ".join(caption_parts))

        sp1, sp2, sp3, sp4, sp5 = st.columns(5)
        sp1.metric(
            "Uptime del pipeline",
            f"{uptime_pct:.1f} %" if uptime_pct is not None else "—",
        )
        sp2.metric("Imágenes subidas", f"{total_subidas:,}")
        sp3.metric("Errores de descarga", err_desc, delta=None)
        sp4.metric("Errores S3", err_s3, delta=None)
        sp5.metric("Duplicadas descartadas", duplicadas)
    else:
        st.info(
            "Aún no hay archivo de stats para hoy. "
            "Se genera automáticamente cada vez que el pipeline imprime métricas "
            f"(cada {DASHBOARD_REFRESH // 60} min aprox.)."
        )


@st.fragment(run_every=DASHBOARD_REFRESH)
def seccion_graficos() -> None:
    """Gráficos de análisis: volumen, heatmap, vehículos, latencia y gaps."""
    fecha_str = st.session_state.get("fecha_str", _ahora_chile().strftime("%Y/%m/%d"))

    df = cargar_dataset_fecha(fecha_str)

    if df.empty:
        st.info("Los gráficos de detección aparecerán cuando validate_vehiculos procese imágenes del día.")
        return

    # -- Sección 2: Volumen en el tiempo ---------------------------------------
    st.subheader("2 · MB de imágenes procesadas por hora y planta (aprox.)")

    df_vol = (
        df.assign(hora_bin=df["timestamp_captura"].dt.floor("h"))
          .groupby(["hora_bin", "planta_nombre"], as_index=False)["mb_archivo"]
          .sum()
    )
    fig_vol = px.bar(
        df_vol,
        x="hora_bin",
        y="mb_archivo",
        color="planta_nombre",
        barmode="stack",
        labels={"hora_bin": "Hora", "mb_archivo": "MB", "planta_nombre": "Planta"},
    )
    fig_vol.update_xaxes(tickformat="%H:%M")
    fig_vol.update_layout(height=380, legend_title=None)
    st.plotly_chart(fig_vol, use_container_width=True)

    st.divider()

    # -- Sección 3: Heatmap planta x hora --------------------------------------
    st.subheader("3 · Capturas de imagen por planta y hora del día (aprox.)")

    heatmap = (
        df.groupby(["planta_id", "hora"]).size().reset_index(name="capturas")
          .pivot(index="planta_id", columns="hora", values="capturas")
          .fillna(0)
          .reindex(index=sorted(DENOMINADORES.values()))
    )
    fig_heat = px.imshow(
        heatmap,
        aspect="auto",
        color_continuous_scale="Viridis",
        labels={"x": "Hora del día", "y": "Planta", "color": "Capturas"},
    )
    fig_heat.update_layout(height=420)
    st.plotly_chart(fig_heat, use_container_width=True)

    st.divider()

    # -- Sección 4: Vehículos detectados ---------------------------------------
    st.subheader("4 · Vehículos detectados por tipo y planta (bboxes YOLO)")

    df_veh = (
        df.groupby("planta_nombre", as_index=False)[["v_auto", "v_moto", "v_bus", "v_camion"]]
          .sum()
          .rename(columns={"v_auto": "Auto", "v_moto": "Moto", "v_bus": "Bus", "v_camion": "Camión"})
    )
    df_veh_long = df_veh.melt(id_vars="planta_nombre", var_name="Tipo", value_name="Conteo")
    fig_veh = px.bar(
        df_veh_long,
        x="planta_nombre",
        y="Conteo",
        color="Tipo",
        barmode="stack",
        labels={"planta_nombre": "Planta", "Conteo": "Vehículos"},
        color_discrete_map={"Auto": "#00c800", "Moto": "#0078ff", "Bus": "#ffc800", "Camión": "#dc2828"},
    )
    fig_veh.update_layout(height=380, legend_title=None, xaxis_tickangle=-30)
    st.plotly_chart(fig_veh, use_container_width=True)

    st.divider()

    # -- Sección 5: Latencia ---------------------------------------------------
    st.subheader("5 · Latencia YOLO por imagen: tiempo entre captura y fin de procesamiento")

    lat = df["latencia_s"].dropna()
    if not lat.empty:
        p50 = lat.quantile(0.5)
        p95 = lat.quantile(0.95)
        p99 = lat.quantile(0.99)

        l1, l2, l3 = st.columns(3)
        l1.metric("p50", f"{p50:.1f} s")
        l2.metric("p95", f"{p95:.1f} s")
        l3.metric("p99", f"{p99:.1f} s")

        fig_lat = px.histogram(
            lat[lat < lat.quantile(0.99)],
            nbins=50,
            labels={"value": "Latencia (s)"},
        )
        fig_lat.update_layout(height=320, showlegend=False)
        st.plotly_chart(fig_lat, use_container_width=True)
    else:
        st.info("No hay datos de latencia todavía.")

    # -- Sección 6: Tasa de éxito de captura -----------------------------------
    st.divider()
    st.subheader("6 · Tasa de éxito de captura por planta vs. esperada (aprox.)")

    _stats_intervalo = cargar_stats_hoy()
    _intervalo_s = _intervalo_captura(_stats_intervalo)
    df_tasa = tasa_exito_capturas(df, intervalo_s=_intervalo_s, bin_min=TASA_BIN_MIN)

    if not df_tasa.empty:
        _esp = int(TASA_BIN_MIN * 60 / _intervalo_s)
        st.caption(
            f"Intervalo captura: **{_intervalo_s}s** · "
            f"Bin: **{TASA_BIN_MIN} min** · "
            f"Esperadas/bin: **{_esp}**"
        )
        fig_tasa = px.line(
            df_tasa,
            x="bin",
            y="tasa_exito",
            color="planta_nombre",
            labels={"bin": "Hora", "tasa_exito": "Tasa de éxito (%)", "planta_nombre": "Planta"},
            range_y=[0, 105],
        )
        fig_tasa.add_hline(y=80, line_dash="dot", line_color="orange", annotation_text="80%")
        fig_tasa.update_xaxes(tickformat="%H:%M")
        fig_tasa.update_layout(height=420, legend_title=None)
        st.plotly_chart(fig_tasa, use_container_width=True)

    # -- Gaps recientes --------------------------------------------------------
    gaps = gaps_por_planta(df)
    if not gaps.empty:
        st.divider()
        st.subheader("Gaps recientes de captura por planta")
        gaps_view = gaps.head(30).copy()
        gaps_view["duracion_min"] = (gaps_view["duracion_s"] / 60).round(1)
        gaps_view["planta"] = gaps_view["planta_id"].map(NOMBRE_POR_ID).fillna(gaps_view["planta_id"])
        st.dataframe(
            gaps_view[["planta", "gap_inicio", "gap_fin", "duracion_min"]],
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------------
# Flujo principal: guardar fecha en session_state e invocar fragments
# ---------------------------------------------------------------------------

st.session_state["fecha_str"] = fecha_str

seccion_estado_y_kpis()
st.divider()
seccion_stats_pipeline()
st.divider()
seccion_graficos()
