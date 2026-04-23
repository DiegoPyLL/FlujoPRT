"""
Dashboard de monitoreo en tiempo real para FlujoPRT.

Lee los JSONs de metadata del día actual desde S3 y los presenta como un
dashboard Streamlit con auto-refresh cada DASHBOARD_REFRESH segundos.

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
GAP_THRESHOLD = int(os.getenv("GAP_THRESHOLD", "180"))
DOWN_THRESHOLD = int(os.getenv("DOWN_THRESHOLD", "900"))
VENTANA_RECIENTE_MIN = int(os.getenv("VENTANA_RECIENTE_MIN", "5"))

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
# S3 helpers (patrón tomado de scripts/validate_metadata.py)
# ---------------------------------------------------------------------------


@st.cache_resource
def s3_client():
    cfg = Config(retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client("s3", config=cfg)


def listar_keys_hoy(s3, bucket: str, prefix: str) -> list[str]:
    hoy = _ahora_chile().strftime("%Y/%m/%d")
    full_prefix = f"{prefix}/{hoy}/"
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json"):
                keys.append(k)
    return keys


def descargar_json(s3, bucket: str, key: str) -> dict | None:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except (BotoCoreError, ClientError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Carga de dataset
# ---------------------------------------------------------------------------


@st.cache_data(ttl=DASHBOARD_REFRESH, show_spinner=False)
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


@st.cache_data(ttl=DASHBOARD_REFRESH, show_spinner=False)
def cargar_dataset_hoy() -> pd.DataFrame:
    s3 = s3_client()
    keys = listar_keys_hoy(s3, S3_BUCKET, METADATA_PREFIX)
    if not keys:
        return pd.DataFrame()

    registros: list[dict] = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        futuros = {pool.submit(descargar_json, s3, S3_BUCKET, k): k for k in keys}
        for fut in as_completed(futuros):
            data = fut.result()
            if data is not None:
                registros.append(data)

    if not registros:
        return pd.DataFrame()

    df = pd.DataFrame(registros)

    df["timestamp_captura"] = pd.to_datetime(df["timestamp_captura"], errors="coerce")
    df["generado_en"] = pd.to_datetime(df["generado_en"], errors="coerce")
    df["latencia_s"] = (df["generado_en"] - df["timestamp_captura"]).dt.total_seconds()
    df["hora"] = df["timestamp_captura"].dt.hour
    df["mb_originales"] = df["bytes_originales"] / 1024 / 1024
    df["mb_comprimidos"] = df["bytes_comprimidos"] / 1024 / 1024

    if "instancia_ec2" in df.columns:
        df["instance_id"] = df["instancia_ec2"].apply(
            lambda v: v.get("instance_id") if isinstance(v, dict) else None
        )
    else:
        df["instance_id"] = None

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


def tabla_estado_plantas(df: pd.DataFrame) -> pd.DataFrame:
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
                f"Últ. {VENTANA_RECIENTE_MIN} min": 0,
                "Estado": _status_planta(None, en_horario),
            })
            continue

        ultima = sub["timestamp_captura"].max()
        delta = (ahora - ultima.to_pydatetime()).total_seconds()
        recientes = int((sub["timestamp_captura"] >= corte).sum())
        filas.append({
            "Planta": nombre,
            "ID": pid,
            "Última captura": ultima.strftime("%H:%M:%S"),
            "Hace (min)": round(delta / 60, 1),
            "Capturas hoy": int(len(sub)),
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


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="FlujoPRT · Live", layout="wide", page_icon="📸")

# -- Sidebar -----------------------------------------------------------------

with st.sidebar:
    st.header("Controles")

    if st.button("Forzar recarga", use_container_width=True):
        cargar_dataset_hoy.clear()
        st.rerun()

    st.divider()
    st.caption(f"Hora Santiago: **{_ahora_chile().strftime('%H:%M:%S')}**")
    st.caption(f"Próximo auto-refresh: {DASHBOARD_REFRESH}s")
    st.caption(f"Umbral GAP: {GAP_THRESHOLD}s · DOWN: {DOWN_THRESHOLD}s")

# -- Título ------------------------------------------------------------------

st.title("FlujoPRT — Monitoreo en tiempo real")
st.caption(
    f"Bucket: `{S3_BUCKET}` · Prefix: `{METADATA_PREFIX}` · "
    f"Refresh: {DASHBOARD_REFRESH}s · Santiago: {_ahora_chile().strftime('%H:%M:%S')}"
)

with st.spinner("Cargando metadata del día desde S3..."):
    df = cargar_dataset_hoy()
    stats_pipeline = cargar_stats_hoy()

if df.empty:
    st.warning(
        "No se encontraron JSONs de metadata para hoy. "
        "Verifica que el capturador esté corriendo y que hay plantas dentro de horario."
    )
    st.stop()

# Mostrar instancia EC2 en sidebar si está disponible
inst_ids = df["instance_id"].dropna().unique()
if len(inst_ids):
    with st.sidebar:
        st.divider()
        st.caption(f"EC2: `{inst_ids[0]}`")

# -- Header con KPIs globales ------------------------------------------------

tabla = tabla_estado_plantas(df)
activas = int((tabla["Estado"] == "OK").sum())
total_plantas = len(DENOMINADORES)
mb_orig_total = df["mb_originales"].sum()
mb_comp_total = df["mb_comprimidos"].sum()
ahorro_pct = (
    (mb_orig_total - mb_comp_total) / mb_orig_total * 100 if mb_orig_total > 0 else 0
)
gaps = gaps_por_planta(df)
col_recientes = f"Últ. {VENTANA_RECIENTE_MIN} min"
capturas_recientes = int(tabla[col_recientes].sum())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Plantas OK", f"{activas} / {total_plantas}")
c2.metric("Capturas hoy", f"{len(df):,}")
c3.metric(
    "Volumen subido",
    f"{mb_comp_total:.1f} MB",
    delta=f"−{ahorro_pct:.1f}% vs original",
    delta_color="inverse",
)
c4.metric("Gaps detectados", len(gaps))
c5.metric(f"Capturas últ. {VENTANA_RECIENTE_MIN} min", capturas_recientes)

st.divider()

# -- Estadísticas acumuladas del pipeline ------------------------------------

st.subheader("Estadísticas del pipeline (acumuladas desde inicio del proceso)")

if stats_pipeline:
    periodo_inicio = stats_pipeline.get("periodo_inicio", "—")
    periodo_fin = stats_pipeline.get("periodo_fin", "—")
    uptime_pct = stats_pipeline.get("uptime_pct")
    err_desc = stats_pipeline.get("errores_descarga", 0)
    err_s3 = stats_pipeline.get("errores_s3", 0)
    duplicadas = stats_pipeline.get("duplicadas_descartadas", 0)

    st.caption(f"Período medido: **{periodo_inicio}** — **{periodo_fin}**")

    sp1, sp2, sp3, sp4 = st.columns(4)
    sp1.metric(
        "Uptime del pipeline",
        f"{uptime_pct:.1f} %" if uptime_pct is not None else "—",
    )
    sp2.metric("Errores de descarga", err_desc, delta=None)
    sp3.metric("Errores S3", err_s3, delta=None)
    sp4.metric("Capturas duplicadas descartadas", duplicadas)
else:
    st.info(
        "Aún no hay archivo de stats para hoy. "
        "Se genera automáticamente cada vez que el pipeline imprime métricas "
        f"(cada {DASHBOARD_REFRESH // 60} min aprox.)."
    )

st.divider()

# -- Sección 1: Estado operacional ------------------------------------------

st.subheader("1 · Estado operacional por planta")


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


st.dataframe(
    tabla.style
        .map(_color_estado, subset=["Estado"])
        .map(_color_recientes, subset=[col_recientes]),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# -- Sección 2: Volumen en el tiempo ----------------------------------------

st.subheader("2 · Volumen subido a S3 por hora")

df_vol = (
    df.assign(hora_bin=df["timestamp_captura"].dt.floor("h"))
      .groupby(["hora_bin", "planta_nombre"], as_index=False)["mb_comprimidos"]
      .sum()
)

fig_vol = px.bar(
    df_vol,
    x="hora_bin",
    y="mb_comprimidos",
    color="planta_nombre",
    barmode="stack",
    labels={"hora_bin": "Hora", "mb_comprimidos": "MB subidos", "planta_nombre": "Planta"},
)
fig_vol.update_xaxes(tickformat="%H:%M")
fig_vol.update_layout(height=380, legend_title=None)
st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# -- Sección 3: Heatmap planta x hora ---------------------------------------

st.subheader("3 · Capturas por planta × hora")

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

# -- Sección 4: Eficiencia de compresión ------------------------------------

st.subheader("4 · Ratio de compresión por planta")
st.caption("Valores más bajos = más ahorro. Cámaras con ratio cercano a 1 ya entregan JPEGs muy comprimidos.")

fig_box = px.box(
    df.dropna(subset=["ratio_compresion"]),
    x="planta_id",
    y="ratio_compresion",
    points="outliers",
    labels={"planta_id": "Planta", "ratio_compresion": "bytes_comp / bytes_orig"},
)
fig_box.update_layout(height=380)
st.plotly_chart(fig_box, use_container_width=True)

st.divider()

# -- Sección 5: Latencia del pipeline ---------------------------------------

st.subheader("5 · Latencia del pipeline (generado − captura)")

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

# -- Gaps recientes (si hay) ------------------------------------------------

if not gaps.empty:
    st.divider()
    st.subheader("Gaps recientes")
    gaps_view = gaps.head(30).copy()
    gaps_view["duracion_min"] = (gaps_view["duracion_s"] / 60).round(1)
    gaps_view["planta"] = gaps_view["planta_id"].map(NOMBRE_POR_ID).fillna(gaps_view["planta_id"])
    st.dataframe(
        gaps_view[["planta", "gap_inicio", "gap_fin", "duracion_min"]],
        use_container_width=True,
        hide_index=True,
    )

# ---------------------------------------------------------------------------
# Auto-refresh: al terminar de renderizar, dormir y re-ejecutar el script.
# ---------------------------------------------------------------------------

import time  # noqa: E402 — import tardío intencional, solo para el sleep final
time.sleep(DASHBOARD_REFRESH)
st.rerun()
