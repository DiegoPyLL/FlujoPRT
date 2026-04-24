"""
Tests para funciones puras de scripts/realtime_dashboard.py.

El dashboard tiene código Streamlit de nivel módulo, por lo que se inyectan
mocks de 'streamlit', 'boto3' en sys.modules y se parchea time.sleep
antes de importar para evitar bloqueos de 300 s.
"""
import sys
import os
import json
import time as _time_module
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import pandas as pd

sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
scripts_dir = os.path.join(project_root, "scripts")

if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

# ---------------------------------------------------------------------------
# 1. Parchear time.sleep ANTES del import (el módulo duerme 300 s al final)
# ---------------------------------------------------------------------------
_original_sleep = _time_module.sleep
_time_module.sleep = lambda _: None


# ---------------------------------------------------------------------------
# 2. Mock de Streamlit con decoradores pass-through y semántica correcta
# ---------------------------------------------------------------------------

def _cache_data(func=None, **kwargs):
    if func is not None:
        return func
    return lambda f: f


def _cache_resource(func=None, **kwargs):
    if func is not None:
        return func
    return lambda f: f


def _columns_factory(n, **kw):
    size = n if isinstance(n, int) else len(n)
    return [MagicMock() for _ in range(size)]


mock_st = MagicMock()
mock_st.cache_data = _cache_data
mock_st.cache_resource = _cache_resource
mock_st.button.return_value = False          # evita cargar_dataset_hoy.clear()
mock_st.columns.side_effect = _columns_factory
mock_st.stop.return_value = None             # no-op (df siempre tendrá filas)

sys.modules["streamlit"] = mock_st


# ---------------------------------------------------------------------------
# 3. Mock de boto3 para evitar llamadas S3 reales
# ---------------------------------------------------------------------------

_valid_record = {
    "version": "1",
    "planta_id": "HCH",
    "planta_nombre": "Huechuraba",
    "plataforma": "TÜV Rheinland",
    "timestamp_captura": "2026-04-23T10:00:00",
    "fecha_str": "20260423_100000",
    "s3_imagen_key": "capturas/2026/04/23/Huechuraba/HCH_20260423_100000.jpg",
    "s3_bucket": "flujo-prt-imagenes",
    "bytes_originales": 100000,
    "bytes_comprimidos": 50000,
    "ratio_compresion": 0.5,
    "instancia_ec2": {"instance_id": "i-0abc", "instance_type": "t3.micro"},
    "generado_en": "2026-04-23T10:00:05",
}

mock_s3_inst = MagicMock()
_mock_paginator = MagicMock()
_mock_paginator.paginate.return_value = [
    {"Contents": [{"Key": "metadata/capturas/2026/04/23/Huechuraba/HCH_20260423_100000.json"}]}
]
mock_s3_inst.get_paginator.return_value = _mock_paginator
_mock_body = MagicMock()
_mock_body.read.return_value = json.dumps(_valid_record).encode("utf-8")
mock_s3_inst.get_object.return_value = {"Body": _mock_body}

mock_boto3 = MagicMock()
mock_boto3.client.return_value = mock_s3_inst
sys.modules["boto3"] = mock_boto3


# ---------------------------------------------------------------------------
# 4. Importar el módulo con todo mockeado
# ---------------------------------------------------------------------------

import realtime_dashboard as dash

# Restaurar time.sleep para el resto de la suite
_time_module.sleep = _original_sleep


# ---------------------------------------------------------------------------
# _ahora_chile
# ---------------------------------------------------------------------------

class TestAhoraChile:

    def test_retorna_datetime_naive(self):
        ahora = dash._ahora_chile()
        assert isinstance(ahora, datetime)
        assert ahora.tzinfo is None

    def test_retorna_hora_valida(self):
        ahora = dash._ahora_chile()
        assert 0 <= ahora.hour <= 23
        assert 0 <= ahora.minute <= 59


# ---------------------------------------------------------------------------
# _status_planta
# ---------------------------------------------------------------------------

class TestStatusPlanta:

    def test_sin_datos(self):
        assert dash._status_planta(None, True) == "SIN DATOS"

    def test_fuera_de_horario(self):
        assert dash._status_planta(10.0, False) == "FUERA DE HORARIO"

    def test_down_supera_umbral(self):
        assert dash._status_planta(float(dash.DOWN_THRESHOLD + 1), True) == "DOWN"

    def test_gap_entre_umbrales(self):
        delta = (dash.DOWN_THRESHOLD + dash.GAP_THRESHOLD) // 2
        assert dash._status_planta(float(delta), True) == "GAP"

    def test_ok_dentro_umbral(self):
        assert dash._status_planta(10.0, True) == "OK"

    def test_ok_en_limite_inferior(self):
        assert dash._status_planta(0.0, True) == "OK"

    def test_fuera_horario_prima_sobre_down(self):
        assert dash._status_planta(float(dash.DOWN_THRESHOLD + 9999), False) == "FUERA DE HORARIO"

    def test_exactamente_gap_threshold_es_ok(self):
        # La condición usa > estricto: delta == GAP_THRESHOLD → "OK"
        assert dash._status_planta(float(dash.GAP_THRESHOLD), True) == "OK"


# ---------------------------------------------------------------------------
# _dentro_horario
# ---------------------------------------------------------------------------

class TestDentroHorario:

    def test_domingo_siempre_false(self):
        domingo = datetime(2026, 4, 26, 10, 0)
        assert dash._dentro_horario("Huechuraba", domingo) is False

    def test_semana_dentro_horario(self):
        miercoles = datetime(2026, 4, 22, 10, 0)
        assert dash._dentro_horario("Huechuraba", miercoles) is True

    def test_semana_fuera_horario(self):
        miercoles = datetime(2026, 4, 22, 23, 0)
        assert dash._dentro_horario("Huechuraba", miercoles) is False

    def test_sabado_dentro_horario(self):
        sabado = datetime(2026, 4, 25, 9, 0)
        assert dash._dentro_horario("Temuco", sabado) is True

    def test_sabado_fuera_horario(self):
        sabado = datetime(2026, 4, 25, 14, 30)  # Temuco sáb cierra 13:50
        assert dash._dentro_horario("Temuco", sabado) is False

    def test_planta_desconocida_false(self):
        miercoles = datetime(2026, 4, 22, 10, 0)
        assert dash._dentro_horario("PlantaInexistente", miercoles) is False

    def test_borde_inicio_es_true(self):
        # Huechuraba semana abre 07:10
        borde = datetime(2026, 4, 22, 7, 10, 0)
        assert dash._dentro_horario("Huechuraba", borde) is True

    def test_un_minuto_antes_de_apertura_es_false(self):
        antes = datetime(2026, 4, 22, 7, 9, 0)
        assert dash._dentro_horario("Huechuraba", antes) is False


# ---------------------------------------------------------------------------
# gaps_por_planta
# ---------------------------------------------------------------------------

class TestGapsPorPlanta:

    def test_df_vacio_retorna_columnas_correctas(self):
        resultado = dash.gaps_por_planta(pd.DataFrame())
        assert resultado.empty
        assert set(resultado.columns) == {"planta_id", "gap_inicio", "gap_fin", "duracion_s"}

    def test_sin_gaps_no_detecta_nada(self):
        df = pd.DataFrame({
            "planta_id": ["HCH", "HCH", "HCH"],
            "timestamp_captura": pd.to_datetime([
                "2026-04-23T10:00:00",
                "2026-04-23T10:01:00",
                "2026-04-23T10:02:00",
            ]),
        })
        assert dash.gaps_por_planta(df).empty

    def test_detecta_gap_grande(self):
        df = pd.DataFrame({
            "planta_id": ["HCH", "HCH"],
            "timestamp_captura": pd.to_datetime([
                "2026-04-23T10:00:00",
                "2026-04-23T10:10:00",  # 600 s > GAP_THRESHOLD (180 s)
            ]),
        })
        resultado = dash.gaps_por_planta(df)
        assert len(resultado) == 1
        assert resultado.iloc[0]["duracion_s"] == pytest.approx(600.0)
        assert resultado.iloc[0]["planta_id"] == "HCH"

    def test_multiples_plantas_multiples_gaps(self):
        df = pd.DataFrame({
            "planta_id": ["HCH", "HCH", "TMU", "TMU"],
            "timestamp_captura": pd.to_datetime([
                "2026-04-23T10:00:00",
                "2026-04-23T10:10:00",
                "2026-04-23T11:00:00",
                "2026-04-23T11:20:00",
            ]),
        })
        resultado = dash.gaps_por_planta(df)
        assert len(resultado) == 2
        assert set(resultado["planta_id"].tolist()) == {"HCH", "TMU"}

    def test_gap_por_debajo_del_umbral_ignorado(self):
        df = pd.DataFrame({
            "planta_id": ["HCH", "HCH"],
            "timestamp_captura": pd.to_datetime([
                "2026-04-23T10:00:00",
                "2026-04-23T10:01:00",  # 60 s < GAP_THRESHOLD
            ]),
        })
        assert dash.gaps_por_planta(df).empty

    def test_solo_un_registro_por_planta_no_genera_gap(self):
        df = pd.DataFrame({
            "planta_id": ["HCH"],
            "timestamp_captura": pd.to_datetime(["2026-04-23T10:00:00"]),
        })
        assert dash.gaps_por_planta(df).empty
