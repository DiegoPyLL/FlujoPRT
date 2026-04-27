import sys
import os
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from imageRecopilator.Local import imageRecompilerLocal as script

_MOD = "imageRecopilator.Local.ImageRecompilerLocal"


@pytest.mark.imageRecopilator
class TestCamarasLocal:

    def test_es_domingo_true(self):
        fecha = datetime(2026, 1, 18, 12, 0, 0)  # domingo
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            assert script.es_domingo() is True

    def test_es_domingo_false(self):
        fecha = datetime(2026, 1, 19, 12, 0, 0)  # lunes
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            assert script.es_domingo() is False

    def test_dentro_horario_semana_abierto(self):
        fecha = datetime(2026, 1, 20, 10, 0, 0)  # martes 10:00
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Huechuraba") is True

    def test_dentro_horario_semana_cerrado(self):
        fecha = datetime(2026, 1, 20, 23, 0, 0)  # martes 23:00
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Huechuraba") is False

    def test_dentro_horario_sabado(self):
        fecha = datetime(2026, 1, 17, 9, 0, 0)  # sábado 09:00
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Temuco") is True

    def test_dentro_horario_domingo_false(self):
        fecha = datetime(2026, 1, 18, 10, 0, 0)  # domingo 10:00
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Huechuraba") is False

    def test_segundos_hasta_apertura_madrugada(self):
        fecha = datetime(2026, 1, 20, 2, 0, 0)  # martes 02:00
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            mock_date.combine = datetime.combine
            espera = script.segundos_hasta_apertura("Huechuraba")
            assert espera == 18600  # 07:10 - 02:00 = 5h10m = 18600s

    def test_segundos_hasta_apertura_domingo_none(self):
        fecha = datetime(2026, 1, 18, 10, 0, 0)  # domingo
        with patch(f'{_MOD}.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            mock_date.combine = datetime.combine
            assert script.segundos_hasta_apertura("Huechuraba") is None

    @pytest.mark.asyncio
    async def test_capturar_camara_reintentos_fallidos(self):
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_session.get.return_value.__aenter__.return_value = mock_resp

        class BreakLoop(Exception):
            pass

        with patch(f'{_MOD}.dentro_horario', return_value=True), \
             patch(f'{_MOD}.os.makedirs'), \
             patch('asyncio.sleep', side_effect=BreakLoop):

            try:
                await script.capturar_camara(mock_session, "Huechuraba", "ID_TEST")
            except BreakLoop:
                pass

            assert mock_session.get.call_count >= 1

    @pytest.mark.asyncio
    async def test_capturar_camara_fuera_de_horario_no_llama_get(self):
        mock_session = MagicMock()

        class BreakLoop(Exception):
            pass

        with patch(f'{_MOD}.dentro_horario', return_value=False), \
             patch(f'{_MOD}.os.makedirs'), \
             patch('asyncio.sleep', side_effect=BreakLoop):

            try:
                await script.capturar_camara(mock_session, "Huechuraba", "ID_TEST")
            except BreakLoop:
                pass

            mock_session.get.assert_not_called()


# ---------------------------------------------------------------------------
# todas_fuera_de_horario (Local)
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestTodasFueraDeHorarioLocal:

    def test_todas_fuera_cuando_ninguna_en_horario(self):
        with patch(f'{_MOD}.dentro_horario', return_value=False):
            assert script.todas_fuera_de_horario() is True

    def test_no_todas_fuera_cuando_alguna_en_horario(self):
        with patch(f'{_MOD}.dentro_horario', return_value=True):
            assert script.todas_fuera_de_horario() is False

    def test_domingo_retorna_true(self):
        fecha = datetime(2026, 1, 18, 10, 0, 0)  # domingo
        with patch(f'{_MOD}.datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            assert script.todas_fuera_de_horario() is True


# ---------------------------------------------------------------------------
# obtener_tiempos_restantes (Local)
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestObtenerTiemposRestantesLocal:

    def test_domingo_todas_none(self):
        with patch(f'{_MOD}.es_domingo', return_value=True):
            tiempos = script.obtener_tiempos_restantes()
            assert len(tiempos) > 0
            assert all(v is None for v in tiempos.values())

    def test_dentro_horario_retorna_cero(self):
        with patch(f'{_MOD}.es_domingo', return_value=False), \
             patch(f'{_MOD}.dentro_horario', return_value=True):
            tiempos = script.obtener_tiempos_restantes()
            assert all(v == 0 for v in tiempos.values())

    def test_fuera_horario_retorna_segundos_hasta_apertura(self):
        with patch(f'{_MOD}.es_domingo', return_value=False), \
             patch(f'{_MOD}.dentro_horario', return_value=False), \
             patch(f'{_MOD}.segundos_hasta_apertura', return_value=1800):
            tiempos = script.obtener_tiempos_restantes()
            assert all(v == 1800 for v in tiempos.values())


# ---------------------------------------------------------------------------
# obtener_menor_tiempo_espera (Local)
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestObtenerMenorTiempoEsperaLocal:

    def test_retorna_minimo_entre_valores_validos(self):
        with patch(f'{_MOD}.obtener_tiempos_restantes',
                   return_value={"A": 300, "B": 120, "C": 600}):
            assert script.obtener_menor_tiempo_espera() == 120

    def test_retorna_none_cuando_todos_none(self):
        with patch(f'{_MOD}.obtener_tiempos_restantes',
                   return_value={"A": None, "B": None}):
            assert script.obtener_menor_tiempo_espera() is None

    def test_ignora_ceros_y_negativos(self):
        with patch(f'{_MOD}.obtener_tiempos_restantes',
                   return_value={"A": 0, "B": -5, "C": 90}):
            assert script.obtener_menor_tiempo_espera() == 90


# ---------------------------------------------------------------------------
# SundayWorkerLocal
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestSundayWorkerLocal:

    def test_obtener_semana_anterior_retorna_tupla_valida(self):
        worker = script.SundayWorkerLocal()
        año, semana = worker.obtener_semana_anterior()
        assert isinstance(año, int)
        assert isinstance(semana, int)
        assert 1 <= semana <= 53

    def test_obtener_semana_anterior_no_es_posterior_a_hoy(self):
        from datetime import date
        worker = script.SundayWorkerLocal()
        año, semana = worker.obtener_semana_anterior()
        año_actual, semana_actual, _ = date.today().isocalendar()
        assert (año, semana) <= (año_actual, semana_actual)

    def test_hash_archivo_consistente(self):
        import tempfile
        worker = script.SundayWorkerLocal()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as f:
            f.write(b"contenido_de_prueba_flujo_prt")
            ruta = f.name
        try:
            h1 = worker.hash_archivo(ruta)
            h2 = worker.hash_archivo(ruta)
            assert h1 == h2
            assert len(h1) == 32
        finally:
            os.unlink(ruta)

    def test_hash_archivo_distinto_para_contenidos_distintos(self):
        import tempfile
        worker = script.SundayWorkerLocal()
        with tempfile.NamedTemporaryFile(delete=False) as f1:
            f1.write(b"contenido_a")
            ruta1 = f1.name
        with tempfile.NamedTemporaryFile(delete=False) as f2:
            f2.write(b"contenido_b")
            ruta2 = f2.name
        try:
            assert worker.hash_archivo(ruta1) != worker.hash_archivo(ruta2)
        finally:
            os.unlink(ruta1)
            os.unlink(ruta2)

    def test_ejecutar_no_reprocesa_misma_semana(self):
        worker = script.SundayWorkerLocal()
        semana = worker.obtener_semana_anterior()
        worker.procesado_semana = semana

        with patch.object(worker, 'identificar_conjuntos') as mock_id:
            worker.ejecutar()
            mock_id.assert_not_called()
