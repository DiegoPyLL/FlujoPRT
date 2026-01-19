import sys
import os
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

# --- CONFIGURACIÓN DE ENTORNO ---
sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from imageRecopilator import imageRecompilerLocal as script


@pytest.mark.imageRecopilator
class TestCamarasLocal:

    def test_es_domingo_true(self):
        fecha = datetime(2026, 1, 18, 12, 0, 0)  # domingo
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            assert script.es_domingo() is True

    def test_es_domingo_false(self):
        fecha = datetime(2026, 1, 19, 12, 0, 0)  # lunes
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            assert script.es_domingo() is False

    def test_dentro_horario_semana_abierto(self):
        fecha = datetime(2026, 1, 20, 10, 0, 0)  # martes
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Huechuraba") is True

    def test_dentro_horario_semana_cerrado(self):
        fecha = datetime(2026, 1, 20, 23, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Huechuraba") is False

    def test_dentro_horario_sabado(self):
        fecha = datetime(2026, 1, 17, 9, 0, 0)  # sábado
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Temuco") is True

    def test_segundos_hasta_apertura_madrugada(self):
        fecha = datetime(2026, 1, 20, 2, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            mock_date.combine = datetime.combine

            espera = script.segundos_hasta_apertura("Huechuraba")

            assert espera == 18600  # 07:10 - 02:00

    @pytest.mark.asyncio
    async def test_capturar_camara_reintentos_fallidos(self):
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_session.get.return_value.__aenter__.return_value = mock_resp

        class BreakLoop(Exception):
            pass

        with patch('imageRecopilator.imageRecompilerLocal.dentro_horario', return_value=True), \
            patch('imageRecopilator.imageRecompilerLocal.os.makedirs'), \
            patch('asyncio.sleep', side_effect=BreakLoop):

            try:
                await script.capturar_camara(mock_session, "Huechuraba", "ID_TEST")
            except BreakLoop:
                pass

            assert mock_session.get.call_count >= 1
