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
