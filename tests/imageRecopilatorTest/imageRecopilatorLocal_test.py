import sys
import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
import asyncio

# 1. Bloqueo de caché y configuración de rutas
sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Importación del script
from imageRecopilator import imageRecompilerLocal as script

@pytest.mark.imageRecopilator
class TestCamarasLocal:

    # --- Tests para es_domingo ---
    def test_es_domingo_verdadero(self):
        """Verifica que detecta correctamente un domingo (2026-01-18)"""
        fecha = datetime(2026, 1, 18, 12, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            assert script.es_domingo() is True

    def test_es_domingo_falso(self):
        """Verifica que detecta que un lunes no es domingo (2026-01-19)"""
        fecha = datetime(2026, 1, 19, 12, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            assert script.es_domingo() is False

    # --- Tests para dentro_horario ---
    def test_dentro_horario_semana_abierto(self):
        """Prueba una planta abierta un martes a las 10:00 AM"""
        # Martes 2026-01-20
        fecha = datetime(2026, 1, 20, 10, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime # Mantener funcionalidad
            assert script.dentro_horario("Huechuraba") is True

    def test_dentro_horario_semana_cerrado(self):
        """Prueba una planta cerrada un martes a las 11:00 PM"""
        fecha = datetime(2026, 1, 20, 23, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Huechuraba") is False

    def test_dentro_horario_sabado(self):
        """Prueba el horario especial de sábado para Temuco (abre 08:10)"""
        # Sábado 2026-01-17
        fecha = datetime(2026, 1, 17, 9, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            assert script.dentro_horario("Temuco") is True

    # --- Tests para segundos_hasta_apertura ---
    def test_segundos_espera_fuera_horario(self):
        """Verifica que retorna el tiempo de reintento configurado si falta mucho para abrir"""
        # Madrugada las 02:00 AM
        fecha = datetime(2026, 1, 20, 2, 0, 0)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            mock_date.combine = datetime.combine
            espera = script.segundos_hasta_apertura("Huechuraba")
            # Debería retornar el máximo configurado (600 segundos)
            assert espera == 600

    def test_segundos_espera_minimo(self):
        """Verifica que el tiempo mínimo de espera sea 60 segundos"""
        # Si son las 07:09:50 y abre a las 07:10:00 (faltan 10 seg)
        # La función debe retornar 60 por el max(60, ...)
        fecha = datetime(2026, 1, 20, 7, 9, 50)
        with patch('imageRecopilator.imageRecompilerLocal.datetime') as mock_date:
            mock_date.now.return_value = fecha
            mock_date.strptime = datetime.strptime
            mock_date.combine = datetime.combine
            espera = script.segundos_hasta_apertura("Huechuraba")
            assert espera == 60

    # --- Test Asíncrono para capturar_camara ---
    @pytest.mark.asyncio
    async def test_capturar_camara_reintentos_fallidos(self):
        """Simula 5 intentos fallidos de conexión HTTP"""
        mock_session = MagicMock()
        mock_response = AsyncMock()
        mock_response.status = 500 # Error de servidor
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mockeamos dependencias para que el bucle se detenga rápido
        with patch('imageRecopilator.imageRecompilerLocal.es_domingo', side_effect=[False, True]), \
             patch('imageRecopilator.imageRecompilerLocal.dentro_horario', return_value=True), \
             patch('imageRecopilator.imageRecompilerLocal.os.makedirs'), \
             patch('asyncio.sleep', return_value=None): # No esperar tiempo real
            
            await script.capturar_camara(mock_session, "Huechuraba", "ID_TEST")
            
            # Verificamos que se intentó 5 veces antes de rendirse en ese ciclo
            assert mock_session.get.call_count == 5