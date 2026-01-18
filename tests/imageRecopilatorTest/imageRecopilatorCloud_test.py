import sys
import os
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
from imageRecopilator import imageRecompilerCloud as cloud

# --- CONFIGURACIÓN DE ENTORNO --
sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)





@pytest.mark.imageRecopilator
class TestCloudRecopilador:

    @pytest.mark.imageRecopilator
    def test_generar_s3_key_valida(self):
        """Verifica la construcción de la ruta S3"""
        fecha = "20260117_230000"
        planta = "Villarica"
        key = cloud.generar_s3_key(planta, fecha)
        assert "Villarica/2026/01/17" in key

    @pytest.mark.imageRecopilator
    def test_hash_imagen_md5(self):
        """Verifica que el hash sea consistente"""
        img = b"data_test"
        assert cloud.hash_imagen(img) == cloud.hash_imagen(img)

    @pytest.mark.asyncio
    @pytest.mark.imageRecopilator
    async def test_metricas_ahorro(self):
        """Verifica el cálculo de métricas de compresión"""
        from imageRecopilator.imageRecompilerCloud import Metricas
        m = Metricas()
        await m.registrar_subida(1000, 400) # 60% ahorro
        assert m.bytes_originales == 1000
        assert m.bytes_comprimidos == 400



# --- CONFIGURACIÓN DE ENTORNO ---

@pytest.mark.asyncio
@pytest.mark.imageRecopilator
async def test_capturar_camara_duplicados(): # <--- SE QUITÓ 'self'
    """Prueba que el bucle ignore imágenes idénticas sin colgarse"""
    mock_session = MagicMock()
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b"bytes_estaticos"
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    # Definimos una excepción para romper el bucle a la fuerza
    class BreakLoop(Exception): pass

    # Mockeamos dependencias críticas
    with patch('imageRecopilator.imageRecompilerCloud.dentro_horario', return_value=True), \
         patch('imageRecopilator.imageRecompilerCloud.recomprimir_jpeg', return_value=b"jpeg_fijo"), \
         patch('imageRecopilator.imageRecompilerCloud.cola_subida.put', new_callable=AsyncMock) as mock_put, \
         patch('imageRecopilator.imageRecompilerCloud.metricas.imprimir_si_toca', new_callable=AsyncMock), \
         patch('asyncio.sleep', side_effect=[None, BreakLoop()]): # El segundo sleep mata el proceso
        
        try:
            # Ejecutamos la función
            await cloud.capturar_camara(mock_session, "Temuco", "ID_CAM")
        except BreakLoop:
            pass  # Salida limpia del bucle diseñado para el test
        
        # Verificamos que se intentó encolar al menos una vez
        assert mock_put.call_count >= 1


class BreakLoop(Exception): pass

@pytest.mark.asyncio
@pytest.mark.imageRecopilator
async def test_worker_s3_procesa_cola():
    """Verifica que el worker procese un item y cierre correctamente"""
    # 1. Limpiar cola global antes de empezar
    while not cloud.cola_subida.empty():
        cloud.cola_subida.get_nowait()
        
    # 2. Insertar un item de prueba
    await cloud.cola_subida.put(("Temuco", "20260117_230000", b"img_data", 500))

    # 3. Mocks de S3 y control de ejecución
    with patch('aioboto3.Session.client') as mock_s3_client, \
         patch('imageRecopilator.imageRecompilerCloud.RUNNING', side_effect=[True, False]), \
         patch('imageRecopilator.imageRecompilerCloud.metricas.registrar_subida', new_callable=AsyncMock):
        
        mock_s3 = AsyncMock()
        mock_s3_client.return_value.__aenter__.return_value = mock_s3
        
        # Ejecutamos con timeout de seguridad
        try:
            await asyncio.wait_for(
                cloud.worker_subida_s3(worker_id=1), 
                timeout=2.0
            )
        except (asyncio.TimeoutError, BreakLoop):
            pass

        # 4. Verificamos que se llamó a la función de subida de AWS
        assert mock_s3.put_object.called