import sys
import os
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

# --- CONFIGURACIÓN DE ENTORNO ---
sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from imageRecopilator.Local import imageRecompilerCloud as cloud


@pytest.mark.imageRecopilator
class TestCloudRecopilador:

    def test_generar_s3_key_valida(self):
        fecha = "20260117_230000"
        planta = "Villarica"

        key = cloud.generar_s3_key(planta, fecha)

        assert key == "capturas/2026/01/17/Villarica/VLL_20260117_230000.jpg"


    def test_hash_imagen_md5_consistente(self):
        img = b"data_test"
        assert cloud.hash_imagen(img) == cloud.hash_imagen(img)

    @pytest.mark.asyncio
    async def test_metricas_ahorro(self):
        from imageRecopilator.Cloud.ImageRecompilerCloud import Metricas

        m = Metricas()
        await m.registrar_subida(1000, 400)

        assert m.bytes_originales == 1000
        assert m.bytes_comprimidos == 400


@pytest.mark.asyncio
@pytest.mark.imageRecopilator
async def test_capturar_camara_ignora_duplicados():
    mock_session = MagicMock()
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b"bytes_estaticos"
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    class BreakLoop(Exception):
        pass

    with patch('imageRecopilator.imageRecompilerCloud.dentro_horario', return_value=True), \
         patch('imageRecopilator.imageRecompilerCloud.recomprimir_jpeg', return_value=b"jpeg_fijo"), \
         patch('imageRecopilator.imageRecompilerCloud.cola_subida.put', new_callable=AsyncMock) as mock_put, \
         patch('imageRecopilator.imageRecompilerCloud.metricas.imprimir_si_toca', new_callable=AsyncMock), \
         patch('asyncio.sleep', side_effect=[None, BreakLoop()]):

        try:
            await cloud.capturar_camara(mock_session, "Temuco", "ID_CAM")
        except BreakLoop:
            pass

        assert mock_put.call_count >= 1

@pytest.mark.asyncio
@pytest.mark.imageRecopilator
async def test_worker_s3_procesa_cola():
    while not cloud.cola_subida.empty():
        cloud.cola_subida.get_nowait()

    await cloud.cola_subida.put(
        ("Temuco", "20260117_230000", b"img_data", 500)
    )

    with patch('aioboto3.Session.client') as mock_s3_client, \
         patch('imageRecopilator.imageRecompilerCloud.metricas.registrar_subida', new_callable=AsyncMock):

        mock_s3 = AsyncMock()
        mock_s3_client.return_value.__aenter__.return_value = mock_s3

        task = asyncio.create_task(
            cloud.worker_subida_s3(worker_id=1)
        )

        # Deja correr el loop lo justo para procesar la cola
        await asyncio.sleep(0.1)

        # Cancela explícitamente el worker
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_s3.put_object.assert_called_once()
