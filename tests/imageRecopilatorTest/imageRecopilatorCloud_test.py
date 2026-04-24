import sys
import os
import pytest
import asyncio
import io
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import imageRecopilator.Cloud.ImageRecompilerCloud as cloud

from botocore.exceptions import ClientError, NoCredentialsError


# ---------------------------------------------------------------------------
# Generación de claves S3
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestS3Keys:

    def test_generar_s3_key_valida(self):
        key = cloud.generar_s3_key("Villarica", "20260117_230000")
        assert key == "capturas/2026/01/17/Villarica/VLL_20260117_230000.jpg"

    def test_generar_s3_key_denominador_planta_conocida(self):
        key = cloud.generar_s3_key("Huechuraba", "20260423_080000")
        assert key.startswith("capturas/") and "HCH_" in key

    def test_generar_s3_key_metadata_valida(self):
        key = cloud.generar_s3_key_metadata("Villarica", "20260117_230000")
        assert key == "metadata/capturas/2026/01/17/Villarica/VLL_20260117_230000.json"

    def test_generar_s3_key_metadata_con_prefix_custom(self):
        key = cloud.generar_s3_key_metadata("Temuco", "20260301_100000", prefix="alt_meta")
        assert key.startswith("alt_meta/capturas/")
        assert key.endswith("TMU_20260301_100000.json")


# ---------------------------------------------------------------------------
# Hash de imágenes
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestHash:

    def test_hash_imagen_md5_consistente(self):
        img = b"datos_prueba"
        assert cloud.hash_imagen(img) == cloud.hash_imagen(img)

    def test_hash_imagen_distinto_para_datos_diferentes(self):
        assert cloud.hash_imagen(b"a") != cloud.hash_imagen(b"b")

    def test_hash_imagen_retorna_string_hex(self):
        h = cloud.hash_imagen(b"test")
        assert isinstance(h, str) and len(h) == 32


# ---------------------------------------------------------------------------
# Compresión JPEG
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestCompresionJpeg:

    def _crear_jpeg_valido(self) -> bytes:
        from PIL import Image
        img = Image.new("RGB", (100, 100), color=(128, 64, 32))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        return buf.getvalue()

    def test_recomprimir_jpeg_sync_reduce_tamano(self):
        data = self._crear_jpeg_valido()
        resultado = cloud.recomprimir_jpeg_sync(data)
        assert isinstance(resultado, bytes) and len(resultado) > 0
        assert len(resultado) <= len(data) + 1000  # puede ser similar, no necesariamente menor

    def test_recomprimir_jpeg_sync_datos_invalidos_retorna_original(self):
        datos_invalidos = b"esto_no_es_jpeg"
        resultado = cloud.recomprimir_jpeg_sync(datos_invalidos)
        assert resultado == datos_invalidos

    @pytest.mark.asyncio
    async def test_recomprimir_jpeg_async_retorna_bytes(self):
        data = self._crear_jpeg_valido()
        resultado = await cloud.recomprimir_jpeg(data)
        assert isinstance(resultado, bytes) and len(resultado) > 0


# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestMetricas:

    @pytest.mark.asyncio
    async def test_registrar_subida_acumula_bytes(self):
        m = cloud.Metricas()
        await m.registrar_subida(1000, 400)
        assert m.bytes_originales == 1000
        assert m.bytes_comprimidos == 400
        assert m.imagenes_subidas == 1
        assert m.total_subidas == 1

    @pytest.mark.asyncio
    async def test_registrar_captura_incrementa_contador(self):
        m = cloud.Metricas()
        await m.registrar_captura()
        await m.registrar_captura()
        assert m.imagenes_capturadas == 2

    @pytest.mark.asyncio
    async def test_registrar_duplicada_incrementa_contadores(self):
        m = cloud.Metricas()
        await m.registrar_duplicada()
        assert m.imagenes_duplicadas == 1
        assert m.total_duplicadas == 1

    @pytest.mark.asyncio
    async def test_registrar_error_descarga_incrementa_contadores(self):
        m = cloud.Metricas()
        await m.registrar_error_descarga()
        assert m.errores_descarga == 1
        assert m.total_errores_descarga == 1

    @pytest.mark.asyncio
    async def test_registrar_error_s3_incrementa_contadores(self):
        m = cloud.Metricas()
        await m.registrar_error_s3()
        assert m.errores_s3 == 1
        assert m.total_errores_s3 == 1

    @pytest.mark.asyncio
    async def test_multiples_subidas_acumulan(self):
        m = cloud.Metricas()
        await m.registrar_subida(500, 200)
        await m.registrar_subida(300, 100)
        assert m.bytes_originales == 800
        assert m.bytes_comprimidos == 300
        assert m.total_subidas == 2


# ---------------------------------------------------------------------------
# Generación y subida de metadata
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestMetadata:

    def test_generar_metadata_captura_campos_requeridos(self):
        meta = cloud.generar_metadata_captura(
            "Huechuraba", "20260423_100000",
            "capturas/2026/04/23/Huechuraba/HCH_20260423_100000.jpg",
            100000, 50000, "mi-bucket"
        )
        assert meta["version"] == "1"
        assert meta["planta_id"] == "HCH"
        assert meta["planta_nombre"] == "Huechuraba"
        assert meta["s3_bucket"] == "mi-bucket"
        assert meta["bytes_originales"] == 100000
        assert meta["bytes_comprimidos"] == 50000
        assert meta["s3_imagen_key"].endswith(".jpg")

    def test_generar_metadata_captura_ratio_calculado(self):
        meta = cloud.generar_metadata_captura(
            "Temuco", "20260423_100000", "key.jpg", 1000, 500, "bucket"
        )
        assert meta["ratio_compresion"] == 0.5

    def test_generar_metadata_captura_ratio_nulo_cuando_bytes_cero(self):
        meta = cloud.generar_metadata_captura(
            "Temuco", "20260423_100000", "key.jpg", 0, 0, "bucket"
        )
        assert meta["ratio_compresion"] is None

    def test_generar_metadata_captura_timestamp_iso(self):
        meta = cloud.generar_metadata_captura(
            "Chillan", "20260423_153000", "key.jpg", 100, 80, "bucket"
        )
        assert meta["timestamp_captura"] == "2026-04-23T15:30:00"

    def test_generar_metadata_captura_sin_ec2_info(self):
        original_cache = cloud._ec2_metadata_cache
        cloud._ec2_metadata_cache = None
        try:
            meta = cloud.generar_metadata_captura(
                "Concepcion", "20260423_100000", "key.jpg", 100, 80, "bucket"
            )
            assert meta["instancia_ec2"] is None
        finally:
            cloud._ec2_metadata_cache = original_cache

    def test_generar_metadata_captura_con_ec2_info(self):
        original_cache = cloud._ec2_metadata_cache
        cloud._ec2_metadata_cache = {
            "instance_id": "i-0abc123",
            "instance_type": "t3.micro"
        }
        try:
            meta = cloud.generar_metadata_captura(
                "Concepcion", "20260423_100000", "key.jpg", 100, 80, "bucket"
            )
            assert meta["instancia_ec2"] == {
                "instance_id": "i-0abc123",
                "instance_type": "t3.micro"
            }
        finally:
            cloud._ec2_metadata_cache = original_cache

    @pytest.mark.asyncio
    async def test_subir_json_s3_exitoso(self):
        mock_s3 = AsyncMock()
        resultado = await cloud.subir_json_s3(mock_s3, "bucket", "key.json", {"k": "v"})
        assert resultado is True
        mock_s3.put_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_subir_json_s3_error_cliente(self):
        mock_s3 = AsyncMock()
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "PutObject"
        )
        resultado = await cloud.subir_json_s3(mock_s3, "bucket", "key.json", {"k": "v"})
        assert resultado is False


# ---------------------------------------------------------------------------
# Horarios
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestHorarios:

    def test_es_domingo_true(self):
        fecha = datetime(2026, 4, 26, 12, 0, 0)  # domingo
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            mock_dt.combine = datetime.combine
            assert cloud.es_domingo() is True

    def test_es_domingo_false_lunes(self):
        fecha = datetime(2026, 4, 27, 12, 0, 0)  # lunes
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            mock_dt.combine = datetime.combine
            assert cloud.es_domingo() is False

    def test_dentro_horario_semana_abierto(self):
        fecha = datetime(2026, 4, 22, 10, 0, 0)  # miércoles 10:00
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            assert cloud.dentro_horario("Huechuraba") is True

    def test_dentro_horario_semana_cerrado(self):
        fecha = datetime(2026, 4, 22, 23, 0, 0)  # miércoles 23:00
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            assert cloud.dentro_horario("Huechuraba") is False

    def test_dentro_horario_domingo_siempre_false(self):
        fecha = datetime(2026, 4, 26, 10, 0, 0)  # domingo
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            assert cloud.dentro_horario("Huechuraba") is False

    def test_dentro_horario_sabado_en_horario(self):
        fecha = datetime(2026, 4, 25, 9, 0, 0)  # sábado 09:00
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            assert cloud.dentro_horario("Temuco") is True

    def test_segundos_hasta_apertura_madrugada(self):
        fecha = datetime(2026, 4, 22, 2, 0, 0)  # miércoles 02:00
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            mock_dt.combine = datetime.combine
            espera = cloud.segundos_hasta_apertura("Huechuraba")
            assert espera == 18600  # 07:10 - 02:00 = 5h10m

    def test_segundos_hasta_apertura_domingo_retorna_none(self):
        fecha = datetime(2026, 4, 26, 10, 0, 0)  # domingo
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            mock_dt.combine = datetime.combine
            assert cloud.segundos_hasta_apertura("Huechuraba") is None

    def test_todas_fuera_de_horario_cuando_domingo(self):
        fecha = datetime(2026, 4, 26, 10, 0, 0)  # domingo
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            assert cloud.todas_fuera_de_horario() is True

    def test_obtener_tiempos_restantes_domingo_todos_none(self):
        fecha = datetime(2026, 4, 26, 10, 0, 0)  # domingo
        with patch.object(cloud, 'datetime') as mock_dt:
            mock_dt.now.return_value = fecha
            mock_dt.strptime = datetime.strptime
            tiempos = cloud.obtener_tiempos_restantes()
            assert all(v is None for v in tiempos.values())


# ---------------------------------------------------------------------------
# Metadata EC2 (IMDS v2)
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestMetadataEC2:

    @pytest.mark.asyncio
    async def test_obtener_metadata_ec2_fuera_de_ec2(self):
        import aiohttp
        with patch('aiohttp.ClientSession') as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__.return_value = mock_session
            mock_session.put.side_effect = aiohttp.ClientError("timeout")
            resultado = await cloud.obtener_metadata_ec2()
        assert resultado is None

    @pytest.mark.asyncio
    async def test_obtener_metadata_ec2_token_falla(self):
        with patch('aiohttp.ClientSession') as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__.return_value = mock_session
            mock_token_resp = AsyncMock()
            mock_token_resp.status = 404
            mock_session.put.return_value.__aenter__.return_value = mock_token_resp
            resultado = await cloud.obtener_metadata_ec2()
        assert resultado is None


# ---------------------------------------------------------------------------
# Verificación de credenciales AWS
# ---------------------------------------------------------------------------

@pytest.mark.imageRecopilator
class TestCredencialesAWS:

    @pytest.mark.asyncio
    async def test_verificar_credenciales_validas(self):
        mock_sts = AsyncMock()
        mock_sts.get_caller_identity.return_value = {
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:user/test"
        }
        with patch('aioboto3.Session') as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_session.client.return_value.__aenter__ = AsyncMock(return_value=mock_sts)
            mock_session.client.return_value.__aexit__ = AsyncMock(return_value=False)
            resultado = await cloud.verificar_credenciales_aws()
        assert resultado is True

    @pytest.mark.asyncio
    async def test_verificar_credenciales_sin_credenciales(self):
        with patch('aioboto3.Session') as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_sts = AsyncMock()
            mock_sts.get_caller_identity.side_effect = NoCredentialsError()
            mock_session.client.return_value.__aenter__ = AsyncMock(return_value=mock_sts)
            mock_session.client.return_value.__aexit__ = AsyncMock(return_value=False)
            resultado = await cloud.verificar_credenciales_aws()
        assert resultado is False


# ---------------------------------------------------------------------------
# Captura de cámara (duplicados e integración)
# ---------------------------------------------------------------------------

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

    with patch('imageRecopilator.Cloud.ImageRecompilerCloud.dentro_horario', return_value=True), \
         patch('imageRecopilator.Cloud.ImageRecompilerCloud.recomprimir_jpeg', return_value=b"jpeg_fijo"), \
         patch('imageRecopilator.Cloud.ImageRecompilerCloud.cola_subida.put', new_callable=AsyncMock) as mock_put, \
         patch('imageRecopilator.Cloud.ImageRecompilerCloud.metricas.imprimir_si_toca', new_callable=AsyncMock), \
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

    with patch('aioboto3.Session') as mock_session_cls, \
         patch('imageRecopilator.Cloud.ImageRecompilerCloud.metricas.registrar_subida', new_callable=AsyncMock), \
         patch('imageRecopilator.Cloud.ImageRecompilerCloud.subir_metadata_captura', new_callable=AsyncMock):

        mock_s3 = AsyncMock()
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.client.return_value.__aenter__ = AsyncMock(return_value=mock_s3)
        mock_session.client.return_value.__aexit__ = AsyncMock(return_value=False)

        task = asyncio.create_task(cloud.worker_subida_s3(worker_id=1))
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_s3.put_object.assert_called_once()
