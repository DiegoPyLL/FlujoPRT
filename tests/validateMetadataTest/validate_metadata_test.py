import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

sys.dont_write_bytecode = True
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
scripts_dir = os.path.join(project_root, "scripts")

if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

import validate_metadata as vm


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _registro_valido(**overrides) -> dict:
    base = {
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
        "instancia_ec2": None,
        "generado_en": "2026-04-23T10:00:05",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _iso_parseable
# ---------------------------------------------------------------------------

class TestIsoParseable:

    def test_fecha_iso_valida(self):
        assert vm._iso_parseable("2026-04-23T10:00:00") is True

    def test_fecha_iso_con_offset(self):
        assert vm._iso_parseable("2026-04-23T10:00:00+00:00") is True

    def test_fecha_solo_dia(self):
        assert vm._iso_parseable("2026-04-23") is True

    def test_string_invalido(self):
        assert vm._iso_parseable("no-es-fecha") is False

    def test_string_vacio(self):
        assert vm._iso_parseable("") is False

    def test_none_retorna_false(self):
        assert vm._iso_parseable(None) is False  # type: ignore

    def test_numero_retorna_false(self):
        assert vm._iso_parseable(12345) is False  # type: ignore


# ---------------------------------------------------------------------------
# validar_registro — registro válido
# ---------------------------------------------------------------------------

class TestValidarRegistroValido:

    def test_registro_completo_sin_errores(self):
        errores = vm.validar_registro("test_key.json", _registro_valido())
        assert errores == []

    def test_instancia_ec2_con_campos_correctos(self):
        rec = _registro_valido(instancia_ec2={
            "instance_id": "i-0abc123",
            "instance_type": "t3.micro"
        })
        assert vm.validar_registro("key", rec) == []

    def test_ratio_compresion_null_es_valido(self):
        rec = _registro_valido(ratio_compresion=None)
        assert vm.validar_registro("key", rec) == []

    def test_bytes_cero_son_validos(self):
        rec = _registro_valido(bytes_originales=0, bytes_comprimidos=0)
        assert vm.validar_registro("key", rec) == []

    def test_ratio_entero_es_valido(self):
        rec = _registro_valido(ratio_compresion=1)
        assert vm.validar_registro("key", rec) == []


# ---------------------------------------------------------------------------
# validar_registro — campos ausentes
# ---------------------------------------------------------------------------

class TestValidarRegistroCampoAusente:

    @pytest.mark.parametrize("campo", [
        "version", "planta_id", "planta_nombre", "plataforma",
        "timestamp_captura", "fecha_str", "s3_imagen_key", "s3_bucket",
        "bytes_originales", "bytes_comprimidos", "ratio_compresion",
        "instancia_ec2", "generado_en"
    ])
    def test_campo_ausente_genera_error(self, campo):
        rec = _registro_valido()
        del rec[campo]
        errores = vm.validar_registro("key.json", rec)
        assert len(errores) >= 1
        assert campo in errores[0]


# ---------------------------------------------------------------------------
# validar_registro — errores de valor
# ---------------------------------------------------------------------------

class TestValidarRegistroErrores:

    def test_version_incorrecta(self):
        errores = vm.validar_registro("k", _registro_valido(version="2"))
        assert any("version" in e for e in errores)

    def test_planta_id_desconocida(self):
        errores = vm.validar_registro("k", _registro_valido(planta_id="XYZ"))
        assert any("planta_id" in e for e in errores)

    def test_planta_id_vacia(self):
        errores = vm.validar_registro("k", _registro_valido(planta_id="   "))
        assert any("planta_id" in e for e in errores)

    def test_planta_nombre_vacio(self):
        errores = vm.validar_registro("k", _registro_valido(planta_nombre=""))
        assert any("planta_nombre" in e for e in errores)

    def test_plataforma_vacia(self):
        errores = vm.validar_registro("k", _registro_valido(plataforma=""))
        assert any("plataforma" in e for e in errores)

    def test_timestamp_captura_no_iso(self):
        errores = vm.validar_registro("k", _registro_valido(timestamp_captura="23/04/2026"))
        assert any("timestamp_captura" in e for e in errores)

    def test_fecha_str_formato_incorrecto(self):
        errores = vm.validar_registro("k", _registro_valido(fecha_str="2026-04-23 10:00:00"))
        assert any("fecha_str" in e for e in errores)

    def test_fecha_str_demasiado_corta(self):
        errores = vm.validar_registro("k", _registro_valido(fecha_str="20260423"))
        assert any("fecha_str" in e for e in errores)

    def test_s3_imagen_key_sin_extension_jpg(self):
        errores = vm.validar_registro("k", _registro_valido(s3_imagen_key="capturas/imagen.png"))
        assert any("s3_imagen_key" in e for e in errores)

    def test_s3_bucket_vacio(self):
        errores = vm.validar_registro("k", _registro_valido(s3_bucket=""))
        assert any("s3_bucket" in e for e in errores)

    def test_bytes_originales_negativo(self):
        errores = vm.validar_registro("k", _registro_valido(bytes_originales=-1))
        assert any("bytes_originales" in e for e in errores)

    def test_bytes_comprimidos_negativo(self):
        errores = vm.validar_registro("k", _registro_valido(bytes_comprimidos=-1))
        assert any("bytes_comprimidos" in e for e in errores)

    def test_bytes_originales_float(self):
        errores = vm.validar_registro("k", _registro_valido(bytes_originales=100.5))
        assert any("bytes_originales" in e for e in errores)

    def test_ratio_negativo(self):
        errores = vm.validar_registro("k", _registro_valido(ratio_compresion=-0.1))
        assert any("ratio_compresion" in e for e in errores)

    def test_ratio_cero_invalido(self):
        errores = vm.validar_registro("k", _registro_valido(ratio_compresion=0))
        assert any("ratio_compresion" in e for e in errores)

    def test_instancia_ec2_no_es_dict(self):
        errores = vm.validar_registro("k", _registro_valido(instancia_ec2="i-123"))
        assert any("instancia_ec2" in e for e in errores)

    def test_instancia_ec2_falta_instance_id(self):
        errores = vm.validar_registro("k", _registro_valido(
            instancia_ec2={"instance_type": "t3.micro"}
        ))
        assert any("instance_id" in e for e in errores)

    def test_instancia_ec2_falta_instance_type(self):
        errores = vm.validar_registro("k", _registro_valido(
            instancia_ec2={"instance_id": "i-0abc"}
        ))
        assert any("instance_type" in e for e in errores)

    def test_generado_en_no_iso(self):
        errores = vm.validar_registro("k", _registro_valido(generado_en="ayer"))
        assert any("generado_en" in e for e in errores)

    def test_multiples_errores_en_mismo_registro(self):
        rec = _registro_valido(version="99", planta_id="ZZZ", bytes_originales=-5)
        errores = vm.validar_registro("k", rec)
        assert len(errores) >= 3


# ---------------------------------------------------------------------------
# S3: listar_keys_hoy
# ---------------------------------------------------------------------------

class TestListarKeysHoy:

    def test_retorna_solo_json(self):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "metadata/capturas/2026/04/23/p/A.json"},
                    {"Key": "metadata/capturas/2026/04/23/p/B.jpg"},
                    {"Key": "metadata/capturas/2026/04/23/p/C.json"},
                ]
            }
        ]
        with patch.object(vm, 'datetime') as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026/04/23"
            keys = vm.listar_keys_hoy(mock_s3, "bucket", "metadata/capturas")

        assert len(keys) == 2
        assert all(k.endswith(".json") for k in keys)

    def test_paginator_vacio_retorna_lista_vacia(self):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]  # sin "Contents"
        with patch.object(vm, 'datetime') as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026/04/23"
            keys = vm.listar_keys_hoy(mock_s3, "bucket", "metadata/capturas")

        assert keys == []


# ---------------------------------------------------------------------------
# S3: descargar_json
# ---------------------------------------------------------------------------

class TestDescargarJson:

    def test_descarga_exitosa(self):
        payload = {"version": "1", "planta_id": "HCH"}
        mock_s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(payload).encode("utf-8")
        mock_s3.get_object.return_value = {"Body": mock_body}

        resultado = vm.descargar_json(mock_s3, "bucket", "key.json")
        assert resultado == payload

    def test_json_malformado_retorna_none(self):
        mock_s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = b"{ invalid json }"
        mock_s3.get_object.return_value = {"Body": mock_body}

        resultado = vm.descargar_json(mock_s3, "bucket", "key.json")
        assert resultado is None

    def test_error_s3_retorna_none(self):
        from botocore.exceptions import ClientError
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
        )
        resultado = vm.descargar_json(mock_s3, "bucket", "key.json")
        assert resultado is None


# ---------------------------------------------------------------------------
# run_ciclo
# ---------------------------------------------------------------------------

class TestRunCiclo:

    def test_ciclo_con_registros_validos(self):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "metadata/capturas/2026/04/23/p/A.json"}]}
        ]

        payload = _registro_valido()
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(payload).encode("utf-8")
        mock_s3.get_object.return_value = {"Body": mock_body}

        with patch.object(vm, 'datetime') as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026/04/23"
            vm.run_ciclo(mock_s3, "bucket", "metadata/capturas")

    def test_ciclo_sin_keys(self):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]

        with patch.object(vm, 'datetime') as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026/04/23"
            vm.run_ciclo(mock_s3, "bucket", "metadata/capturas")
