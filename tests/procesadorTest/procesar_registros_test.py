import json
import logging
import sys
import os
from pathlib import Path

import pytest

sys.dont_write_bytecode = True

import procesar_registros as pr


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def logger():
    log = logging.getLogger("test_procesar_registros")
    log.addHandler(logging.NullHandler())
    return log


@pytest.fixture
def registro_valido():
    return {
        "archivo": "HCH_20260419_100523.jpg",
        "s3_key": "capturas/2026/04/19/Huechuraba/HCH_20260419_100523.jpg",
        "planta": "Huechuraba",
        "planta_codigo": "HCH",
        "fecha": "2026-04-19",
        "hora": "10:05:23",
        "bytes_archivo": 85432,
        "ancho_px": 1920,
        "alto_px": 1080,
        "conteo": {"auto": 1, "moto": 0, "bus": 0, "camion": 0, "total": 1},
        "detecciones": [{"bbox": [120, 200, 400, 480], "tipo": "auto", "confianza": 0.87}],
        "error": None,
    }


# ---------------------------------------------------------------------------
# limpiar_errores
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_limpiar_errores_rechaza_registro_con_error_no_nulo(logger, registro_valido):
    registro_valido["error"] = "timeout al descargar imagen"
    validos, rechazados = pr.limpiar_errores([registro_valido], logger)
    assert len(validos) == 0
    assert len(rechazados) == 1
    assert "motivo_rechazo" in rechazados[0]


@pytest.mark.procesador
def test_limpiar_errores_conserva_registro_con_error_nulo(logger, registro_valido):
    validos, rechazados = pr.limpiar_errores([registro_valido], logger)
    assert len(validos) == 1
    assert len(rechazados) == 0


# ---------------------------------------------------------------------------
# limpiar_campos_nulos
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_limpiar_campos_nulos_rechaza_fila_sin_s3_key(logger, registro_valido):
    del registro_valido["s3_key"]
    validos, rechazados = pr.limpiar_campos_nulos([registro_valido], logger)
    assert len(validos) == 0
    assert rechazados[0]["motivo_rechazo"] == "campo_nulo: s3_key"


# ---------------------------------------------------------------------------
# limpiar_duplicados
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_limpiar_duplicados_conserva_primera_ocurrencia(logger, registro_valido):
    r2 = dict(registro_valido)
    validos, rechazados = pr.limpiar_duplicados([registro_valido, r2], logger)
    assert len(validos) == 1
    assert validos[0]["s3_key"] == registro_valido["s3_key"]


@pytest.mark.procesador
def test_limpiar_duplicados_rechaza_segunda_aparicion(logger, registro_valido):
    r2 = dict(registro_valido)
    validos, rechazados = pr.limpiar_duplicados([registro_valido, r2], logger)
    assert len(rechazados) == 1
    assert rechazados[0]["motivo_rechazo"] == "duplicado_s3_key"


# ---------------------------------------------------------------------------
# agregar_hora_categoria
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_agregar_hora_categoria_clasifica_manana_tarde_cierre(logger, registro_valido):
    casos = [
        ("07:30:00", "manana"),
        ("14:00:00", "tarde"),
        ("19:00:00", "cierre"),
        ("00:00:00", "fuera_horario"),
    ]
    for hora, categoria_esperada in casos:
        r = dict(registro_valido)
        r["hora"] = hora
        resultado = pr.agregar_hora_categoria([r], logger)
        assert resultado[0]["hora_categoria"] == categoria_esperada, f"hora={hora}"


# ---------------------------------------------------------------------------
# agregar_metricas_deteccion
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_agregar_metricas_deteccion_calcula_confianza_media(logger, registro_valido):
    registro_valido["detecciones"] = [
        {"tipo": "auto", "confianza": 0.80},
        {"tipo": "moto", "confianza": 0.60},
    ]
    resultado = pr.agregar_metricas_deteccion([registro_valido], logger)
    assert resultado[0]["confianza_media"] == pytest.approx(0.70, abs=1e-4)


@pytest.mark.procesador
def test_agregar_metricas_deteccion_conteo_consistente(logger, registro_valido):
    resultado = pr.agregar_metricas_deteccion([registro_valido], logger)
    assert resultado[0]["conteo_consistente"] is True

    r = dict(registro_valido)
    r["conteo"] = {"auto": 1, "moto": 0, "bus": 0, "camion": 0, "total": 99}
    resultado2 = pr.agregar_metricas_deteccion([r], logger)
    assert resultado2[0]["conteo_consistente"] is False


# ---------------------------------------------------------------------------
# validar_tipos_y_estructura
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_validar_tipos_estructura_detecta_fecha_mal_formada(logger, registro_valido):
    registro_valido["fecha"] = "19-04-2026"
    warnings = pr.validar_tipos_y_estructura([registro_valido], logger)
    tipos = [w["tipo"] for w in warnings]
    assert "formato_fecha_invalido" in tipos


# ---------------------------------------------------------------------------
# validar_semantica
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_validar_semantica_detecta_fecha_futura(logger, registro_valido):
    registro_valido["fecha"] = "2099-01-01"
    warnings = pr.validar_semantica([registro_valido], logger)
    tipos = [w["tipo"] for w in warnings]
    assert "fecha_futura" in tipos


@pytest.mark.procesador
def test_validar_semantica_detecta_confianza_fuera_de_rango(logger, registro_valido):
    registro_valido["detecciones"] = [{"tipo": "auto", "confianza": 1.5}]
    warnings = pr.validar_semantica([registro_valido], logger)
    tipos = [w["tipo"] for w in warnings]
    assert "confianza_invalida" in tipos


@pytest.mark.procesador
def test_validar_semantica_detecta_tipo_vehiculo_invalido(logger, registro_valido):
    registro_valido["detecciones"] = [{"tipo": "avion", "confianza": 0.9}]
    warnings = pr.validar_semantica([registro_valido], logger)
    tipos = [w["tipo"] for w in warnings]
    assert "tipo_vehiculo_invalido" in tipos


# ---------------------------------------------------------------------------
# validar_integridad_referencial
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_validar_integridad_referencial_detecta_planta_desconocida(logger, registro_valido):
    registro_valido["planta"] = "PlantaInexistente"
    errores = pr.validar_integridad_referencial([registro_valido], logger)
    tipos = [e["tipo"] for e in errores]
    assert "planta_desconocida" in tipos


@pytest.mark.procesador
def test_validar_integridad_referencial_detecta_codigo_incorrecto(logger, registro_valido):
    registro_valido["planta_codigo"] = "XXX"
    errores = pr.validar_integridad_referencial([registro_valido], logger)
    tipos = [e["tipo"] for e in errores]
    assert "codigo_incorrecto" in tipos


# ---------------------------------------------------------------------------
# generar_reporte
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_reporte_json_tiene_estructura_esperada(logger):
    stats = {
        "registros_cargados": 10,
        "registros_validos": 8,
        "registros_rechazados": 2,
        "rechazados_por_error_deteccion": 1,
        "rechazados_por_campo_nulo": 1,
        "rechazados_por_duplicado": 0,
        "por_planta": {"Huechuraba": {"total": 8, "con_vehiculos": 5, "sin_vehiculos": 3}},
    }
    reporte = pr.generar_reporte(stats, [], [], [], {}, "capturas/2026/04/29/")
    assert "version" in reporte
    assert "resumen" in reporte
    assert "validacion_estructura" in reporte
    assert "validacion_semantica" in reporte
    assert "integridad_referencial" in reporte
    assert "distribucion_por_planta" in reporte
    assert reporte["resumen"]["registros_validos"] == 8


# ---------------------------------------------------------------------------
# guardar_procesados
# ---------------------------------------------------------------------------

@pytest.mark.procesador
def test_guardar_procesados_no_crea_archivo_rechazados_si_lista_vacia(logger, registro_valido, tmp_path):
    ruta_validos, ruta_rechazados = pr.guardar_procesados(
        [registro_valido], [], tmp_path, "20260429", logger
    )
    assert ruta_validos.exists()
    assert ruta_rechazados is None
