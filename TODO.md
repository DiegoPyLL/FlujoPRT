# TODO

## Tests pendientes — `scripts/procesar_registros.py`

Ubicar en `tests/procesadorTest/procesar_registros_test.py`.

- [ ] `test_limpiar_errores_rechaza_registro_con_error_no_nulo`
- [ ] `test_limpiar_errores_conserva_registro_con_error_nulo`
- [ ] `test_limpiar_campos_nulos_rechaza_fila_sin_s3_key`
- [ ] `test_limpiar_duplicados_conserva_primera_ocurrencia`
- [ ] `test_limpiar_duplicados_rechaza_segunda_aparicion`
- [ ] `test_agregar_hora_categoria_clasifica_manana_tarde_cierre`
- [ ] `test_agregar_metricas_deteccion_calcula_confianza_media`
- [ ] `test_agregar_metricas_deteccion_conteo_consistente`
- [ ] `test_validar_tipos_estructura_detecta_fecha_mal_formada`
- [ ] `test_validar_semantica_detecta_fecha_futura`
- [ ] `test_validar_semantica_detecta_confianza_fuera_de_rango`
- [ ] `test_validar_semantica_detecta_tipo_vehiculo_invalido`
- [ ] `test_validar_integridad_referencial_detecta_planta_desconocida`
- [ ] `test_validar_integridad_referencial_detecta_codigo_incorrecto`
- [ ] `test_reporte_json_tiene_estructura_esperada`
- [ ] `test_guardar_procesados_no_crea_archivo_rechazados_si_lista_vacia`
