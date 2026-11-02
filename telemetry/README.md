# Telemetría y evidencia

`collector.py` consulta Prometheus local únicamente (`127.0.0.1`, `localhost` o `::1`) por un rango epoch y guarda consultas PromQL, series crudas, etiquetas, ventana, cobertura, valores iniciales/finales, RSS máximo y deltas de contadores. Detecta resets y cambios en `container_start_time_seconds`; con restart, reset o menos de dos muestras, el delta es `null` y la calidad es error, nunca cero inventado. La selección del motor exige simultáneamente el ID Docker exacto y la etiqueta Compose del servicio. RSS usa `container_memory_rss`; no se sustituye por working set ni memoria total. Métricas requeridas ausentes o con cobertura insuficiente dejan `status=error`.

```bash
.venv/bin/python telemetry/collector.py \
  --run-id RUN_ID --engine mongodb --container-id CONTAINER_ID \
  --start START_EPOCH --end END_EPOCH --step 1 \
  --output results/telemetry/CAMPAIGN/RUN_ID/metrics.json
```

`evidence.py` captura `memory.events` al inicio y al final, identidad del contenedor, estado Docker, eventos OOM y logs del motor. Clasifica OOM como positivo con evento o incremento `oom`/`oom_kill`, negativo comprobado solo con la misma identidad, contadores disponibles sin incremento y `OOMKilled=false`; otros casos son desconocidos. MongoDB clasifica spill desde `usedDisk` en logs estructurados. Un aggregate completado con `allowDiskUse:false` es evidencia negativa porque ese comando prohíbe spill. Para `allowDiskUse:true`, la ausencia del indicador solo se considera negativa cuando el log está completo, cada consulta exitosa de la ventana tiene un `Slow query` etiquetado con su `run_id`, y ninguna entrada incluye `usedDisk:true`; cobertura parcial queda `unknown`. MongoDB documenta que el slow-query log incluye `usedDisk` si alguna etapa escribió temporales ([aggregate](https://www.mongodb.com/docs/manual/reference/method/db.collection.aggregate/), [log messages](https://www.mongodb.com/docs/manual/reference/log-messages/)). PostgreSQL usa `application_name` en `log_line_prefix` y `log_temp_files=0`; un archivo temporal atribuido al `run_id` demuestra spill. No se infiere spill desde EXPLAIN ni desde una opción habilitada.

Para el runner, PostgreSQL también puede probar spill negativo si los logs completos cubren la ventana medida, el ajuste efectivo `log_temp_files=0` está capturado y hubo consultas medidas completas. Ese ajuste registra cualquier archivo temporal aunque una consulta rápida no produzca una línea de sentencia `application_name`. Si falta una de esas condiciones, el resultado permanece desconocido.

`smoke_query.py` ejecuta una sola Q1 secuencial, consume todas sus filas y conserva el resultado diagnóstico, snapshot, logs Docker, eventos, series y resúmenes. No alimenta el CSV ni constituye una medición de rendimiento.

```bash
.venv/bin/python telemetry/smoke_query.py --engine mongodb \
  --container u2-benchmark-mongodb-1 \
  --output results/telemetry/CAMPAIGN/RUN_ID
```

Pruebas locales de clasificación y calidad (fixtures, no resultados del motor):

```bash
.venv/bin/python -m unittest tests.test_telemetry_evidence -v
```

La configuración fija el scrape y housekeeping en 1 s; Prometheus retiene 14 días. cAdvisor observa solo contenedores Docker y expone únicamente labels permitidos. Los puertos publicados se ligan a loopback. cAdvisor usa modo privilegiado y montajes de lectura del host necesarios para cgroups v2; Node Exporter ve el PID/árbol raíz del host para CPU, disco, swap y presión. Estos exportadores carecen de cuotas explícitas de motor (`NanoCpus=0`, memoria ilimitada en la inspección); permanecen fuera del cgroup de los motores limitados. Esta instrumentación y el logging quedan fijos durante las corridas porque el scrape de 1 s, el housekeeping y los logs pueden añadir costo.

Evidencia real de fase 05: `results/telemetry/phase05/`. La primera ejecución Mongo conservó `spill=unknown` porque el log de esa versión omitió `usedDisk`; la repetición confirmó Q1 completa con `allowDiskUse:false` y se clasificó como negativo comprobado. La ejecución PostgreSQL registró un archivo temporal etiquetado por `run_id`. Ambos motores tuvieron OOM negativo comprobado y telemetría requerida con cobertura completa. Son observaciones diagnósticas, no resultados oficiales.
