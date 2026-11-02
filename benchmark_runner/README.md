# Runner concurrente

Paquete ejecutable con `python -m benchmark_runner`. Opera una celda y un solo motor; no inicia/detiene Compose ni el otro motor. Antes de abrir conexiones revisa los contenedores activos del proyecto y ejecuta el preflight del engine/profile seleccionado. Cada worker abre su conexión nativa antes de la barrera; hay como máximo una consulta activa por cliente. Q1/Q2 consumen por completo los cursores/respuestas. Mongo incluye `comment=run_id`, `allowDiskUse` por operación y `maxTimeMS=120000`. PostgreSQL usa una conexión por cliente, `application_name=run_id`, aplica y comprueba `work_mem` por sesión y conserva `statement_timeout=120s`.

```bash
.venv/bin/python -m benchmark_runner --help
.venv/bin/python -m benchmark_runner --engine mongodb --profile PROFILE_A_BASELINE --query Q1 --concurrency 10 --run-number 1 --mode smoke --dry-run
```

`--dry-run` comprueba argumentos/consultas/tratamiento y presenta conexiones y tiempos sin tocar Docker, motores o resultados. `smoke` usa 2 s de warmup, 60 s de medición y 2 s cooldown en `results/smoke/`; se amplió de 8 a 60 s porque Q1 concurrente tardó más que la ventana inicial y no completaba solicitudes medidas. `pilot`/`official` usan 60/300/60. Cada intento crea un directorio exclusivo con `manifest.json`, `summary.csv` (cabecera fija), `requests.jsonl`, `telemetry/metrics.json`, `engine_logs/`, `host_events/`, `quality.json` y `auxiliary.json`. No se sobrescriben resultados. El máximo de drenaje es 120 s y el cooldown empieza después del drenaje.

El CSV cuenta solo operaciones completas que terminen dentro de la ventana monotónica. QPS divide esos éxitos por 300 s efectivos; latencias nearest-rank usan el mismo conjunto. Si la ventana fue válida y no tuvo éxitos, QPS es cero y latencias quedan vacías. Falta de telemetría deja RSS/throttling vacíos y pone status `error`; no rellena con cero. La precedencia es `oom > error > spill > ok`. Spill y OOM requieren evidencia; un estado desconocido impide `ok`. Fallos y consultas incompletas permanecen en JSONL/quality. CPU e inputs de Q1/Q2 estimados desde planes fase 04 quedan en el sidecar M3, sin modificar el CSV.

El JSONL se amortiza en memoria y se escribe al finalizar la ventana; esta elección reduce I/O síncrono por operación y su costo escala con el total de solicitudes. Manifest documenta ese efecto, el plan auxiliar, versiones, límites y `cache_protocol=alternative_no_host_drop_caches`. El runner no llama `drop_caches`, no cambia sysctl y no altera volúmenes.

Las pruebas usan resultados deterministas y drivers falsos para percentiles, límites de ventana, precedencia, cabecera CSV, tratamientos Q2 y concurrencia simultánea de clientes. Evidencia smoke por motor y los cuatro pilotos del perfil A están en `results/smoke/` y `results/pilot/`; véase `docs/auditoria-piloto.md`. La matriz larga sigue reservada para fase 09.
