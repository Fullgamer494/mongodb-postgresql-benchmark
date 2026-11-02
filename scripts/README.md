# Componentes implementados

- `synthetic_data_generator.py`: genera JSONL canónico determinista por streaming y un manifiesto sidecar con SHA-256, código/config/esquema, conteo, bytes, rangos y distribuciones. Rehúsa reemplazar salidas discordantes salvo `--allow-overwrite` explícito.
- `validate_dataset.py`: valida esquema y formatos, UUID/fecha reales, unicidad, rangos, distribuciones y consistencia del manifiesto; recorre el JSONL por streaming.

Comandos desde la raíz del proyecto:

```bash
.venv/bin/python scripts/synthetic_data_generator.py --count 500000 --seed 20261101 --output data/generated/events.jsonl
.venv/bin/python scripts/validate_dataset.py --input data/generated/events.jsonl
```

Importación, preflight, equivalencia, extracción de planes, orquestación de campañas y validación de resultados están implementados. La matriz extendida no se inicia sin `--execute` y sigue reservada para el prompt 09.

## Fase 03

`select_resource_profile.py` escribe `.env.resources` para el perfil elegido; el archivo es local e ignorado por Git. `preflight.py` comprueba Docker/cgroups, versión de kernel y, para PostgreSQL, readiness, etiquetas, límites Docker y cgroup. Para MongoDB, el gate del kernel se evalúa antes de inspeccionar o iniciar el servicio.

`load_data.py` valida primero el JSONL y lo carga por lotes en el motor elegido, sin reset destructivo. En Mongo crea `event_id_idx` antes del upsert idempotente para evitar escaneos de colección; `timestamp_idx` se crea después de cargar. Ejemplo ya ejecutado para PostgreSQL:

```bash
.venv/bin/python scripts/select_resource_profile.py --profile PROFILE_A_BASELINE
docker compose --env-file .env --env-file .env.resources config --quiet
docker compose --env-file .env --env-file .env.resources up -d --no-deps --wait postgresql
.venv/bin/python scripts/preflight.py --engine postgresql --profile PROFILE_A_BASELINE --output docs/preflight-postgresql-profile-a-final.json
.venv/bin/python scripts/load_data.py --engine postgresql --input data/generated/events.jsonl --env-file .env --batch-size 2000
```

`verify_equivalence.py capture` valida el dataset, IDs, índices, filtros y resultados Q1/Q2 en un motor activo. Detenga ese servicio antes de iniciar el otro; luego compare las capturas con `verify_equivalence.py compare`. Q1 normaliza promedios con tolerancia 1e-9; Q2 valida puntajes y elegibilidad de empates sin exigir UUID idénticos en el corte. Son consultas de corrección, no mediciones de rendimiento. Evidencia de fase 03 en `results/validation/phase03-profile-a/`.

`create_query_fixture.py` produce resultados conocidos de desarrollo; `verify_query_fixture.py` lo carga en una base MongoDB separada o una tabla temporal PostgreSQL. `collect_plans.py` guarda un plan por motor/consulta/tratamiento y conserva errores. `executionStats` y `EXPLAIN ANALYZE` ejecutan las consultas y se usan fuera de mediciones oficiales. Evidencia fase 04: `results/validation/phase04-profile-a/` y `results/plans/phase04-profile-a/`.

```bash
.venv/bin/python scripts/create_query_fixture.py
.venv/bin/python scripts/verify_query_fixture.py --engine mongodb --output results/validation/phase04-profile-a/mongodb-fixture.json
.venv/bin/python scripts/verify_query_fixture.py --engine postgresql --output results/validation/phase04-profile-a/postgresql-fixture.json
.venv/bin/python scripts/collect_plans.py --engine mongodb --query Q2 --treatment Q2_ALT --output results/plans/phase04-profile-a/mongodb-q2-alt-current.json
.venv/bin/python scripts/collect_plans.py --engine postgresql --query Q2 --treatment Q2_ALT --output results/plans/phase04-profile-a/postgresql-q2-alt.json
```

El preflight de MongoDB evalúa kernel, distribución Ubuntu y versión de la imagen fijada. Permite Ubuntu 24.04+ con MongoDB 9.0.4+ según SERVER-131779/SERVER-125742 y conserva el bloqueo en el intervalo afectado para combinaciones sin esa excepción. `--host-only` comprueba compatibilidad y Docker antes de iniciar un servicio; el modo normal valida también contenedor, salud, pin y límites efectivos. Para operar un único motor use el servicio explícito y `--no-deps`; no ejecute `up` sin nombre de servicio. Evidencia A/B/C en `docs/preflight-mongodb-profile-*.json`.

## Runner y campañas

`python -m benchmark_runner` ejecuta una sola celda. `./run_benchmarks.sh --mode base|extended` produce un dry-run determinista; solo `--execute` inicia la campaña. `--resume` lee `results/campaigns/<campaign_id>.checkpoint.json`, no repite resultados `ok`/`spill`/`oom` y limita a uno el reintento de fallos de infraestructura. Un error del motor queda para revisión.

`scripts/validate_results.py --root ... --plan ...` contrasta CSV, manifiestos, quality, sidecars, unicidad y celdas esperadas. `scripts/drop_caches.py` solo informa en modo predeterminado; `--apply` requiere ejecutarse explícitamente como root. No se llama desde el runner ni la campaña.
