# Interfaz objetivo y estado de implementación

Los comandos siguientes describen el contrato del kit. Importación, equivalencia, planes, runner de celda, smoke por motor, automatización serial, checkpoint y validador global están implementados. Los pilotos de fase 08 se conservan en `results/pilot/`; la campaña oficial extendida sigue pendiente de fase 09.

```bash
python scripts/preflight.py --output docs/entorno.json
python scripts/synthetic_data_generator.py --count 500000 --seed 20261101 --output data/generated/events.jsonl
python scripts/validate_dataset.py --input data/generated/events.jsonl
python scripts/load_data.py --engine mongodb --input data/generated/events.jsonl
python scripts/load_data.py --engine postgresql --input data/generated/events.jsonl
.venv/bin/python scripts/verify_equivalence.py capture --engine mongodb --input data/generated/events.jsonl --output results/validation/phase03-profile-a/mongodb.json
.venv/bin/python scripts/verify_equivalence.py capture --engine postgresql --input data/generated/events.jsonl --output results/validation/phase03-profile-a/postgresql.json
.venv/bin/python scripts/verify_equivalence.py compare --mongodb results/validation/phase03-profile-a/mongodb.json --postgresql results/validation/phase03-profile-a/postgresql.json --output results/validation/phase03-profile-a/comparison.json
.venv/bin/python scripts/create_query_fixture.py
.venv/bin/python scripts/verify_query_fixture.py --engine mongodb --output results/validation/phase04-profile-a/mongodb-fixture.json
.venv/bin/python scripts/verify_query_fixture.py --engine postgresql --output results/validation/phase04-profile-a/postgresql-fixture.json
.venv/bin/python scripts/collect_plans.py --engine mongodb --query Q1 --output results/plans/phase04-profile-a/mongodb-q1-fixed-current.json
.venv/bin/python scripts/collect_plans.py --engine mongodb --query Q2 --treatment Q2 --output results/plans/phase04-profile-a/mongodb-q2-base-current.json
.venv/bin/python scripts/collect_plans.py --engine mongodb --query Q2 --treatment Q2_ALT --output results/plans/phase04-profile-a/mongodb-q2-alt-current.json
.venv/bin/python scripts/collect_plans.py --engine postgresql --query Q1 --output results/plans/phase04-profile-a/postgresql-q1-fixed-retry.json
.venv/bin/python scripts/collect_plans.py --engine postgresql --query Q2 --treatment Q2 --output results/plans/phase04-profile-a/postgresql-q2-base.json
.venv/bin/python scripts/collect_plans.py --engine postgresql --query Q2 --treatment Q2_ALT --output results/plans/phase04-profile-a/postgresql-q2-alt.json
.venv/bin/python -m benchmark_runner --help
.venv/bin/python -m benchmark_runner --engine mongodb --profile PROFILE_A_BASELINE --query Q1 --concurrency 10 --run-number 1 --mode smoke --dry-run
python -m benchmark_runner --engine mongodb --profile PROFILE_A_BASELINE --query Q1 --concurrency 1 --run-number 1 --mode smoke
python -m benchmark_runner --engine postgresql --profile PROFILE_A_BASELINE --query Q1 --concurrency 1 --run-number 1 --mode smoke
./run_benchmarks.sh --mode extended
./run_benchmarks.sh --mode extended --execute
./run_benchmarks.sh --mode extended --execute --resume
.venv/bin/python scripts/validate_results.py --root results/official --plan results/campaigns/extended-fbfcde4c3425.plan.json --output results/campaigns/extended-validation.json
```

Mantén activo un solo motor entre corridas. El dry-run de campaña no abre conexiones ni inicia servicios; imprime el plan completo. Los smoke van separados bajo `results/smoke/`; los pilotos bajo `results/pilot/`; resultados de matriz bajo `results/official/`.

`--mode smoke` usa 2/60/2 y nunca alimenta datos oficiales. Piloto/official conservan dataset 500k, 60/300/60, perfiles reales y tratamiento fijado. `run_benchmarks.sh` exige `--execute`; `--resume` salta resultados válidos y permite un único reintento para error de infraestructura, nunca repite éxitos. El validador reconoce estados experimentales `oom`/`spill` y conserva `error` para auditoría.
