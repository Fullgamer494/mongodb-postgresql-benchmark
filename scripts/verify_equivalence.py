#!/usr/bin/env python3
"""Capture and compare Q1/Q2 results against the canonical dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient

from load_data import DEFAULT_CONFIG, DEFAULT_ENV, DEFAULT_INPUT, DEFAULT_SCHEMA, ImportErrorSafe, merged_environment
from validate_dataset import DatasetValidationError, validate_dataset

ROOT = Path(__file__).resolve().parents[1]
Q1_MONGO = ROOT / "queries/mongodb/q1.pipeline.json"
Q2_MONGO = ROOT / "queries/mongodb/q2.pipeline.json"
Q1_POSTGRES = ROOT / "queries/postgresql/q1.sql"
Q2_POSTGRES = ROOT / "queries/postgresql/q2.sql"
EXPECTED_MONGO_INDEXES = {"_id_", "event_id_idx", "timestamp_idx"}
EXPECTED_POSTGRES_INDEXES = {"events_pkey", "events_timestamp_idx"}
Q2_TIMESTAMP = "2026-01-01T00:00:00Z"
Q2_LIMIT = 1000
Q1_TOLERANCE = Decimal("0.000000001")


def decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ImportErrorSafe(f"Valor numérico de consulta inválido: {value!r}") from exc


def decimal_text(value: Any, *, quantize: bool = False) -> str:
    number = decimal(value)
    if quantize:
        number = number.quantize(Q1_TOLERANCE, rounding=ROUND_HALF_EVEN)
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def digest_json(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def read_jsonl_events(input_path: Path):
    with input_path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ImportErrorSafe(f"JSON inválido en línea {line_number}: {exc}") from exc


def source_q2_reference(input_path: Path) -> dict[str, Any]:
    histogram: Counter[int] = Counter()
    event_fields: dict[str, tuple[str, int]] = {}
    eligible_count = 0
    for event in read_jsonl_events(input_path):
        if event["incident"]["status"] != "confirmed" or event["timestamp"] < Q2_TIMESTAMP:
            continue
        event_id = event["event_id"]
        device_id = event["device"]["device_id"]
        risk_score = int(event["incident"]["risk_score"])
        eligible_count += 1
        histogram[risk_score] += 1
        event_fields[event_id] = (device_id, risk_score)

    above = 0
    cutoff: int | None = None
    at_cutoff = 0
    for score in sorted(histogram, reverse=True):
        if above + histogram[score] >= Q2_LIMIT:
            cutoff = score
            at_cutoff = histogram[score]
            break
        above += histogram[score]
    if cutoff is None:
        raise ImportErrorSafe(f"El dataset tiene menos de {Q2_LIMIT} eventos elegibles para Q2")
    expected_scores = [score for score in sorted(histogram, reverse=True) for _ in range(histogram[score])]
    return {
        "eligible_count": eligible_count,
        "risk_score_histogram": {str(score): histogram[score] for score in sorted(histogram, reverse=True)},
        "cutoff_risk_score": cutoff,
        "eligible_above_cutoff": above,
        "eligible_at_cutoff": at_cutoff,
        "slots_at_cutoff": Q2_LIMIT - above,
        "expected_top_risk_scores": expected_scores[:Q2_LIMIT],
        "eligible_event_fields": event_fields,
    }


def verify_q2_rows(rows: list[dict[str, Any]], reference: dict[str, Any], engine: str) -> dict[str, Any]:
    if len(rows) != Q2_LIMIT:
        raise ImportErrorSafe(f"{engine} Q2 devolvió {len(rows)} resultados; se esperaban {Q2_LIMIT}")
    ids = [row["event_id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ImportErrorSafe(f"{engine} Q2 devolvió event_id duplicados")
    scores = [int(decimal(row["risk_score"])) for row in rows]
    if scores != reference["expected_top_risk_scores"]:
        raise ImportErrorSafe(f"{engine} Q2 no coincide con la secuencia esperada de risk_score del JSONL")

    source_fields = reference["eligible_event_fields"]
    for row in rows:
        expected = source_fields.get(row["event_id"])
        observed = (row["device_id"], int(decimal(row["risk_score"])))
        if expected != observed:
            raise ImportErrorSafe(f"{engine} Q2 devolvió un evento fuera del filtro o con campos distintos")

    cutoff = reference["cutoff_risk_score"]
    above_ids = {row["event_id"] for row in rows if int(decimal(row["risk_score"])) > cutoff}
    source_above_ids = {
        event_id for event_id, (_, score) in source_fields.items() if score > cutoff
    }
    if above_ids != source_above_ids:
        raise ImportErrorSafe(f"{engine} Q2 no devolvió los mismos eventos con riesgo por encima del corte")
    at_cutoff_rows = [row for row in rows if int(decimal(row["risk_score"])) == cutoff]
    if len(at_cutoff_rows) != reference["slots_at_cutoff"]:
        raise ImportErrorSafe(f"{engine} Q2 seleccionó una cantidad inesperada de eventos empatados en el corte")
    return {
        "returned_count": len(rows),
        "returned_event_ids_unique": True,
        "risk_score_sequence_matches_dataset": True,
        "eligible_above_cutoff_ids_match_dataset": True,
        "cutoff_risk_score": cutoff,
        "eligible_above_cutoff": reference["eligible_above_cutoff"],
        "eligible_at_cutoff": reference["eligible_at_cutoff"],
        "selected_at_cutoff": len(at_cutoff_rows),
        "risk_score_sequence_sha256": digest_json(scores),
        "above_cutoff_event_ids_sha256": digest_json(sorted(above_ids)),
        "cutoff_event_ids_sha256": digest_json(sorted(row["event_id"] for row in at_cutoff_rows)),
    }


def capture_mongodb(input_path: Path, env: dict[str, str], reference: dict[str, Any]) -> dict[str, Any]:
    username = env.get("MONGO_INITDB_ROOT_USERNAME")
    password = env.get("MONGO_INITDB_ROOT_PASSWORD")
    if not username or not password:
        raise ImportErrorSafe("Faltan credenciales MongoDB en el archivo de entorno")
    client = MongoClient(
        host=env.get("MONGO_HOST", "127.0.0.1"),
        port=int(env.get("MONGO_HOST_PORT", "27017")),
        username=username,
        password=password,
        authSource="admin",
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        appname="u2-benchmark-equivalence-check",
    )
    try:
        client.admin.command("ping")
        database = client["security_benchmark"]
        collection = database["events"]
        count = collection.count_documents({})
        if count != 500000:
            raise ImportErrorSafe(f"MongoDB contiene {count} documentos; se esperan 500000")
        duplicate = next(collection.aggregate([
            {"$group": {"_id": "$event_id", "n": {"$sum": 1}}},
            {"$match": {"n": {"$gt": 1}}},
            {"$limit": 1},
        ], allowDiskUse=True), None)
        if duplicate:
            raise ImportErrorSafe(f"MongoDB contiene event_id duplicado: {duplicate['_id']}")
        indexes = [
            {"name": item["name"], "key": dict(item["key"]), "unique": bool(item.get("unique", False))}
            for item in collection.list_indexes()
        ]
        if {item["name"] for item in indexes} != EXPECTED_MONGO_INDEXES:
            raise ImportErrorSafe(f"Índices MongoDB inesperados: {sorted(item['name'] for item in indexes)}")

        q1_pipeline = json.loads(Q1_MONGO.read_text(encoding="utf-8"))
        q1_rows = []
        for row in collection.aggregate(q1_pipeline, allowDiskUse=False):
            q1_rows.append({
                "country": row["_id"]["country"],
                "category": row["_id"]["category"],
                "total_impact": decimal_text(row["total_impact"], quantize=True),
                "avg_impact": decimal_text(row["avg_impact"], quantize=True),
                "count": int(row["count"]),
            })
        q1_rows.sort(key=lambda row: (row["country"], row["category"]))

        q2_pipeline = json.loads(Q2_MONGO.read_text(encoding="utf-8"))
        q2_results: dict[str, Any] = {}
        for treatment, allow_disk_use in (("Q2", False), ("Q2_ALT", True)):
            q2_rows = []
            for row in collection.aggregate(q2_pipeline, allowDiskUse=allow_disk_use):
                q2_rows.append({
                    "event_id": row["event_id"],
                    "device_id": row["device"]["device_id"],
                    "risk_score": decimal_text(row["incident"]["risk_score"]),
                })
            q2_rows.sort(key=lambda row: (-int(decimal(row["risk_score"])), row["event_id"]))
            q2_results[treatment] = {
                "rows": q2_rows,
                "allowDiskUse": allow_disk_use,
                **verify_q2_rows(q2_rows, reference, f"MongoDB {treatment}"),
            }
        return {
            "engine": "mongodb",
            "server_version": client.server_info().get("version"),
            "database": "security_benchmark",
            "collection": "events",
            "document_count": count,
            "distinct_event_id_count": count,
            "indexes": indexes,
            "q1": {"result_count": len(q1_rows), "rows": q1_rows, "sha256": digest_json(q1_rows)},
            "q2": q2_results["Q2"],
            "q2_alt": q2_results["Q2_ALT"],
        }
    finally:
        client.close()


def capture_postgresql(env: dict[str, str], reference: dict[str, Any]) -> dict[str, Any]:
    username = env.get("POSTGRES_USER")
    password = env.get("POSTGRES_PASSWORD")
    if not username or not password:
        raise ImportErrorSafe("Faltan credenciales PostgreSQL en el archivo de entorno")
    params = {
        "host": env.get("POSTGRES_HOST", "127.0.0.1"),
        "port": int(env.get("POSTGRES_HOST_PORT", "5432")),
        "dbname": env.get("POSTGRES_DB", "security_benchmark"),
        "user": username,
        "password": password,
        "connect_timeout": 10,
        "application_name": "u2-benchmark-equivalence-check",
    }
    with psycopg.connect(**params, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) AS count, count(DISTINCT id) AS distinct_ids FROM events")
            counts = cursor.fetchone()
            count = int(counts["count"])
            distinct_ids = int(counts["distinct_ids"])
            if count != 500000 or distinct_ids != count:
                raise ImportErrorSafe(f"PostgreSQL count/IDs inesperados: {count}/{distinct_ids}, se esperan 500000/500000")
            cursor.execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname='public' AND tablename='events' ORDER BY indexname"
            )
            indexes = [{"name": row["indexname"], "definition": row["indexdef"]} for row in cursor.fetchall()]
            if {item["name"] for item in indexes} != EXPECTED_POSTGRES_INDEXES:
                raise ImportErrorSafe(f"Índices PostgreSQL inesperados: {sorted(item['name'] for item in indexes)}")
            cursor.execute("SELECT set_config('work_mem', %s, true) AS work_mem", ("4MB",))
            q1_set_value = cursor.fetchone()["work_mem"]
            cursor.execute("SHOW work_mem")
            q1_work_mem = cursor.fetchone()["work_mem"]
            if q1_set_value != "4MB" or q1_work_mem != "4MB":
                raise ImportErrorSafe(f"PostgreSQL Q1: work_mem esperado 4MB, observado {q1_work_mem}")
            cursor.execute(Q1_POSTGRES.read_text(encoding="utf-8"))
            q1_rows = []
            for row in cursor.fetchall():
                q1_rows.append({
                    "country": row["country"],
                    "category": row["category"],
                    "total_impact": decimal_text(row["total_impact"], quantize=True),
                    "avg_impact": decimal_text(row["avg_impact"], quantize=True),
                    "count": int(row["count"]),
                })
            q1_rows.sort(key=lambda row: (row["country"], row["category"]))
            q2_results: dict[str, Any] = {}
            for treatment, work_mem_value in (("Q2", "4MB"), ("Q2_ALT", "64MB")):
                cursor.execute("SELECT set_config('work_mem', %s, true) AS work_mem", (work_mem_value,))
                set_value = cursor.fetchone()["work_mem"]
                cursor.execute("SHOW work_mem")
                work_mem = cursor.fetchone()["work_mem"]
                if set_value != work_mem_value or work_mem != work_mem_value:
                    raise ImportErrorSafe(
                        f"PostgreSQL {treatment}: work_mem esperado {work_mem_value}, observado {work_mem}"
                    )
                cursor.execute(Q2_POSTGRES.read_text(encoding="utf-8"))
                q2_rows = [{
                    "event_id": row["event_id"],
                    "device_id": row["device_id"],
                    "risk_score": decimal_text(row["risk_score"]),
                } for row in cursor.fetchall()]
                q2_rows.sort(key=lambda row: (-int(decimal(row["risk_score"])), row["event_id"]))
                q2_results[treatment] = {
                    "rows": q2_rows,
                    "work_mem": work_mem,
                    **verify_q2_rows(q2_rows, reference, f"PostgreSQL {treatment}"),
                }
            cursor.execute("SELECT version() AS version")
            server_version = cursor.fetchone()["version"]
    return {
        "engine": "postgresql",
        "server_version": server_version,
        "database": params["dbname"],
        "table": "events",
        "row_count": count,
        "distinct_id_count": distinct_ids,
        "indexes": indexes,
        "q1_work_mem": q1_work_mem,
        "q1": {"result_count": len(q1_rows), "rows": q1_rows, "sha256": digest_json(q1_rows)},
        "q2": q2_results["Q2"],
        "q2_alt": q2_results["Q2_ALT"],
    }


def capture(args: argparse.Namespace) -> int:
    dataset = validate_dataset(args.input, args.config, args.schema)
    if dataset["count"] != 500000:
        raise ImportErrorSafe(f"Dataset no oficial: contiene {dataset['count']} documentos")
    reference = source_q2_reference(args.input)
    env = merged_environment(args.env_file)
    report = {
        "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset": {"count": dataset["count"], "sha256": dataset["sha256"]},
        "queries": {"q1": str((Q1_MONGO if args.engine == "mongodb" else Q1_POSTGRES).relative_to(ROOT)),
                    "q2": str((Q2_MONGO if args.engine == "mongodb" else Q2_POSTGRES).relative_to(ROOT))},
        "q2_reference": {key: value for key, value in reference.items() if key != "eligible_event_fields" and key != "expected_top_risk_scores"},
        "capture": capture_mongodb(args.input, env, reference) if args.engine == "mongodb"
                   else capture_postgresql(env, reference),
    }
    write_json(args.output, report)
    print(json.dumps({"engine": args.engine, "output": str(args.output), "dataset_sha256": dataset["sha256"],
                      "q1_groups": report["capture"]["q1"]["result_count"],
                      "q1_sha256": report["capture"]["q1"]["sha256"],
                      "q2_cutoff": report["capture"]["q2"]["cutoff_risk_score"],
                      "q2_score_sha256": report["capture"]["q2"]["risk_score_sequence_sha256"]},
                     ensure_ascii=False, sort_keys=True))
    return 0


def write_json(path: Path, value: dict[str, Any]) -> None:
    output = path if path.is_absolute() else ROOT / path
    if output.exists():
        raise ImportErrorSafe(f"No se sobrescribe evidencia existente: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def compare(args: argparse.Namespace) -> int:
    mongo = json.loads(args.mongodb.read_text(encoding="utf-8"))["capture"]
    postgres = json.loads(args.postgresql.read_text(encoding="utf-8"))["capture"]
    if mongo["engine"] != "mongodb" or postgres["engine"] != "postgresql":
        raise ImportErrorSafe("Los archivos de captura no corresponden a MongoDB y PostgreSQL respectivamente")
    for key in ("dataset",):
        mongo_dataset = json.loads(args.mongodb.read_text(encoding="utf-8"))[key]
        postgres_dataset = json.loads(args.postgresql.read_text(encoding="utf-8"))[key]
        if mongo_dataset != postgres_dataset:
            raise ImportErrorSafe("Las capturas no usan el mismo dataset oficial")
    q1_equal = mongo["q1"]["rows"] == postgres["q1"]["rows"]
    if not q1_equal:
        raise ImportErrorSafe("Q1 difiere entre motores después de normalizar promedios a 1e-9")
    for treatment in ("q2", "q2_alt"):
        q2_checks = (
            mongo[treatment]["risk_score_sequence_sha256"] == postgres[treatment]["risk_score_sequence_sha256"]
            and mongo[treatment]["above_cutoff_event_ids_sha256"] == postgres[treatment]["above_cutoff_event_ids_sha256"]
            and mongo[treatment]["cutoff_risk_score"] == postgres[treatment]["cutoff_risk_score"]
            and mongo[treatment]["selected_at_cutoff"] == postgres[treatment]["selected_at_cutoff"]
        )
        if not q2_checks:
            raise ImportErrorSafe(f"{treatment.upper()} difiere en puntajes, IDs por encima del corte o elegibilidad de empates")
    report = {
        "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset": json.loads(args.mongodb.read_text(encoding="utf-8"))["dataset"],
        "engines": {"mongodb": mongo["server_version"], "postgresql": postgres["server_version"]},
        "equivalence": {
            "q1": {"ok": True, "groups": mongo["q1"]["result_count"],
                   "sha256": mongo["q1"]["sha256"], "average_numeric_tolerance": "1e-9"},
            "q2": {
                "ok": True,
                "limit": Q2_LIMIT,
                "risk_score_sequence_sha256": mongo["q2"]["risk_score_sequence_sha256"],
                "cutoff_risk_score": mongo["q2"]["cutoff_risk_score"],
                "eligible_above_cutoff": mongo["q2"]["eligible_above_cutoff"],
                "eligible_at_cutoff": mongo["q2"]["eligible_at_cutoff"],
                "selected_at_cutoff": mongo["q2"]["selected_at_cutoff"],
                "above_cutoff_event_ids_sha256": mongo["q2"]["above_cutoff_event_ids_sha256"],
                "cutoff_tie_ids_may_differ": True,
            },
            "q2_alt": {
                "ok": True,
                "treatment": {"mongodb": "allowDiskUse=true", "postgresql": "work_mem=64MB"},
                "limit": Q2_LIMIT,
                "risk_score_sequence_sha256": mongo["q2_alt"]["risk_score_sequence_sha256"],
                "cutoff_risk_score": mongo["q2_alt"]["cutoff_risk_score"],
                "eligible_above_cutoff": mongo["q2_alt"]["eligible_above_cutoff"],
                "eligible_at_cutoff": mongo["q2_alt"]["eligible_at_cutoff"],
                "selected_at_cutoff": mongo["q2_alt"]["selected_at_cutoff"],
                "above_cutoff_event_ids_sha256": mongo["q2_alt"]["above_cutoff_event_ids_sha256"],
                "cutoff_tie_ids_may_differ": True,
            },
        },
        "import_invariants": {
            "mongodb_document_count": mongo["document_count"],
            "mongodb_distinct_event_id_count": mongo["distinct_event_id_count"],
            "mongodb_indexes": mongo["indexes"],
            "postgresql_row_count": postgres["row_count"],
            "postgresql_distinct_id_count": postgres["distinct_id_count"],
            "postgresql_indexes": postgres["indexes"],
        },
    }
    write_json(args.output, report)
    print(json.dumps(report["equivalence"], ensure_ascii=False, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture_parser = subparsers.add_parser("capture", help="Run correctness queries on one active engine")
    capture_parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    capture_parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    capture_parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    capture_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    capture_parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    capture_parser.add_argument("--output", type=Path, required=True)
    compare_parser = subparsers.add_parser("compare", help="Compare two saved, sequential engine captures")
    compare_parser.add_argument("--mongodb", type=Path, required=True)
    compare_parser.add_argument("--postgresql", type=Path, required=True)
    compare_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        return capture(args) if args.command == "capture" else compare(args)
    except (DatasetValidationError, ImportErrorSafe, OSError, ValueError, psycopg.Error) as exc:
        print(f"Verificación de equivalencia detenida: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
