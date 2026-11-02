#!/usr/bin/env python3
"""Execute Q1 and both Q2 treatments against an isolated known-results fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pymongo import ASCENDING, MongoClient, ReplaceOne
from jsonschema import Draft202012Validator, FormatChecker

from load_data import DEFAULT_ENV, ImportErrorSafe, merged_environment

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = ROOT / "data/development/phase04_query_fixture.jsonl"
Q1_MONGO = ROOT / "queries/mongodb/q1.pipeline.json"
Q2_MONGO = ROOT / "queries/mongodb/q2.pipeline.json"
Q1_POSTGRES = ROOT / "queries/postgresql/q1.sql"
Q2_POSTGRES = ROOT / "queries/postgresql/q2.sql"
SCHEMA = ROOT / "contracts/event.schema.json"
TOLERANCE = Decimal("0.000000001")
LIMIT = 1000


def number(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ImportErrorSafe(f"Número de consulta inválido: {value!r}") from exc


def number_text(value: Any) -> str:
    number_value = number(value).quantize(TOLERANCE, rounding=ROUND_HALF_EVEN)
    text = format(number_value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def load_fixture(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    expected_path = path.with_suffix(".expected.json")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected["fixture_sha256"]:
        raise ImportErrorSafe("El SHA-256 del fixture no coincide con su sidecar esperado")
    events = [json.loads(line) for line in raw.decode("utf-8").splitlines()]
    if len(events) != expected["fixture_count"] or len(events) != 1004:
        raise ImportErrorSafe(f"Fixture con {len(events)} filas; se esperaban {expected['fixture_count']}")
    validator = Draft202012Validator(json.loads(SCHEMA.read_text(encoding="utf-8")), format_checker=FormatChecker())
    ids: set[str] = set()
    for number_line, event in enumerate(events, start=1):
        errors = list(validator.iter_errors(event))
        if errors:
            raise ImportErrorSafe(f"Fixture inválido en línea {number_line}: {errors[0].message}")
        if event["event_id"] in ids:
            raise ImportErrorSafe(f"event_id duplicado en fixture: {event['event_id']}")
        ids.add(event["event_id"])
    return events, expected, digest


def normalize_q1(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [{
        "country": row["country"],
        "category": row["category"],
        "total_impact": number_text(row["total_impact"]),
        "avg_impact": number_text(row["avg_impact"]),
        "count": int(row["count"]),
    } for row in rows]
    return sorted(normalized, key=lambda row: (row["country"], row["category"]))


def validate_q1(rows: list[dict[str, Any]], expected: dict[str, Any], engine: str) -> None:
    if rows != expected["q1"]:
        raise ImportErrorSafe(f"{engine} Q1 no coincide con los resultados conocidos: {rows!r}")


def validate_q2(rows: list[dict[str, Any]], events: list[dict[str, Any]], expected: dict[str, Any], engine: str) -> dict[str, Any]:
    if len(rows) != LIMIT:
        raise ImportErrorSafe(f"{engine} Q2 devolvió {len(rows)} filas; se esperaban {LIMIT}")
    eligible = {
        event["event_id"]: event for event in events
        if event["incident"]["status"] == "confirmed" and event["timestamp"] >= "2026-01-01T00:00:00Z"
    }
    cutoff = int(expected["q2"]["cutoff_risk_score"])
    above_expected = {
        event_id for event_id, event in eligible.items()
        if int(event["incident"]["risk_score"]) > cutoff
    }
    at_cutoff_expected = {
        event_id for event_id, event in eligible.items()
        if int(event["incident"]["risk_score"]) == cutoff
    }
    ids = [row["event_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise ImportErrorSafe(f"{engine} Q2 devolvió event_id repetidos")
    observed_scores = [int(number(row["risk_score"])) for row in rows]
    expected_scores = [100, 90] + [80] * 998
    if observed_scores != expected_scores:
        raise ImportErrorSafe(f"{engine} Q2 puntajes no coinciden con el Top-K esperado")
    for row in rows:
        event = eligible.get(row["event_id"])
        if event is None or event["device"]["device_id"] != row["device_id"] or int(event["incident"]["risk_score"]) != int(number(row["risk_score"])):
            raise ImportErrorSafe(f"{engine} Q2 devolvió un resultado fuera del filtro o con campos distintos")
    selected_above = {row["event_id"] for row in rows if int(number(row["risk_score"])) > cutoff}
    selected_cutoff = {row["event_id"] for row in rows if int(number(row["risk_score"])) == cutoff}
    if selected_above != above_expected or len(selected_cutoff) != int(expected["q2"]["selected_at_cutoff"]):
        raise ImportErrorSafe(f"{engine} Q2 no coincide con elegibilidad/empates del corte")
    if not selected_cutoff.issubset(at_cutoff_expected):
        raise ImportErrorSafe(f"{engine} Q2 eligió un event_id que no pertenece al empate válido")
    return {
        "result_count": len(rows),
        "scores_match_known_top_k": True,
        "eligible_ids_match_filters": True,
        "cutoff_risk_score": cutoff,
        "eligible_above_cutoff": len(above_expected),
        "eligible_at_cutoff": len(at_cutoff_expected),
        "selected_at_cutoff": len(selected_cutoff),
        "cutoff_tie_ids_may_differ": True,
    }


def q2_mongo_rows(collection, allow_disk_use: bool) -> list[dict[str, Any]]:
    pipeline = json.loads(Q2_MONGO.read_text(encoding="utf-8"))
    rows = []
    for row in collection.aggregate(pipeline, allowDiskUse=allow_disk_use):
        rows.append({
            "event_id": row["event_id"],
            "device_id": row["device"]["device_id"],
            "risk_score": number_text(row["incident"]["risk_score"]),
        })
    rows.sort(key=lambda row: (-int(number(row["risk_score"])), row["event_id"]))
    return rows


def capture_mongodb(events: list[dict[str, Any]], expected: dict[str, Any], digest: str, env: dict[str, str]) -> dict[str, Any]:
    username, password = env.get("MONGO_INITDB_ROOT_USERNAME"), env.get("MONGO_INITDB_ROOT_PASSWORD")
    if not username or not password:
        raise ImportErrorSafe("Faltan credenciales MongoDB")
    client = MongoClient(
        host=env.get("MONGO_HOST", "127.0.0.1"),
        port=int(env.get("MONGO_HOST_PORT", "27017")),
        username=username,
        password=password,
        authSource="admin",
        serverSelectionTimeoutMS=10000,
        appname="u2-query-fixture-validation",
    )
    try:
        client.admin.command("ping")
        database = client["security_benchmark_dev"]
        collection = database["events"]
        collection.create_index([("event_id", ASCENDING)], name="event_id_idx")
        collection.create_index([("timestamp", ASCENDING)], name="timestamp_idx")
        collection.bulk_write([ReplaceOne({"event_id": event["event_id"]}, event, upsert=True) for event in events], ordered=False)
        count = collection.count_documents({})
        if count != len(events):
            raise ImportErrorSafe(f"MongoDB fixture contiene {count} docs; se esperaban {len(events)}; no se borraron sobrantes")
        duplicate = next(collection.aggregate([
            {"$group": {"_id": "$event_id", "n": {"$sum": 1}}},
            {"$match": {"n": {"$gt": 1}}},
            {"$limit": 1},
        ], allowDiskUse=True), None)
        if duplicate:
            raise ImportErrorSafe(f"Fixture MongoDB tiene event_id repetido: {duplicate['_id']}")
        q1_pipeline = json.loads(Q1_MONGO.read_text(encoding="utf-8"))
        q1 = normalize_q1([{
            "country": row["_id"]["country"],
            "category": row["_id"]["category"],
            "total_impact": row["total_impact"],
            "avg_impact": row["avg_impact"],
            "count": row["count"],
        } for row in collection.aggregate(q1_pipeline, allowDiskUse=False)])
        validate_q1(q1, expected, "MongoDB")
        q2_results = {}
        for treatment, allow_disk_use in (("Q2", False), ("Q2_ALT", True)):
            rows = q2_mongo_rows(collection, allow_disk_use)
            q2_results[treatment] = {
                "allowDiskUse": allow_disk_use,
                **validate_q2(rows, events, expected, f"MongoDB {treatment}"),
            }
        return {
            "engine": "mongodb",
            "server_version": client.server_info()["version"],
            "database": "security_benchmark_dev",
            "collection": "events",
            "document_count": count,
            "indexes": [item["name"] for item in collection.list_indexes()],
            "q1": {"rows": q1, "matches_known_results": True},
            "q2": q2_results,
            "fixture_sha256": digest,
        }
    finally:
        client.close()


def q2_postgres_rows(cursor) -> list[dict[str, Any]]:
    cursor.execute(Q2_POSTGRES.read_text(encoding="utf-8"))
    rows = [{
        "event_id": row["event_id"],
        "device_id": row["device_id"],
        "risk_score": number_text(row["risk_score"]),
    } for row in cursor.fetchall()]
    rows.sort(key=lambda row: (-int(number(row["risk_score"])), row["event_id"]))
    return rows


def capture_postgresql(events: list[dict[str, Any]], expected: dict[str, Any], digest: str, env: dict[str, str]) -> dict[str, Any]:
    username, password = env.get("POSTGRES_USER"), env.get("POSTGRES_PASSWORD")
    if not username or not password:
        raise ImportErrorSafe("Faltan credenciales PostgreSQL")
    params = {
        "host": env.get("POSTGRES_HOST", "127.0.0.1"),
        "port": int(env.get("POSTGRES_HOST_PORT", "5432")),
        "dbname": env.get("POSTGRES_DB", "security_benchmark"),
        "user": username,
        "password": password,
        "connect_timeout": 10,
        "application_name": "u2-query-fixture-validation",
    }
    with psycopg.connect(**params, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE TEMP TABLE events (id UUID PRIMARY KEY, data JSONB NOT NULL)")
            with cursor.copy("COPY pg_temp.events (id, data) FROM STDIN") as copy:
                for event in events:
                    copy.write_row((event["event_id"], Jsonb(event)))
            cursor.execute("SET LOCAL work_mem = '4MB'")
            cursor.execute("SHOW work_mem")
            q1_work_mem = cursor.fetchone()["work_mem"]
            cursor.execute(Q1_POSTGRES.read_text(encoding="utf-8"))
            q1 = normalize_q1(cursor.fetchall())
            validate_q1(q1, expected, "PostgreSQL")
            q2_results = {}
            for treatment, work_mem in (("Q2", "4MB"), ("Q2_ALT", "64MB")):
                cursor.execute("SELECT set_config('work_mem', %s, true) AS work_mem", (work_mem,))
                observed = cursor.fetchone()["work_mem"]
                cursor.execute("SHOW work_mem")
                shown = cursor.fetchone()["work_mem"]
                if observed != work_mem or shown != work_mem:
                    raise ImportErrorSafe(f"PostgreSQL {treatment} work_mem: se esperaba {work_mem}, observado {shown}")
                rows = q2_postgres_rows(cursor)
                q2_results[treatment] = {
                    "work_mem": shown,
                    **validate_q2(rows, events, expected, f"PostgreSQL {treatment}"),
                }
        return {
            "engine": "postgresql",
            "database": params["dbname"],
            "scope": "temporary table events; removed automatically when connection closes",
            "row_count": len(events),
            "q1_work_mem": q1_work_mem,
            "q1": {"rows": q1, "matches_known_results": True},
            "q2": q2_results,
            "fixture_sha256": digest,
        }


def write_json(path: Path, value: dict[str, Any]) -> None:
    output = path if path.is_absolute() else ROOT / path
    if output.exists():
        raise ImportErrorSafe(f"No se sobrescribe evidencia existente: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixture_path = args.fixture if args.fixture.is_absolute() else ROOT / args.fixture
    env_path = args.env_file if args.env_file.is_absolute() else ROOT / args.env_file
    try:
        events, expected, digest = load_fixture(fixture_path)
        env = merged_environment(env_path)
        captured = capture_mongodb(events, expected, digest, env) if args.engine == "mongodb" else capture_postgresql(events, expected, digest, env)
        report = {
            "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "fixture": str(fixture_path.relative_to(ROOT)),
            "expected_results": expected,
            "capture": captured,
            "status": "ok",
        }
        write_json(args.output, report)
        print(json.dumps({"engine": args.engine, "status": "ok", "fixture_sha256": digest,
                          "q1_groups": len(captured["q1"]["rows"]),
                          "q2_treatments": list(captured["q2"])}, ensure_ascii=False, sort_keys=True))
        return 0
    except (ImportErrorSafe, OSError, ValueError, psycopg.Error) as exc:
        print(f"Validación fixture detenida: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
