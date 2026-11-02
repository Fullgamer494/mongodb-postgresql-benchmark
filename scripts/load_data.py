#!/usr/bin/env python3
"""Idempotently load the canonical JSONL into one explicitly selected engine."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus

import psycopg
from psycopg.types.json import Jsonb
from pymongo import ASCENDING, MongoClient, ReplaceOne

from validate_dataset import DatasetValidationError, validate_dataset

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data/generated/events.jsonl"
DEFAULT_ENV = ROOT / ".env"
DEFAULT_CONFIG = ROOT / "config/dataset.json"
DEFAULT_SCHEMA = ROOT / "contracts/event.schema.json"


class ImportErrorSafe(Exception):
    """Raised when import preconditions or post-load verification fail."""


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ImportErrorSafe(f"No se puede leer el archivo de entorno {path}: {exc}") from exc
    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise ImportErrorSafe(f"Línea {line_number} inválida en {path}; se esperaba CLAVE=valor")
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not key:
            raise ImportErrorSafe(f"Clave vacía en la línea {line_number} de {path}")
        if raw_value.startswith(("'", '"')):
            try:
                raw_value = shlex.split(raw_value)[0]
            except (ValueError, IndexError) as exc:
                raise ImportErrorSafe(f"Valor entrecomillado inválido en línea {line_number}") from exc
        values[key] = raw_value
    return values


def merged_environment(path: Path) -> dict[str, str]:
    return {**read_env_file(path), **os.environ}


def read_events(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ImportErrorSafe(f"JSON inválido en la línea {line_number}: {exc}") from exc
            yield event


def write_report(path: Path, report: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = path.with_name(f"{path.stem}-{stamp}{path.suffix}")
        if path.exists():
            raise ImportErrorSafe(f"Ya existe el informe {path}; no se sobrescribió")
    path.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def load_postgresql(input_path: Path, env: dict[str, str], dataset_report: dict[str, Any], batch_size: int) -> dict[str, Any]:
    required = ("POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [name for name in required if not env.get(name)]
    if missing:
        raise ImportErrorSafe(f"Faltan variables del archivo de entorno: {', '.join(missing)}")
    params = {
        "host": env.get("POSTGRES_HOST", "127.0.0.1"),
        "port": int(env.get("POSTGRES_HOST_PORT", "5432")),
        "dbname": env.get("POSTGRES_DB", "security_benchmark"),
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
        "application_name": "u2-benchmark-import",
    }
    expected_count = int(dataset_report["count"])
    sample_positions = {0, expected_count // 2, expected_count - 1}
    samples: dict[int, dict[str, Any]] = {}
    imported = 0
    conn = psycopg.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS events (id UUID PRIMARY KEY, data JSONB NOT NULL)")
            cur.execute("CREATE TEMP TABLE _u2_events_import (id UUID NOT NULL, data JSONB NOT NULL) ON COMMIT PRESERVE ROWS")
            conn.commit()

        batch: list[dict[str, Any]] = []

        def flush_batch(items: list[dict[str, Any]]) -> None:
            nonlocal imported
            with conn.cursor() as cur:
                cur.execute("TRUNCATE pg_temp._u2_events_import")
                with cur.copy("COPY pg_temp._u2_events_import (id, data) FROM STDIN") as copy:
                    for event in items:
                        copy.write_row((event["event_id"], Jsonb(event)))
                cur.execute(
                    """INSERT INTO events AS current_events (id, data)
                       SELECT id, data FROM pg_temp._u2_events_import
                       ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
                       WHERE current_events.data IS DISTINCT FROM EXCLUDED.data"""
                )
            conn.commit()
            imported += len(items)
            if imported % (batch_size * 10) == 0 or imported == expected_count:
                print(f"PostgreSQL: procesados {imported}/{expected_count}", file=sys.stderr, flush=True)

        for index, event in enumerate(read_events(input_path)):
            if index in sample_positions:
                samples[index] = event
            batch.append(event)
            if len(batch) >= batch_size:
                flush_batch(batch)
                batch = []
        if batch:
            flush_batch(batch)
        if imported != expected_count:
            raise ImportErrorSafe(f"Se leyeron {imported} eventos; el manifiesto indica {expected_count}")

        with conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS events_timestamp_idx ON events ((data->>'timestamp'))")
            cur.execute("ANALYZE events")
            cur.execute("SELECT count(*) FROM events")
            actual_count = int(cur.fetchone()[0])
            if actual_count != expected_count:
                raise ImportErrorSafe(
                    f"La tabla tiene {actual_count} filas, se esperaban {expected_count}; no se borraron filas sobrantes"
                )
            for index, event in samples.items():
                cur.execute("SELECT data FROM events WHERE id = %s", (event["event_id"],))
                row = cur.fetchone()
                if row is None or row[0] != event:
                    raise ImportErrorSafe(f"La muestra de verificación en posición {index} no coincide con el JSONL")
            cur.execute("SELECT version(), current_setting('server_version_num')")
            server_version, server_version_num = cur.fetchone()
            cur.execute("SELECT name, setting, unit FROM pg_settings WHERE name = ANY(%s) ORDER BY name", ([
                "shared_buffers", "max_connections", "effective_cache_size", "hash_mem_multiplier",
                "max_parallel_workers", "max_parallel_workers_per_gather", "work_mem", "statement_timeout",
            ],))
            parameters = {name: {"setting": value, "unit": unit} for name, value, unit in cur.fetchall()}
            cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'events' ORDER BY indexname")
            indexes = [{"name": name, "definition": definition} for name, definition in cur.fetchall()]
            index_names = {index["name"] for index in indexes}
            expected_indexes = {"events_pkey", "events_timestamp_idx"}
            if index_names != expected_indexes:
                raise ImportErrorSafe(f"Inventario de índices inesperado: {sorted(index_names)}")
            cur.execute("SELECT pg_relation_size('events'), pg_indexes_size('events'), pg_total_relation_size('events')")
            table_bytes, index_bytes, total_bytes = (int(value) for value in cur.fetchone())
        conn.commit()
        return {
            "engine": "postgresql",
            "database": params["dbname"],
            "table": "events",
            "server_version": server_version,
            "server_version_num": server_version_num,
            "rows_imported_or_matched": imported,
            "row_count": actual_count,
            "samples_verified": len(samples),
            "indexes": indexes,
            "sizes_bytes": {"table": table_bytes, "indexes": index_bytes, "total": total_bytes},
            "parameters": parameters,
            "analyze": "completed",
            "dataset_sha256": dataset_report["sha256"],
            "dataset_count": expected_count,
        }
    finally:
        conn.close()


def load_mongodb(input_path: Path, env: dict[str, str], dataset_report: dict[str, Any], batch_size: int) -> dict[str, Any]:
    required = ("MONGO_INITDB_ROOT_USERNAME", "MONGO_INITDB_ROOT_PASSWORD")
    missing = [name for name in required if not env.get(name)]
    if missing:
        raise ImportErrorSafe(f"Faltan variables del archivo de entorno: {', '.join(missing)}")
    client = MongoClient(
        host=env.get("MONGO_HOST", "127.0.0.1"),
        port=int(env.get("MONGO_HOST_PORT", "27017")),
        username=env["MONGO_INITDB_ROOT_USERNAME"],
        password=env["MONGO_INITDB_ROOT_PASSWORD"],
        authSource="admin",
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        appname="u2-benchmark-import",
    )
    try:
        client.admin.command("ping")
        database = client["security_benchmark"]
        collection = database["events"]
        if "events" not in database.list_collection_names():
            database.create_collection("events")
            collection = database["events"]

        existing_indexes = list(collection.list_indexes())
        expected_names = {"_id_", "event_id_idx", "timestamp_idx"}
        if existing_indexes:
            names = {index["name"] for index in existing_indexes}
            if not names.issubset(expected_names):
                raise ImportErrorSafe(f"La colección tiene índices no acordados: {sorted(names - expected_names)}")

        # The idempotent upsert filters by event_id; build this agreed index first
        # so each batch does not scan every document imported by prior batches.
        collection.create_index([("event_id", ASCENDING)], name="event_id_idx")
        expected_count = int(dataset_report["count"])
        sample_positions = {0, expected_count // 2, expected_count - 1}
        samples: dict[int, dict[str, Any]] = {}
        operations: list[ReplaceOne] = []
        imported = 0
        for index, event in enumerate(read_events(input_path)):
            if index in sample_positions:
                samples[index] = event
            operations.append(ReplaceOne({"event_id": event["event_id"]}, event, upsert=True))
            if len(operations) >= batch_size:
                collection.bulk_write(operations, ordered=False)
                imported += len(operations)
                operations = []
                if imported % (batch_size * 10) == 0:
                    print(f"MongoDB: procesados {imported}/{expected_count}", file=sys.stderr, flush=True)
        if operations:
            collection.bulk_write(operations, ordered=False)
            imported += len(operations)
        if imported != expected_count:
            raise ImportErrorSafe(f"Se leyeron {imported} eventos; el manifiesto indica {expected_count}")

        collection.create_index([("timestamp", ASCENDING)], name="timestamp_idx")
        actual_count = collection.count_documents({})
        if actual_count != expected_count:
            raise ImportErrorSafe(
                f"La colección tiene {actual_count} documentos, se esperaban {expected_count}; no se borraron documentos sobrantes"
            )
        for index, event in samples.items():
            stored = collection.find_one({"event_id": event["event_id"]}, {"_id": 0})
            if stored != event:
                raise ImportErrorSafe(f"La muestra de verificación en posición {index} no coincide con el JSONL")
        indexes = list(collection.list_indexes())
        names = {index["name"] for index in indexes}
        if names != expected_names:
            raise ImportErrorSafe(f"Inventario de índices inesperado: {sorted(names)}")
        stats = database.command("collStats", "events")
        return {
            "engine": "mongodb",
            "database": "security_benchmark",
            "collection": "events",
            "server_version": client.server_info().get("version"),
            "rows_imported_or_matched": imported,
            "document_count": actual_count,
            "samples_verified": len(samples),
            "indexes": indexes,
            "sizes_bytes": {
                "data": int(stats.get("size", 0)),
                "storage": int(stats.get("storageSize", 0)),
                "indexes": int(stats.get("totalIndexSize", 0)),
            },
            "dataset_sha256": dataset_report["sha256"],
            "dataset_count": expected_count,
        }
    finally:
        client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--report", type=Path, help="Ruta del informe; por defecto crea uno nuevo en data/imports/")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size debe ser mayor que cero")
    input_path = args.input if args.input.is_absolute() else ROOT / args.input
    env_path = args.env_file if args.env_file.is_absolute() else ROOT / args.env_file
    try:
        dataset_report = validate_dataset(input_path, args.config, args.schema)
        config = json.loads(args.config.read_text(encoding="utf-8"))
        if dataset_report["count"] != int(config["official_count"]):
            raise ImportErrorSafe(
                f"La importación de fase 03 requiere official_count={config['official_count']}; se recibieron {dataset_report['count']} eventos"
            )
        env = merged_environment(env_path)
        if args.engine == "postgresql":
            engine_report = load_postgresql(input_path, env, dataset_report, args.batch_size)
        else:
            engine_report = load_mongodb(input_path, env, dataset_report, args.batch_size)
        report = {
            "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "dataset": dataset_report,
            "import": engine_report,
        }
        report_path = args.report
        if report_path is None:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            report_path = ROOT / "data/imports" / f"{args.engine}-{stamp}.json"
        elif not report_path.is_absolute():
            report_path = ROOT / report_path
        report_path = write_report(report_path, report)
    except (DatasetValidationError, ImportErrorSafe, OSError, ValueError, psycopg.Error) as exc:
        print(f"Importación detenida: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        # Driver exceptions should be surfaced without exposing passwords or DSNs.
        print(f"Importación detenida ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"report": str(report_path), "import": engine_report}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
