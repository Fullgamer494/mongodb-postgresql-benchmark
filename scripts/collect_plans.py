#!/usr/bin/env python3
"""Save real, non-benchmark execution plans for one engine and one query."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient

from load_data import DEFAULT_ENV, ImportErrorSafe, merged_environment

ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "data/generated/events.jsonl"
DATASET_CONFIG = ROOT / "config/dataset.json"
SCHEMA = ROOT / "contracts/event.schema.json"
QUERY_FILES = {
    ("mongodb", "Q1"): ROOT / "queries/mongodb/q1.pipeline.json",
    ("mongodb", "Q2"): ROOT / "queries/mongodb/q2.pipeline.json",
    ("postgresql", "Q1"): ROOT / "queries/postgresql/q1.sql",
    ("postgresql", "Q2"): ROOT / "queries/postgresql/q2.sql",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def docker_metadata(service: str) -> dict[str, Any]:
    try:
        ids = subprocess.run(
            ["docker", "ps", "-q", "--filter", f"label=com.docker.compose.service={service}"],
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout.splitlines()
        if len(ids) != 1:
            return {"container_inspect": "unavailable", "reason": f"expected one running {service} container, found {len(ids)}"}
        raw = subprocess.run(["docker", "inspect", ids[0]], capture_output=True, text=True, check=True, timeout=10).stdout
        container = json.loads(raw)[0]
        config = container.get("Config", {})
        host = container.get("HostConfig", {})
        return {
            "container_id": container.get("Id"),
            "image_id": container.get("Image"),
            "image": config.get("Image"),
            "labels": {key: val for key, val in config.get("Labels", {}).items() if key.startswith("com.docker.compose") or key.startswith("com.u2-benchmark")},
            "nano_cpus": host.get("NanoCpus"),
            "memory_bytes": host.get("Memory"),
            "memory_swap_bytes": host.get("MemorySwap"),
        }
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        return {"container_inspect": "unavailable", "reason": f"{type(exc).__name__}: {exc}"}


def pg_session_settings(cursor) -> dict[str, Any]:
    names = (
        "server_version", "work_mem", "shared_buffers", "effective_cache_size",
        "hash_mem_multiplier", "max_parallel_workers", "max_parallel_workers_per_gather",
        "statement_timeout",
    )
    settings: dict[str, Any] = {}
    for name in names:
        cursor.execute("SELECT current_setting(%s)", (name,))
        settings[name] = cursor.fetchone()["current_setting"]
    return settings


def run_postgresql(query: str, treatment: str, env: dict[str, str]) -> tuple[Any, dict[str, Any], float, str, str]:
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
        "application_name": "u2-plan-collection",
    }
    work_mem = "4MB" if query == "Q1" or treatment == "Q2" else "64MB"
    started_at = now()
    start = time.monotonic()
    with psycopg.connect(**params, row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT set_config('work_mem', %s, true)", (work_mem,))
            configured = cursor.fetchone()["set_config"]
            cursor.execute("SHOW work_mem")
            observed = cursor.fetchone()["work_mem"]
            if configured != work_mem or observed != work_mem:
                raise ImportErrorSafe(f"work_mem esperado {work_mem}, observado {observed}")
            settings = pg_session_settings(cursor)
            settings["work_mem_set_config"] = configured
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + (QUERY_FILES[("postgresql", query)]).read_text(encoding="utf-8"))
            result = cursor.fetchone()["QUERY PLAN"]
    duration = time.monotonic() - start
    return result, settings, duration, started_at, now()


def run_mongodb(query: str, treatment: str, env: dict[str, str]) -> tuple[Any, dict[str, Any], float, str, str]:
    username, password = env.get("MONGO_INITDB_ROOT_USERNAME"), env.get("MONGO_INITDB_ROOT_PASSWORD")
    if not username or not password:
        raise ImportErrorSafe("Faltan credenciales MongoDB")
    client = MongoClient(
        host=env.get("MONGO_HOST", "127.0.0.1"), port=int(env.get("MONGO_HOST_PORT", "27017")),
        username=username, password=password, authSource="admin", serverSelectionTimeoutMS=10000,
        appname="u2-plan-collection",
    )
    allow_disk_use = query == "Q2" and treatment == "Q2_ALT"
    pipeline = json.loads(QUERY_FILES[("mongodb", query)].read_text(encoding="utf-8"))
    started_at = now()
    start = time.monotonic()
    try:
        client.admin.command("ping")
        server = client.server_info()
        engine_settings = client.admin.command({
            "getParameter": 1,
            "internalQueryMaxMemoryUsageBytesPerOperation": 1,
            "internalQueryMaxSingleExpressionMemoryUsageBytes": 1,
            "internalQueryStatsSampleRate": 1,
        })
        command = {
            "explain": {
                "aggregate": "events", "pipeline": pipeline, "cursor": {},
                "allowDiskUse": allow_disk_use,
            },
            "verbosity": "executionStats",
        }
        plan = client["security_benchmark"].command(command)
    finally:
        client.close()
    duration = time.monotonic() - start
    settings = {
        "server_version": server.get("version"),
        "allowDiskUse": allow_disk_use,
        "engine_settings": {key: value for key, value in engine_settings.items() if key != "ok"},
        "database": "security_benchmark",
        "collection": "events",
    }
    return plan, settings, duration, started_at, now()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    parser.add_argument("--query", required=True, choices=("Q1", "Q2"))
    parser.add_argument("--treatment", choices=("Q2", "Q2_ALT"))
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.query == "Q1" and args.treatment is not None:
        parser.error("Q1 tiene tratamiento fijo; omite --treatment")
    treatment = args.treatment or "fixed"
    output = args.output if args.output.is_absolute() else ROOT / args.output
    if output.exists():
        parser.error(f"No se sobrescribe evidencia existente: {output}")
    query_path = QUERY_FILES[(args.engine, args.query)]
    report: dict[str, Any] = {
        "created_at_utc": now(), "status": "error", "engine": args.engine,
        "query": args.query, "treatment": treatment,
        "query_file": str(query_path.relative_to(ROOT)), "query_sha256": sha256(query_path),
        "dataset": None, "parameters": {}, "duration_seconds": None,
        "container": docker_metadata(args.engine),
        "started_at_utc": None, "finished_at_utc": None, "plan": None, "error": None,
    }
    collection_start = time.monotonic()
    collection_started_at = now()
    try:
        manifest_path = DATASET.with_suffix(DATASET.suffix + ".manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        actual_sha256 = sha256(DATASET)
        if manifest.get("count") != 500000 or manifest.get("sha256") != actual_sha256:
            raise ImportErrorSafe("Dataset no coincide con el manifiesto oficial de 500000 filas")
        report["dataset"] = {"count": manifest["count"], "sha256": actual_sha256,
                              "manifest": str(manifest_path.relative_to(ROOT))}
        env = merged_environment(args.env_file)
        if args.engine == "mongodb":
            result, settings, duration, started, finished = run_mongodb(args.query, treatment, env)
        else:
            result, settings, duration, started, finished = run_postgresql(args.query, treatment, env)
        report.update({
            "status": "ok", "parameters": settings, "duration_seconds": duration,
            "started_at_utc": started, "finished_at_utc": finished, "plan": result,
        })
    except Exception as exc:  # Persist the diagnostic failure as requested by the phase contract.
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        report["started_at_utc"] = collection_started_at
        report["finished_at_utc"] = now()
        report["duration_seconds"] = time.monotonic() - collection_start
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "engine": args.engine, "query": args.query,
                      "treatment": treatment, "duration_seconds": report["duration_seconds"],
                      "output": str(output), "error": report["error"]}, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
