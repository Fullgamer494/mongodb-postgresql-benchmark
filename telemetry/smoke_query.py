#!/usr/bin/env python3
"""Run exactly one full Q1 diagnostic query and capture its telemetry evidence."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from load_data import merged_environment  # noqa: E402
from telemetry import collector, evidence  # noqa: E402


def utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def run_id(engine: str) -> str:
    return f"phase05-{engine}-q1-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def execute_query(engine: str, ident: str) -> dict:
    env = merged_environment(ROOT / ".env")
    start = time.time()
    monotonic = time.monotonic()
    if engine == "mongodb":
        from pymongo import MongoClient

        client = MongoClient(
            host=env.get("MONGO_HOST", "127.0.0.1"),
            port=int(env.get("MONGO_HOST_PORT", "27017")),
            username=env["MONGO_INITDB_ROOT_USERNAME"],
            password=env["MONGO_INITDB_ROOT_PASSWORD"],
            authSource="admin",
            serverSelectionTimeoutMS=10000,
            appname=ident,
        )
        try:
            collection = client.security_benchmark.events
            pipeline = json.loads((ROOT / "queries/mongodb/q1.pipeline.json").read_text())
            count = sum(1 for _ in collection.aggregate(pipeline, allowDiskUse=False, comment=ident))
        finally:
            client.close()
    else:
        import psycopg

        params = {
            "host": env.get("POSTGRES_HOST", "127.0.0.1"),
            "port": int(env.get("POSTGRES_HOST_PORT", "5432")),
            "dbname": env.get("POSTGRES_DB", "security_benchmark"),
            "user": env["POSTGRES_USER"],
            "password": env["POSTGRES_PASSWORD"],
            "connect_timeout": 10,
            "application_name": ident,
        }
        with psycopg.connect(**params) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET work_mem='4MB'")
                cursor.execute("SHOW work_mem")
                work_mem = cursor.fetchone()[0]
                cursor.execute((ROOT / "queries/postgresql/q1.sql").read_text())
                count = len(cursor.fetchall())
        if work_mem != "4MB":
            raise RuntimeError(f"work_mem inesperado: {work_mem}")
    finish = time.time()
    return {
        "run_id": ident,
        "engine": engine,
        "query_id": "Q1",
        "diagnostic_only": True,
        "started_epoch": start,
        "started_at_utc": utc(start),
        "finished_epoch": finish,
        "finished_at_utc": utc(finish),
        "wall_seconds": time.monotonic() - monotonic,
        "result_rows_consumed": count,
        "expected_result_rows": 30,
        "status": "ok" if count == 30 else "error",
        "work_mem": work_mem if engine == "postgresql" else None,
    }


def capture(engine: str, container: str, ident: str, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=False)
    state_path = out_dir / "snapshot-before.json"
    before = evidence.inspect_container(container)
    if before["status"] != "running":
        raise RuntimeError(f"Motor no está activo: {container}")
    started = time.time()
    state_path.write_text(json.dumps({
        "schema_version": 1,
        "run_id": ident,
        "engine": engine,
        "container_name": container,
        "started_epoch": started,
        "started_at_utc": utc(started),
        "before": before,
    }, sort_keys=True, indent=2) + "\n")

    query_error = None
    try:
        query = execute_query(engine, ident)
    except Exception as exc:
        query_error = {"type": type(exc).__name__, "message": str(exc)}
        finished = time.time()
        query = {
            "run_id": ident, "engine": engine, "query_id": "Q1", "diagnostic_only": True,
            "started_epoch": started, "started_at_utc": utc(started),
            "finished_epoch": finished, "finished_at_utc": utc(finished),
            "status": "error", "error": query_error,
        }
    query_path = out_dir / "query.json"
    query_path.write_text(json.dumps(query, sort_keys=True, indent=2) + "\n")

    # Let a final one-second Prometheus scrape land; collection range still ends at query finish.
    time.sleep(2.2)
    after = evidence.inspect_container(container)
    finished_capture = time.time()
    logs_result = evidence.run(["docker", "logs", "--timestamps", "--since", utc(started), "--until", utc(finished_capture), container])
    events_result = evidence.run(["docker", "events", "--since", utc(started), "--until", utc(finished_capture), "--filter", f"container={before['container_id']}", "--format", "{{json .}}"])
    logs = logs_result.stdout + logs_result.stderr
    events = evidence.parse_docker_events(events_result.stdout + events_result.stderr)
    spill = evidence.classify_mongodb_spill(logs, ident) if engine == "mongodb" else evidence.classify_postgresql_spill(logs, ident)
    oom = evidence.classify_oom(before, after, events)
    (out_dir / "engine.log").write_text(logs)
    (out_dir / "docker-events.jsonl").write_text("\n".join(json.dumps(row, sort_keys=True) for row in events) + ("\n" if events else ""))
    evidence_report = {
        "schema_version": 1, "run_id": ident, "engine": engine,
        "container_name": container, "container_id_before": before["container_id"],
        "container_id_after": after["container_id"], "started_at_utc": utc(started),
        "query_finished_at_utc": query.get("finished_at_utc"), "captured_at_utc": utc(finished_capture),
        "spill_evidence": spill, "oom_evidence": oom,
        "engine_logs_exit_code": logs_result.returncode,
        "docker_events_exit_code": events_result.returncode,
        "docker_event_count": len(events),
        "status": "ok" if spill["status"] != "unknown" and oom["status"] != "unknown" and logs_result.returncode == 0 else "error",
    }
    (out_dir / "evidence.json").write_text(json.dumps(evidence_report, sort_keys=True, indent=2) + "\n")

    if query.get("status") == "ok":
        args = argparse.Namespace(
            prometheus_url="http://127.0.0.1:9090", run_id=ident, engine=engine,
            container_id=before["container_id"], start=max(0, query["started_epoch"] - 2),
            end=query["finished_epoch"], step=1.0, timeout=15.0,
        )
        report, _ = collector.collect(args)
        (out_dir / "metrics.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    status = "ok" if (
        query.get("status") == "ok"
        and evidence_report["status"] == "ok"
        and report is not None
        and report["status"] == "ok"
    ) else "error"
    print(json.dumps({
        "run_id": ident, "engine": engine, "status": status,
        "query_rows": query.get("result_rows_consumed"), "query_seconds": query.get("wall_seconds"),
        "spill": spill["status"], "oom": oom["status"],
        "metrics_status": report["status"] if query.get("status") == "ok" else "not_collected",
        "output": str(out_dir.relative_to(ROOT)),
    }, sort_keys=True))
    return {"status": status, "output": str(out_dir), "metrics": report if query.get("status") == "ok" else None}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("mongodb", "postgresql"), required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    ident = args.run_id or run_id(args.engine)
    out = args.output if args.output.is_absolute() else ROOT / args.output
    try:
        result = capture(args.engine, args.container, ident, out)
    except Exception as exc:
        print(f"smoke bloqueado: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0 if result["status"] == "ok" and result["metrics"] and result["metrics"]["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
