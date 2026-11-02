"""Single-cell benchmark lifecycle. This module performs no orchestration of other engines."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import queue
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
from telemetry import collector, evidence
sys.path.insert(0, str(ROOT / "scripts"))
from load_data import merged_environment

from .drivers import connect_worker, query_path, treatment
from .metrics import classify_run_status, completed_inside_window, csv_values, write_requests, write_summary

CSV_HEADER = next(csv_line.strip().split(",") for csv_line in (ROOT / "contracts/results_header.csv").read_text(encoding="utf-8").splitlines() if csv_line.strip())
RESOURCE_PROFILES = json.loads((ROOT / "config/resource_profiles.json").read_text(encoding="utf-8"))
DATASET_MANIFEST = ROOT / "data/generated/events.jsonl.manifest.json"


class RunFailure(RuntimeError):
    pass


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def new_run_id(engine: str, mode: str, query_id: str, profile: str, concurrency: int, run_number: int) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    engine_code = "mg" if engine == "mongodb" else "pg"
    profile_code = {"PROFILE_A_BASELINE": "a", "PROFILE_B_MEDIUM": "b", "PROFILE_C_SEVERE": "c"}[profile]
    mode_code = {"smoke": "s", "pilot": "p", "official": "o"}[mode]
    return f"{engine_code}-{query_id.lower()}-{profile_code}-c{concurrency}-r{run_number}-{mode_code}-{stamp}-{uuid.uuid4().hex[:6]}"


def query_options(engine: str, query_id: str) -> dict[str, Any]:
    return treatment(engine, query_id)


def plan_work_reference(engine: str, query_id: str) -> dict[str, Any]:
    """Attach separately collected EXPLAIN work counts as estimates, never run plans here."""
    if query_id == "Q1":
        filename = "mongodb-q1-fixed-current.json" if engine == "mongodb" else "postgresql-q1-fixed-retry.json"
    elif query_id in {"Q2", "Q2_ALT"}:
        filename = f"{engine}-q2-{'alt' if query_id == 'Q2_ALT' else 'base'}" + ("-current.json" if engine == "mongodb" else ".json")
    else:
        return {"source": None, "documents_per_query": None, "detections_per_query": None, "basis": "no_known_plan"}
    path = ROOT / "results/plans/phase04-profile-a" / filename
    if not path.exists():
        return {"source": str(path.relative_to(ROOT)), "documents_per_query": None, "detections_per_query": None, "basis": "plan_missing"}
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("status") != "ok":
        return {"source": str(path.relative_to(ROOT)), "documents_per_query": None, "detections_per_query": None, "basis": "plan_not_ok"}
    documents = None
    detections = None
    if engine == "mongodb":
        stages: list[dict[str, Any]] = []
        def walk(node: Any) -> None:
            if isinstance(node, dict):
                stages.append(node)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)
        walk(doc.get("plan", {}))
        values = [node.get("docsExamined") for node in stages if isinstance(node.get("docsExamined"), (int, float))]
        documents = max(values) if values else None
        if query_id == "Q1" and documents is not None:
            detections = int(json.loads(DATASET_MANIFEST.read_text(encoding="utf-8"))["detections"]["total"])
    else:
        roots = doc.get("plan") or []
        nodes: list[dict[str, Any]] = []
        def walk(node: Any) -> None:
            if isinstance(node, dict):
                nodes.append(node)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)
        walk(roots)
        scans = [node for node in nodes if node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "events"]
        if scans:
            documents = max(
                int(round((float(node.get("Actual Rows", 0)) + float(node.get("Rows Removed by Filter", 0))) * float(node.get("Actual Loops", 1))))
                for node in scans
            )
        if query_id == "Q1" and documents is not None:
            detections = int(json.loads(DATASET_MANIFEST.read_text(encoding="utf-8"))["detections"]["total"])
    return {
        "source": str(path.relative_to(ROOT)),
        "documents_per_query": documents,
        "detections_per_query": detections,
        "basis": "separate_phase04_execution_plan; estimate_only",
    }


def _write_json(path: Path, value: Any) -> None:
    if path.exists():
        raise FileExistsError(f"No se sobrescribe evidencia: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _classify_exception(exc: BaseException, cancellation_requested: bool) -> str:
    if cancellation_requested:
        return "cancelled"
    name = type(exc).__name__.lower()
    if any(term in name for term in ("timeout", "executiontimeout", "querycanceled", "querycancelled")):
        return "timeout"
    return "error"


def _preflight(engine: str, profile: str, output: Path) -> dict[str, Any]:
    command = [sys.executable, "scripts/preflight.py", "--engine", engine, "--profile", profile, "--output", str(output)]
    proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    if proc.returncode:
        raise RunFailure(f"preflight terminó con estado {proc.returncode}: {proc.stdout[-1500:]} {proc.stderr[-1500:]}")
    report = json.loads(output.read_text(encoding="utf-8"))
    if not report.get("ready"):
        raise RunFailure("preflight no marca ready=true")
    return report


def _assert_single_engine_active(selected_engine: str) -> None:
    listed = subprocess.run(
        ["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=u2-benchmark"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if listed.returncode:
        raise RunFailure(f"no se pudo inspeccionar motores activos: {listed.stderr.strip()}")
    for container_id in listed.stdout.splitlines():
        inspected = subprocess.run(["docker", "inspect", container_id], cwd=ROOT, capture_output=True, text=True, check=False)
        if inspected.returncode:
            raise RunFailure(f"docker inspect falló para {container_id}: {inspected.stderr.strip()}")
        container = json.loads(inspected.stdout)[0]
        service = (container.get("Config", {}).get("Labels") or {}).get("com.docker.compose.service")
        if service in {"mongodb", "postgresql"} and service != selected_engine:
            raise RunFailure(f"motor no seleccionado sigue activo: {service}; deténgalo antes de correr {selected_engine}")


def _run_queries(args: Any, env: dict[str, str], run_id: str, window: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    concurrency = args.concurrency
    ready_state: dict[str, Any] = {"warmup_start_mono": None, "measurement_start_mono": None, "measurement_start_epoch": None}
    admission_open = threading.Event()
    ready_barrier = threading.Barrier(concurrency + 1, action=lambda: ready_state.update(warmup_start_mono=time.monotonic()))
    clock_lock = threading.Lock()
    def start_measurement() -> None:
        with clock_lock:
            ready_state["measurement_start_mono"] = time.monotonic()
            ready_state["measurement_start_epoch"] = time.time()
    measurement_barrier = threading.Barrier(concurrency + 1, action=start_measurement)
    shared_lock = threading.Lock()
    shared = {
        "workers_ready": 0,
        "connections_opened": 0,
        "server_version": None,
        "engine_parameters": None,
        "active": 0,
        "max_in_flight": 0,
        "max_in_flight_measurement": 0,
        "pending_at_window_close": 0,
        "errors": [],
        "worker_threads": [],
    }
    cancel_requested = threading.Event()
    drivers: dict[int, Any] = {}
    latencies: list[float] = []
    outcomes = {"warmup_completed": 0, "measurement_completed": 0, "measurement_successes": 0, "measurement_failures": 0, "drain_completed": 0, "drain_failures": 0, "cancelled": 0, "timed_out": 0}

    def append_event(event: dict[str, Any]) -> None:
        with shared_lock:
            events.append(event)

    def execute_one(driver: Any, client_id: int, phase: str) -> bool:
        started_mono = time.monotonic()
        started_epoch = time.time()
        with shared_lock:
            shared["active"] += 1
            shared["max_in_flight"] = max(shared["max_in_flight"], shared["active"])
            if phase == "measurement":
                shared["max_in_flight_measurement"] = max(shared["max_in_flight_measurement"], shared["active"])
            in_flight = shared["active"]
        result_count = None
        error = None
        status = "ok"
        connection_usable = True
        try:
            result_count = driver.execute()
        except Exception as exc:
            status = _classify_exception(exc, cancel_requested.is_set())
            error = {"type": type(exc).__name__, "message": str(exc)[:1000]}
            check_connection = getattr(driver, "connection_usable", None)
            if callable(check_connection):
                try:
                    connection_usable = bool(check_connection())
                except Exception:
                    connection_usable = False
        finished_mono = time.monotonic()
        finished_epoch = time.time()
        latency_ms = (finished_mono - started_mono) * 1000
        measured = phase == "measurement" and completed_inside_window(
            started_mono,
            finished_mono,
            float(ready_state["measurement_start_mono"]),
            float(ready_state["measurement_start_mono"]) + args.measurement_seconds,
        )
        with shared_lock:
            shared["active"] -= 1
            if phase == "warmup" and status == "ok": outcomes["warmup_completed"] += 1
            if phase == "measurement":
                if measured:
                    outcomes["measurement_completed"] += 1
                    if status == "ok":
                        outcomes["measurement_successes"] += 1
                        latencies.append(latency_ms)
                    else:
                        outcomes["measurement_failures"] += 1
                elif status == "ok":
                    outcomes["drain_completed"] += 1
                else:
                    outcomes["drain_failures"] += 1
            if status == "timeout": outcomes["timed_out"] += 1
            if status == "cancelled": outcomes["cancelled"] += 1
            if error:
                shared["errors"].append({"phase": phase, "client_id": client_id, **error, "classification": status})
        append_event({
            "request_id": str(uuid.uuid4()), "run_id": run_id, "client_id": client_id,
            "thread_name": threading.current_thread().name, "phase": phase,
            "started_epoch": started_epoch, "finished_epoch": finished_epoch,
            "elapsed_ms": round(latency_ms, 6), "status": status,
            "result_count": result_count, "client_in_flight_at_start": in_flight,
            "completed_inside_measurement_window": measured, "error": error,
        })
        return connection_usable

    def worker(client_id: int) -> None:
        driver = None
        try:
            driver = connect_worker(args.engine, env, args.query, run_id, client_id)
            drivers[client_id] = driver
            with shared_lock:
                shared["workers_ready"] += 1
                shared["connections_opened"] += 1
                if shared["server_version"] is None:
                    shared["server_version"] = driver.server_version()
                    shared["engine_parameters"] = driver.effective_settings()
            ready_barrier.wait(timeout=60)
            warmup_start = ready_state["warmup_start_mono"]
            warmup_end = float(warmup_start or time.monotonic()) + args.warmup_seconds
            while time.monotonic() < warmup_end:
                if not execute_one(driver, client_id, "warmup"):
                    break
            measurement_barrier.wait(timeout=max(60, args.warmup_seconds + 10))
            while admission_open.is_set() and time.monotonic() < float(ready_state["measurement_start_mono"]) + args.measurement_seconds:
                if not execute_one(driver, client_id, "measurement"):
                    break
        except threading.BrokenBarrierError as exc:
            with shared_lock:
                shared["errors"].append({"client_id": client_id, "type": type(exc).__name__, "message": "worker synchronization failed", "classification": "error"})
            try: ready_barrier.abort()
            except Exception: pass
            try: measurement_barrier.abort()
            except Exception: pass
        except Exception as exc:
            with shared_lock:
                shared["errors"].append({"client_id": client_id, "type": type(exc).__name__, "message": str(exc)[:1000], "classification": "error"})
            try: ready_barrier.abort()
            except Exception: pass
            try: measurement_barrier.abort()
            except Exception: pass
        finally:
            if driver is not None:
                try: driver.close()
                except Exception: pass

    threads = [threading.Thread(target=worker, args=(client_id,), name=f"benchmark-client-{client_id:02d}", daemon=True) for client_id in range(1, concurrency + 1)]
    for thread in threads: thread.start()
    try:
        ready_barrier.wait(timeout=60)
        warmup_end = float(ready_state["warmup_start_mono"] or time.monotonic()) + args.warmup_seconds
        while time.monotonic() < warmup_end:
            time.sleep(min(0.1, max(0, warmup_end - time.monotonic())))
        admission_open.set()
        measurement_barrier.wait(timeout=60)
        measurement_start = float(ready_state["measurement_start_mono"])
        measurement_end = measurement_start + args.measurement_seconds
        window.update({"start_epoch": ready_state["measurement_start_epoch"], "start_monotonic": measurement_start})
        while time.monotonic() < measurement_end:
            time.sleep(min(0.1, max(0, measurement_end - time.monotonic())))
        admission_open.clear()
        with shared_lock:
            shared["pending_at_window_close"] = shared["active"]
        window.update({
            "end_epoch": float(ready_state["measurement_start_epoch"]) + args.measurement_seconds,
            "end_monotonic": measurement_end,
            "valid": True,
        })
        drain_deadline = time.monotonic() + args.drain_seconds
        for thread in threads:
            remaining = max(0.0, drain_deadline - time.monotonic())
            thread.join(timeout=remaining)
        lingering = [thread for thread in threads if thread.is_alive()]
        if lingering:
            cancel_requested.set()
            for driver in list(drivers.values()):
                try: driver.cancel()
                except Exception: pass
            with shared_lock:
                outcomes["cancelled"] += len(lingering)
                for thread in lingering:
                    events.append({
                        "request_id": str(uuid.uuid4()), "run_id": run_id,
                        "client_id": int(thread.name.rsplit("-", 1)[-1]),
                        "thread_name": thread.name, "phase": "drain",
                        "status": "cancel_requested", "completed_inside_measurement_window": False,
                        "error": {"type": "DrainDeadlineExceeded", "message": "cancellation requested at maximum drain deadline"},
                    })
            for thread in lingering:
                thread.join(timeout=10)
        if args.cooldown_seconds:
            time.sleep(args.cooldown_seconds)
    except (threading.BrokenBarrierError, Exception) as exc:
        admission_open.clear()
        with shared_lock:
            shared["errors"].append({"type": type(exc).__name__, "message": str(exc)[:1000], "classification": "error"})
        ready_barrier.abort()
        measurement_barrier.abort()
        for thread in threads: thread.join(timeout=1)
    shared["worker_threads"] = [{"name": thread.name, "alive_after_drain": thread.is_alive()} for thread in threads]
    shared["client_concurrency"] = {
        "requested_clients": concurrency,
        "workers_ready": shared["workers_ready"],
        "connections_opened": shared["connections_opened"],
        "max_in_flight_client_operations": shared["max_in_flight"],
        "max_in_flight_measurement_operations": shared["max_in_flight_measurement"],
        "one_active_operation_per_client": True,
    }
    shared["outcomes"] = outcomes
    shared["latencies_ms"] = latencies
    return shared


def _write_failed_artifacts(run_dir: Path, args: Any, run_id: str, reason: str) -> None:
    row = csv_values(
        timestamp_epoch=None, engine=args.engine, profile=args.profile, query_id=args.query,
        concurrency=args.concurrency, run_number=args.run_number, measurement_valid=False,
        measurement_seconds=None, latencies_ms=[], cpu_throttled_seconds=None,
        max_rss_bytes=None, disk_spill=None, status="error",
    )
    write_summary(run_dir / "summary.csv", CSV_HEADER, row)
    write_requests(run_dir / "requests.jsonl", [])
    _write_json(run_dir / "quality.json", {"run_id": run_id, "status": "error", "measurement_valid": False, "reason": reason})


def execute_run(args: Any) -> Path:
    run_id = args.run_id or new_run_id(args.engine, args.mode, args.query, args.profile, args.concurrency, args.run_number)
    output_root = ROOT / "results" / args.mode
    if args.output_root:
        output_root = (ROOT / args.output_root).resolve() if not Path(args.output_root).is_absolute() else Path(args.output_root)
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    for child in ("telemetry", "engine_logs", "host_events"):
        (run_dir / child).mkdir()
    args.run_id = run_id
    # A smoke must allow at least one real query to finish inside its measurement
    # window; large JSON aggregates can take tens of seconds under concurrency.
    args.warmup_seconds, args.measurement_seconds, args.cooldown_seconds = (2, 60, 2) if args.mode == "smoke" else (60, 300, 60)
    args.drain_seconds = 120
    events: list[dict[str, Any]] = []
    state_path = run_dir / "host_events" / "before.json"
    preflight_path = run_dir / "preflight.json"
    manifest = {
        "schema_version": 1, "run_id": run_id, "attempt_id": uuid.uuid4().hex,
        "created_at_utc": now_utc(), "mode": args.mode, "engine": args.engine,
        "profile": args.profile, "query_id": args.query, "concurrency": args.concurrency,
        "run_number": args.run_number, "dataset_manifest": "data/generated/events.jsonl.manifest.json",
        "dataset": json.loads(DATASET_MANIFEST.read_text(encoding="utf-8")),
        "image_lock": json.loads((ROOT / "config/images.lock.json").read_text(encoding="utf-8")),
        "query_file": str(query_path(args.engine, args.query).relative_to(ROOT)),
        "query_sha256": sha256_file(query_path(args.engine, args.query)),
        "query_treatment": query_options(args.engine, args.query),
        "schedule": {"warmup_seconds": args.warmup_seconds, "measurement_seconds": args.measurement_seconds, "cooldown_seconds": args.cooldown_seconds, "max_drain_seconds": args.drain_seconds},
        "percentile_method": "nearest-rank; one-based rank ceil(p*n)",
        "cache_protocol": "alternative_no_host_drop_caches",
        "environment": {
            "python": sys.version, "runner_version": "0.1.0", "git_commit": None,
            "pymongo": importlib.metadata.version("pymongo"),
            "psycopg": importlib.metadata.version("psycopg"),
        },
        "code_sha256": {
            str(path.relative_to(ROOT)): sha256_file(path)
            for path in (
                ROOT / "benchmark_runner/cli.py", ROOT / "benchmark_runner/runner.py",
                ROOT / "benchmark_runner/drivers.py", ROOT / "benchmark_runner/metrics.py",
                ROOT / "telemetry/collector.py", ROOT / "telemetry/evidence.py",
                ROOT / "scripts/preflight.py",
            )
        },
        "plan_reference_for_m3": plan_work_reference(args.engine, args.query),
        "client_effects": ["one dedicated native connection per worker", "warmup queries excluded", "buffered request JSONL serialized after execution; memory use scales with request count"],
    }
    try:
        _write_json(run_dir / "manifest.started.json", manifest)
        _assert_single_engine_active(args.engine)
        _preflight(args.engine, args.profile, preflight_path)
        container = f"u2-benchmark-{args.engine}-1"
        before = evidence.inspect_container(container)
        if before["status"] != "running":
            raise RunFailure(f"motor {args.engine} no está running")
        _write_json(state_path, {"schema_version": 1, "run_id": run_id, "engine": args.engine, "container_name": container, "started_epoch": time.time(), "started_at_utc": now_utc(), "before": before})
        env = merged_environment(ROOT / ".env")
        window: dict[str, Any] = {}
        result = _run_queries(args, env, run_id, window, events)
    except Exception as exc:
        _write_failed_artifacts(run_dir, args, run_id, f"{type(exc).__name__}: {exc}")
        initial_manifest = json.loads((run_dir / "manifest.started.json").read_text(encoding="utf-8"))
        initial_manifest.update({"completed_at_utc": now_utc(), "summary_status": "error", "failure": {"type": type(exc).__name__, "message": str(exc)[:1500]}})
        _write_json(run_dir / "manifest.json", initial_manifest)
        print(f"run {run_id} falló antes de completar medición: {type(exc).__name__}: {exc}", file=sys.stderr)
        return run_dir

    after = evidence.inspect_container(container)
    finish_capture_epoch = time.time()
    logs = evidence.run(["docker", "logs", "--timestamps", "--since", json.loads(state_path.read_text())["started_at_utc"], "--until", evidence.iso(finish_capture_epoch), container])
    docker_events = evidence.run(["docker", "events", "--since", json.loads(state_path.read_text())["started_at_utc"], "--until", evidence.iso(finish_capture_epoch), "--filter", f"container={before['container_id']}", "--format", "{{json .}}"])
    log_text = logs.stdout + logs.stderr
    (run_dir / "engine_logs" / "docker.log").write_text(log_text, encoding="utf-8")
    event_rows = evidence.parse_docker_events(docker_events.stdout + docker_events.stderr)
    (run_dir / "host_events" / "docker-events.jsonl").write_text("\n".join(json.dumps(row, sort_keys=True) for row in event_rows) + ("\n" if event_rows else ""), encoding="utf-8")
    spill_start = window.get("start_epoch")
    spill_end = window.get("end_epoch")
    spill_outside_measurement_window = None
    if not window.get("valid"):
        # Warmup/drain spill is diagnostic evidence, not the official window metric.
        if args.engine == "mongodb":
            spill_outside_measurement_window = evidence.classify_mongodb_spill(log_text, run_id)
        else:
            spill_outside_measurement_window = evidence.classify_postgresql_spill(
                log_text, run_id,
                log_temp_files=(result["engine_parameters"] or {}).get("log_temp_files"),
            )
        spill = {
            "status": "unknown", "run_id": run_id,
            "basis": "no_valid_measurement_window; spill is not assigned to the official CSV",
            "logs_complete": logs.returncode == 0,
        }
    elif args.engine == "mongodb":
        spill = evidence.classify_mongodb_spill(
            log_text, run_id, start_epoch=spill_start, end_epoch=spill_end,
            completed_queries=result["outcomes"]["measurement_successes"],
            logs_complete=logs.returncode == 0 and bool(window.get("valid")),
        )
    else:
        spill = evidence.classify_postgresql_spill(
            log_text, run_id, start_epoch=spill_start, end_epoch=spill_end,
            completed_queries=result["outcomes"]["measurement_successes"],
            log_temp_files=(result["engine_parameters"] or {}).get("log_temp_files"),
            logs_complete=logs.returncode == 0 and bool(window.get("valid")),
        )
    oom = evidence.classify_oom(before, after, event_rows)
    _write_json(run_dir / "host_events" / "evidence.json", {"spill_evidence": spill, "spill_outside_measurement_window": spill_outside_measurement_window, "oom_evidence": oom, "docker_logs_exit_code": logs.returncode, "docker_events_exit_code": docker_events.returncode})

    metrics_args = type("Args", (), {
        "prometheus_url": args.prometheus_url, "run_id": run_id, "engine": args.engine,
        "container_id": before["container_id"], "start": max(0, float(window.get("start_epoch", 0)) - 2),
        "end": float(window.get("end_epoch", 0)), "step": 1.0, "timeout": 15.0,
    })()
    try:
        metrics_report, metrics_rc = collector.collect(metrics_args)
        _write_json(run_dir / "telemetry" / "metrics.json", metrics_report)
    except Exception as exc:
        metrics_report = {"status": "error", "error": {"type": type(exc).__name__, "message": str(exc)}}
        metrics_rc = 1
        _write_json(run_dir / "telemetry" / "metrics.json", metrics_report)

    measurement_latencies = result["latencies_ms"]
    errors_exist = bool(result["errors"]) or result["outcomes"]["measurement_failures"] > 0 or any(row["alive_after_drain"] for row in result["worker_threads"])
    metrics_ok = metrics_rc == 0 and metrics_report.get("status") == "ok"
    spill_state = spill.get("status", "unknown")
    oom_state = oom.get("status", "unknown")
    status = classify_run_status(oom=oom_state, infrastructure_error=errors_exist or not metrics_ok or logs.returncode != 0 or docker_events.returncode != 0, spill=spill_state)
    disk_spill = True if spill_state == "positive" else False if spill_state == "negative_comprobado" else None
    summaries = metrics_report.get("summaries", {})
    throttled_summary = summaries.get("container_throttled_seconds_total", {})
    throttled = throttled_summary.get("delta") if not throttled_summary.get("reset_detected") else None
    max_rss = metrics_report.get("max_rss_bytes")
    row = csv_values(
        timestamp_epoch=window.get("start_epoch"), engine=args.engine, profile=args.profile,
        query_id=args.query, concurrency=args.concurrency, run_number=args.run_number,
        measurement_valid=bool(window.get("valid")),
        measurement_seconds=(float(window.get("end_monotonic", 0)) - float(window.get("start_monotonic", 0))) if window.get("valid") else None,
        latencies_ms=measurement_latencies, cpu_throttled_seconds=throttled,
        max_rss_bytes=max_rss, disk_spill=disk_spill, status=status,
    )
    write_requests(run_dir / "requests.jsonl", events)
    write_summary(run_dir / "summary.csv", CSV_HEADER, row)
    completed = result["outcomes"]["measurement_successes"]
    docs_per_query = manifest["plan_reference_for_m3"].get("documents_per_query")
    detections_per_query = manifest["plan_reference_for_m3"].get("detections_per_query")
    cpu_seconds = summaries.get("container_cpu_seconds_total", {}).get("delta")
    _write_json(run_dir / "auxiliary.json", {
        "cpu_consumed_seconds": cpu_seconds,
        "queries_completed_in_measurement": completed,
        "documents_processed_estimate": None if docs_per_query is None else docs_per_query * completed,
        "detections_processed_estimate": None if detections_per_query is None else detections_per_query * completed,
        "plan_basis": manifest["plan_reference_for_m3"],
        "cpu_seconds_per_1000_documents_estimate": None if not docs_per_query or not completed or cpu_seconds is None else cpu_seconds / (docs_per_query * completed / 1000),
        "rss_normalization": None,
        "note": "M3 sidecar estimate uses separately collected phase 04 plan counts; CPU remains a measured cgroup delta. This sidecar does not alter the fixed CSV.",
    })
    quality = {
        "schema_version": 1, "run_id": run_id, "status": status,
        "measurement_valid": bool(window.get("valid")),
        "telemetry_complete": metrics_ok,
        "window": {key: window.get(key) for key in ("start_epoch", "end_epoch", "start_monotonic", "end_monotonic", "valid")},
        "duration_seconds": None if not window.get("valid") else float(window["end_monotonic"]) - float(window["start_monotonic"]),
        "counts": result["outcomes"], "pending_at_measurement_close": result["pending_at_window_close"],
        "client_concurrency": result["client_concurrency"], "worker_threads": result["worker_threads"],
        "errors": result["errors"], "spill": spill, "oom": oom,
        "telemetry_status": metrics_report.get("status"),
        "telemetry_quality": metrics_report.get("quality", metrics_report.get("error")),
        "telemetry_missing_is_not_zero": True,
        "measurement_counting_rule": "full results consumed and completion timestamp inside fixed measurement window",
        "drain_seconds_max": args.drain_seconds,
        "cooldown_seconds": args.cooldown_seconds,
        "status_precedence": ["oom", "error", "spill", "ok"],
    }
    _write_json(run_dir / "quality.json", quality)
    manifest.update({
        "completed_at_utc": now_utc(), "server_version": result["server_version"],
        "effective_engine_parameters": result["engine_parameters"],
        "container_id": before["container_id"], "container_image": before["image"],
        "effective_limits": json.loads(preflight_path.read_text(encoding="utf-8")),
        "measurement_window": quality["window"],
        "cache_protocol_result": "not_applied; no host cache operation was run by the runner",
        "request_event_count": len(events), "summary_status": status,
        "telemetry_file": "telemetry/metrics.json", "evidence_file": "host_events/evidence.json",
    })
    _write_json(run_dir / "manifest.json", manifest)
    print(json.dumps({"run_id": run_id, "status": status, "measurement_valid": quality["measurement_valid"], "summary_csv": str(run_dir / "summary.csv"), "client_concurrency": result["client_concurrency"]}, sort_keys=True))
    return run_dir
