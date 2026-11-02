#!/usr/bin/env python3
"""Capture cgroup OOM counters and engine log evidence around one run window."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def epoch_now() -> float:
    from time import time
    return time()


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def run(command: list[str], *, timeout: int = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)


def inspect_container(name: str) -> dict[str, Any]:
    result = run(["docker", "inspect", name])
    if result.returncode:
        raise RuntimeError(f"docker inspect {name}: {result.stderr.strip()}")
    doc = json.loads(result.stdout)[0]
    state = doc.get("State", {})
    pid = int(state.get("Pid") or 0)
    cgroup_path = None
    memory_events = None
    cgroup_error = None
    if pid:
        try:
            lines = Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").splitlines()
            unified = next(line.split(":", 2)[2] for line in lines if line.startswith("0::"))
            cgroup_path = unified
            events_file = Path("/sys/fs/cgroup") / unified.lstrip("/") / "memory.events"
            values = {}
            for line in events_file.read_text(encoding="utf-8").splitlines():
                key, value = line.split()
                values[key] = int(value)
            memory_events = values
        except (OSError, StopIteration, ValueError) as exc:
            cgroup_error = f"{type(exc).__name__}: {exc}"
    return {
        "name": doc.get("Name", "").lstrip("/"),
        "container_id": doc.get("Id"),
        "image_id": doc.get("Image"),
        "image": doc.get("Config", {}).get("Image"),
        "pid": pid,
        "started_at": state.get("StartedAt"),
        "status": state.get("Status"),
        "oom_killed": state.get("OOMKilled"),
        "cgroup_path": cgroup_path,
        "memory_events": memory_events,
        "cgroup_error": cgroup_error,
    }


def parse_docker_events(raw: str) -> list[dict[str, Any]]:
    result = []
    for line in raw.splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return result


def classify_oom(before: dict[str, Any], after: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    same_container = before.get("container_id") == after.get("container_id")
    same_start = before.get("started_at") == after.get("started_at")
    same_cgroup = before.get("cgroup_path") == after.get("cgroup_path")
    initial = before.get("memory_events")
    final = after.get("memory_events")
    oom_events = [event for event in events if str(event.get("Action", event.get("status", ""))).lower() in {"oom", "oom_kill", "oomkill"}]
    deltas = None
    if same_container and initial is not None and final is not None:
        deltas = {key: final.get(key, 0) - initial.get(key, 0) for key in sorted(set(initial) | set(final))}
    counter_reset = bool(deltas and any(value < 0 for value in deltas.values()))
    positive = bool(oom_events) or bool(deltas and (deltas.get("oom", 0) > 0 or deltas.get("oom_kill", 0) > 0))
    negative = same_container and same_start and same_cgroup and deltas is not None and not counter_reset and not positive and after.get("oom_killed") is False
    return {
        "status": "positive" if positive else "negative_comprobado" if negative else "unknown",
        "same_container_identity": same_container,
        "same_container_start": same_start,
        "same_cgroup_identity": same_cgroup,
        "counter_reset_detected": counter_reset,
        "memory_events_delta": deltas,
        "docker_oom_events": oom_events,
        "container_oom_killed_at_end": after.get("oom_killed"),
    }


def _log_line_in_window(line: str, start_epoch: float | None, end_epoch: float | None) -> bool:
    if start_epoch is None and end_epoch is None:
        return True
    try:
        stamp = datetime.fromisoformat(line.split(" ", 1)[0].replace("Z", "+00:00")).timestamp()
    except (ValueError, IndexError):
        return False
    return (start_epoch is None or stamp >= start_epoch) and (end_epoch is None or stamp <= end_epoch)


def classify_mongodb_spill(
    logs: str,
    run_id: str,
    *,
    start_epoch: float | None = None,
    end_epoch: float | None = None,
    completed_queries: int | None = None,
    logs_complete: bool = False,
) -> dict[str, Any]:
    records = []
    completed_with_spill_disabled = []
    completed_without_spill_indicator = []
    for line in logs.splitlines():
        if not _log_line_in_window(line, start_epoch, end_epoch):
            continue
        object_start = line.find("{")
        if object_start < 0:
            continue
        raw = line[object_start:]
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            continue
        attr = record.get("attr", {})
        command = attr.get("command", {}) if isinstance(attr, dict) else {}
        comment = command.get("comment", attr.get("comment")) if isinstance(command, dict) else attr.get("comment")
        if comment == run_id and isinstance(attr, dict):
            if record.get("msg") == "Slow query" and isinstance(command, dict):
                evidence_record = {
                    "timestamp": record.get("t", {}).get("$date"),
                    "message": record.get("msg"),
                    "allowDiskUse": command.get("allowDiskUse"),
                    "durationMillis": attr.get("durationMillis"),
                    "usedDisk": attr.get("usedDisk"),
                    "peakTrackedMemBytes": attr.get("peakTrackedMemBytes"),
                }
                if isinstance(attr.get("usedDisk"), bool):
                    records.append(evidence_record)
                elif command.get("allowDiskUse") is False and isinstance(attr.get("durationMillis"), (int, float)):
                    # An explicitly no-disk aggregate completed; MongoDB cannot spill it.
                    completed_with_spill_disabled.append(evidence_record)
                else:
                    # MongoDB slow-query logs report usedDisk when a stage spilled.
                    # Absence is negative only when complete logs account for every
                    # successful operation in the measured window.
                    completed_without_spill_indicator.append(evidence_record)
    states = {record["usedDisk"] for record in records}
    complete_no_spill_coverage = (
        logs_complete
        and completed_queries is not None
        and completed_queries > 0
        and len(completed_without_spill_indicator) + len(completed_with_spill_disabled) + len(records) == completed_queries
        and not any(record.get("usedDisk") is True for record in records)
    )
    if True in states:
        status = "positive"
    elif complete_no_spill_coverage:
        status = "negative_comprobado"
    elif completed_queries is None and ((records and states == {False}) or completed_with_spill_disabled):
        # Backward-compatible path for diagnostic callers without an expected count.
        status = "negative_comprobado"
    else:
        status = "unknown"
    return {
        "status": status,
        "run_id": run_id,
        "matching_log_records": records,
        "completed_with_spill_disabled": completed_with_spill_disabled,
        "completed_without_spill_indicator": completed_without_spill_indicator,
        "completed_queries_expected": completed_queries,
        "completed_queries_logged": len(completed_without_spill_indicator) + len(completed_with_spill_disabled) + len(records),
        "logs_complete": logs_complete,
        "evidence_basis": "complete_run_scoped_slow_query_logs_without_usedDisk_indicator" if complete_no_spill_coverage else None,
    }


def classify_postgresql_spill(
    logs: str,
    run_id: str,
    *,
    start_epoch: float | None = None,
    end_epoch: float | None = None,
    completed_queries: int | None = None,
    log_temp_files: str | None = None,
    logs_complete: bool = False,
) -> dict[str, Any]:
    matching = [line for line in logs.splitlines() if f"app={run_id}" in line and _log_line_in_window(line, start_epoch, end_epoch)]
    temp_files = [line for line in matching if "temporary file:" in line.lower()]
    if temp_files:
        status = "positive"
        basis = "run_scoped_temporary_file_log"
    elif matching:
        status = "negative_comprobado"
        basis = "run_scoped_logs_without_temporary_file"
    elif logs_complete and str(log_temp_files).strip() == "0" and (completed_queries or 0) > 0:
        # PostgreSQL emits a log entry for every temp file when log_temp_files=0.
        # With complete container logs, a valid window and successful completed queries,
        # absence of such entries is evidence of no temp-file spill even if no statement
        # log line exists for a fast query.
        status = "negative_comprobado"
        basis = "complete_window_logs_log_temp_files_zero_and_completed_queries"
    else:
        status = "unknown"
        basis = "insufficient_run_scoped_or_complete_logging_evidence"
    return {
        "status": status, "run_id": run_id, "matching_log_lines": matching,
        "temporary_file_log_lines": temp_files, "basis": basis,
        "completed_queries": completed_queries, "effective_log_temp_files": log_temp_files,
        "logs_complete": logs_complete,
    }


def begin(args: argparse.Namespace) -> int:
    output = args.output if args.output.is_absolute() else ROOT / args.output
    if output.exists():
        raise FileExistsError(f"No se sobrescribe el snapshot: {output}")
    before = inspect_container(args.container)
    if before["status"] != "running":
        raise RuntimeError(f"El contenedor {args.container} no está en ejecución")
    started = epoch_now()
    report = {
        "schema_version": 1,
        "run_id": args.run_id,
        "engine": args.engine,
        "container_name": args.container,
        "started_epoch": started,
        "started_at_utc": iso(started),
        "before": before,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "snapshot_saved", "run_id": args.run_id, "container_id": before["container_id"], "cgroup_counters_available": before["memory_events"] is not None, "output": str(output)}, sort_keys=True))
    return 0


def end(args: argparse.Namespace) -> int:
    state_path = args.state if args.state.is_absolute() else ROOT / args.state
    output = args.output if args.output.is_absolute() else ROOT / args.output
    log_output = args.engine_logs if args.engine_logs.is_absolute() else ROOT / args.engine_logs
    events_output = args.docker_events if args.docker_events.is_absolute() else ROOT / args.docker_events
    if output.exists() or log_output.exists() or events_output.exists():
        raise FileExistsError("No se sobrescribe evidencia de telemetría existente")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    after = inspect_container(state["container_name"])
    finished = epoch_now()
    since = state["started_at_utc"]
    until = iso(finished)
    logs_result = run(["docker", "logs", "--timestamps", "--since", since, "--until", until, state["container_name"]])
    events_result = run(["docker", "events", "--since", since, "--until", until, "--filter", f"container={state['before']['container_id']}", "--format", "{{json .}}"])
    logs = logs_result.stdout + logs_result.stderr
    events_raw = events_result.stdout + events_result.stderr
    event_rows = parse_docker_events(events_raw)
    spill_start = getattr(args, "spill_start_epoch", None)
    spill_end = getattr(args, "spill_end_epoch", None)
    if args.engine == "mongodb":
        spill = classify_mongodb_spill(logs, state["run_id"], start_epoch=spill_start, end_epoch=spill_end)
    else:
        spill = classify_postgresql_spill(logs, state["run_id"], start_epoch=spill_start, end_epoch=spill_end)
    oom = classify_oom(state["before"], after, event_rows)
    log_output.parent.mkdir(parents=True, exist_ok=True)
    events_output.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    log_output.write_text(logs, encoding="utf-8")
    events_output.write_text("\n".join(json.dumps(item, ensure_ascii=False, sort_keys=True) for item in event_rows) + ("\n" if event_rows else ""), encoding="utf-8")
    report = {
        "schema_version": 1,
        "run_id": state["run_id"],
        "engine": args.engine,
        "container_name": state["container_name"],
        "container_id_before": state["before"]["container_id"],
        "container_id_after": after["container_id"],
        "started_at_utc": state["started_at_utc"],
        "finished_at_utc": until,
        "duration_seconds": finished - state["started_epoch"],
        "spill_evidence": spill,
        "oom_evidence": oom,
        "engine_logs": {"path": str(log_output.relative_to(ROOT)) if log_output.is_relative_to(ROOT) else str(log_output), "sha256": hashlib.sha256(logs.encode()).hexdigest(), "docker_logs_exit_code": logs_result.returncode},
        "docker_events": {"path": str(events_output.relative_to(ROOT)) if events_output.is_relative_to(ROOT) else str(events_output), "docker_events_exit_code": events_result.returncode, "event_count": len(event_rows)},
        "after": after,
    }
    report["status"] = "ok" if oom["status"] != "unknown" and spill["status"] != "unknown" and logs_result.returncode == 0 else "error"
    output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "run_id": state["run_id"], "spill": spill["status"], "oom": oom["status"], "engine_logs": str(log_output), "output": str(output)}, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "ok" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("begin", "end"):
        part = sub.add_parser(name)
        part.add_argument("--run-id", required=name == "begin")
        part.add_argument("--engine", choices=("mongodb", "postgresql"), required=name == "begin")
        part.add_argument("--container", required=name == "begin")
        if name == "begin":
            part.add_argument("--output", type=Path, required=True)
        else:
            part.add_argument("--state", type=Path, required=True)
            part.add_argument("--output", type=Path, required=True)
            part.add_argument("--engine-logs", type=Path, required=True)
            part.add_argument("--docker-events", type=Path, required=True)
            part.add_argument("--spill-start-epoch", type=float)
            part.add_argument("--spill-end-epoch", type=float)
    args = parser.parse_args()
    try:
        return begin(args) if args.command == "begin" else end(args)
    except (FileExistsError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(f"Captura de evidencia detenida: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
