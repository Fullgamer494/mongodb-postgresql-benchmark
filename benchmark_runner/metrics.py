"""Pure result calculations and exact fixed CSV serialization."""

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any, Iterable


def nearest_rank(values: Iterable[float], percentile: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if not 0 < percentile <= 1:
        raise ValueError("percentile debe estar en (0, 1]")
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def completed_inside_window(start: float, finish: float, window_start: float, window_end: float) -> bool:
    return start >= window_start and finish <= window_end


def classify_run_status(*, oom: str, infrastructure_error: bool, spill: str) -> str:
    if oom == "positive":
        return "oom"
    if infrastructure_error or oom == "unknown" or spill == "unknown":
        return "error"
    if spill == "positive":
        return "spill"
    if spill == "negative_comprobado" and oom == "negative_comprobado":
        return "ok"
    return "error"


def csv_values(
    *, timestamp_epoch: float | None,
    engine: str,
    profile: str,
    query_id: str,
    concurrency: int,
    run_number: int,
    measurement_valid: bool,
    measurement_seconds: float | None,
    latencies_ms: list[float],
    cpu_throttled_seconds: float | None,
    max_rss_bytes: float | None,
    disk_spill: bool | None,
    status: str,
) -> dict[str, Any]:
    seconds = measurement_seconds if measurement_valid else None
    successes = len(latencies_ms)
    return {
        "timestamp_epoch": "" if timestamp_epoch is None else f"{timestamp_epoch:.6f}",
        "engine": engine,
        "profile": profile,
        "query_id": query_id,
        "concurrency": concurrency,
        "run_number": run_number,
        "throughput_qps": "" if seconds is None else f"{successes / seconds:.9f}",
        "latency_p50_ms": _number(nearest_rank(latencies_ms, 0.50)),
        "latency_p95_ms": _number(nearest_rank(latencies_ms, 0.95)),
        "latency_p99_ms": _number(nearest_rank(latencies_ms, 0.99)),
        "cpu_throttled_seconds": _number(cpu_throttled_seconds),
        "max_rss_mb": "" if max_rss_bytes is None else f"{max_rss_bytes / (1024 * 1024):.6f}",
        "disk_spill": "" if disk_spill is None else str(disk_spill).lower(),
        "status": status,
    }


def _number(value: float | None) -> str:
    return "" if value is None else f"{value:.6f}"


def write_summary(path: Path, header: list[str], row: dict[str, Any]) -> None:
    if list(row) != header:
        raise ValueError(f"La fila CSV no coincide con el contrato: {list(row)!r}")
    if path.exists():
        raise FileExistsError(f"No se sobrescribe el resumen: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=header, extrasaction="raise", lineterminator="\n")
        writer.writeheader()
        writer.writerow(row)


def write_requests(path: Path, events: list[dict[str, Any]]) -> None:
    if path.exists():
        raise FileExistsError(f"No se sobrescriben eventos: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        for event in events:
            import json
            stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
