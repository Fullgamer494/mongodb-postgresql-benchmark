#!/usr/bin/env python3
"""Collect raw Prometheus series and window quality for one engine container."""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]


def utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def engine_promql(engine: str, container_id: str) -> dict[str, dict[str, Any]]:
    service = engine
    container = f'id=~".*{container_id}.*",container_label_com_docker_compose_service="{service}"'
    return {
        "container_throttled_seconds_total": {"query": f"container_cpu_cfs_throttled_seconds_total{{{container}}}", "kind": "counter", "required": True},
        "container_rss_bytes": {"query": f"container_memory_rss{{{container}}}", "kind": "gauge", "required": True},
        "container_cpu_seconds_total": {"query": f'container_cpu_usage_seconds_total{{{container},cpu="total"}}', "kind": "counter", "required": True},
        "container_fs_reads_bytes_total": {"query": f"container_fs_reads_bytes_total{{{container}}}", "kind": "counter", "required": False},
        "container_fs_writes_bytes_total": {"query": f"container_fs_writes_bytes_total{{{container}}}", "kind": "counter", "required": False},
        "container_oom_events_total": {"query": f"container_oom_events_total{{{container}}}", "kind": "counter", "required": True},
        "container_start_time_seconds": {"query": f"container_start_time_seconds{{{container}}}", "kind": "gauge", "required": True},
        "host_cpu_seconds_total": {"query": 'node_cpu_seconds_total{mode!="idle"}', "kind": "counter", "required": True},
        "host_disk_read_bytes_total": {"query": "node_disk_read_bytes_total", "kind": "counter", "required": True},
        "host_disk_written_bytes_total": {"query": "node_disk_written_bytes_total", "kind": "counter", "required": True},
        "host_swap_total_bytes": {"query": "node_memory_SwapTotal_bytes", "kind": "gauge", "required": True},
        "host_swap_free_bytes": {"query": "node_memory_SwapFree_bytes", "kind": "gauge", "required": True},
        "host_cpu_pressure_waiting_seconds_total": {"query": "node_pressure_cpu_waiting_seconds_total", "kind": "counter", "required": True},
        "host_memory_pressure_waiting_seconds_total": {"query": "node_pressure_memory_waiting_seconds_total", "kind": "counter", "required": True},
        "cadvisor_up": {"query": 'up{job="cadvisor"}', "kind": "gauge", "required": True},
        "node_exporter_up": {"query": 'up{job="node-exporter"}', "kind": "gauge", "required": True},
    }


def parse_matrix(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("status") != "success":
        raise ValueError(f"Prometheus respondió status={payload.get('status')!r}: {payload.get('error', 'sin detalle')}")
    data = payload.get("data", {})
    if data.get("resultType") != "matrix":
        raise ValueError(f"Prometheus devolvió resultType={data.get('resultType')!r}; se esperaba matrix")
    result = []
    for series in data.get("result", []):
        result.append({
            "labels": series.get("metric", {}),
            "values": [{"timestamp": float(stamp), "value": str(value)} for stamp, value in series.get("values", [])],
        })
    return result


def counter_delta(values: list[dict[str, Any]]) -> dict[str, Any]:
    if len(values) < 2:
        return {"initial_value": values[0]["value"] if values else None, "final_value": values[-1]["value"] if values else None, "delta": None, "reset_count": 0, "reason": "fewer_than_two_samples"}
    parsed = [float(item["value"]) for item in values]
    resets = sum(1 for prior, current in zip(parsed, parsed[1:]) if current < prior)
    if resets:
        return {"initial_value": values[0]["value"], "final_value": values[-1]["value"], "delta": None, "reset_count": resets, "reason": "counter_reset"}
    return {"initial_value": values[0]["value"], "final_value": values[-1]["value"], "delta": parsed[-1] - parsed[0], "reset_count": 0, "reason": None}


def summarize_metric(series: list[dict[str, Any]], spec: dict[str, Any], start: float, end: float, step: float) -> dict[str, Any]:
    expected = math.floor((end - start) / step) + 1
    counts = [len(item["values"]) for item in series]
    coverage_by_series = []
    for item in series:
        values = item["values"]
        stamps = [point["timestamp"] for point in values]
        coverage_by_series.append({
            "labels": item["labels"],
            "samples": len(values),
            "expected_samples": expected,
            "coverage_ratio": min(1.0, len(values) / expected) if expected else 0.0,
            "first_sample_utc": utc(stamps[0]) if stamps else None,
            "last_sample_utc": utc(stamps[-1]) if stamps else None,
            "starts_within_step": bool(stamps and stamps[0] <= start + step * 2),
            "ends_within_step": bool(stamps and stamps[-1] >= end - step * 2),
        })
    item: dict[str, Any] = {
        "required": spec["required"],
        "series_count": len(series),
        "samples_expected_per_series": expected,
        "coverage_by_series": coverage_by_series,
        "available": bool(series),
    }
    if spec["kind"] == "counter":
        deltas = []
        for row in series:
            delta = counter_delta(row["values"])
            deltas.append({"labels": row["labels"], **delta})
        complete = bool(deltas) and all(value["delta"] is not None for value in deltas)
        item["counter_series_deltas"] = deltas
        item["delta"] = sum(value["delta"] for value in deltas) if complete else None
        item["reset_detected"] = any(value["reset_count"] for value in deltas)
        item["delta_reason"] = None if complete else "one_or_more_series_missing_samples_or_reset"
    else:
        all_values = [float(point["value"]) for row in series for point in row["values"]]
        item["max"] = max(all_values) if all_values else None
        item["first_values"] = [row["values"][0]["value"] if row["values"] else None for row in series]
        item["last_values"] = [row["values"][-1]["value"] if row["values"] else None for row in series]
    item["coverage_complete"] = bool(coverage_by_series) and all(
        point["coverage_ratio"] >= 0.8 and point["starts_within_step"] and point["ends_within_step"]
        for point in coverage_by_series
    )
    return item


def query_range(base_url: str, promql: str, start: float, end: float, step: float, timeout: float) -> tuple[list[dict[str, Any]], str | None]:
    params = urllib.parse.urlencode({"query": promql, "start": str(start), "end": str(end), "step": str(step)})
    request = urllib.request.Request(f"{base_url.rstrip('/')}/api/v1/query_range?{params}", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return parse_matrix(json.load(response)), None
    except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as exc:
        return [], f"{type(exc).__name__}: {exc}"


def collect(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    parsed = urlparse(args.prometheus_url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Prometheus debe consultarse por HTTP local (127.0.0.1/localhost/::1)")
    if not (args.start < args.end) or args.step <= 0:
        raise ValueError("Se requiere start < end y step > 0")
    specs = engine_promql(args.engine, args.container_id)
    series_by_metric: dict[str, list[dict[str, Any]]] = {}
    queries: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for key, spec in specs.items():
        rows, error = query_range(args.prometheus_url, spec["query"], args.start, args.end, args.step, args.timeout)
        queries[key] = {"promql": spec["query"], "range_start_utc": utc(args.start), "range_end_utc": utc(args.end), "step_seconds": args.step}
        series_by_metric[key] = rows
        if error:
            errors[key] = error
    summaries = {key: summarize_metric(series_by_metric[key], specs[key], args.start, args.end, args.step) for key in specs}
    missing = [key for key, spec in specs.items() if spec["required"] and not summaries[key]["available"]]
    bad_coverage = [key for key, spec in specs.items() if spec["required"] and not summaries[key]["coverage_complete"]]
    invalid_counters = [
        key for key, spec in specs.items()
        if spec["required"] and spec["kind"] == "counter"
        and any(item["delta"] is None for item in summaries[key].get("counter_series_deltas", []))
    ]
    start_time_rows = series_by_metric["container_start_time_seconds"]
    observed_start_times = {
        value["value"] for row in start_time_rows for value in row["values"]
    }
    engine_restart_detected = len(observed_start_times) > 1
    engine_series = series_by_metric["container_rss_bytes"]
    wrong_target = [row["labels"] for row in engine_series if row["labels"].get("container_label_com_docker_compose_service") != args.engine or args.container_id not in row["labels"].get("id", "")]
    if wrong_target:
        errors["target_identity"] = "cAdvisor devolvió una serie que no corresponde al ID de contenedor y servicio solicitados"
    if engine_restart_detected:
        errors["engine_restart"] = "container_start_time_seconds cambió dentro de la ventana"
    if invalid_counters:
        errors["invalid_counter_delta"] = "Una o más series de contadores requeridas tienen reset o menos de dos muestras: " + ", ".join(invalid_counters)
    rss_values = [float(point["value"]) for row in engine_series for point in row["values"]]
    report = {
        "schema_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_id": args.run_id,
        "engine": args.engine,
        "container_id": args.container_id,
        "prometheus_url": args.prometheus_url,
        "window": {"start_epoch": args.start, "start_utc": utc(args.start), "end_epoch": args.end, "end_utc": utc(args.end), "step_seconds": args.step},
        "queries": queries,
        "series": series_by_metric,
        "summaries": summaries,
        "max_rss_bytes": max(rss_values) if rss_values else None,
        "max_rss_mib": max(rss_values) / (1024 * 1024) if rss_values else None,
        "quality": {
            "missing_required_metrics": missing,
            "incomplete_coverage_metrics": bad_coverage,
            "invalid_counter_delta_metrics": invalid_counters,
            "engine_restart_detected": engine_restart_detected,
            "engine_start_time_values": sorted(observed_start_times),
            "errors": errors,
        },
        "status": "ok" if not missing and not bad_coverage and not errors else "error",
    }
    return report, 0 if report["status"] == "ok" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    parser.add_argument("--container-id", required=True)
    parser.add_argument("--start", required=True, type=float, help="Inicio epoch UTC de la ventana")
    parser.add_argument("--end", required=True, type=float, help="Fin epoch UTC de la ventana")
    parser.add_argument("--step", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    if output.exists():
        parser.error(f"No se sobrescribe evidencia: {output}")
    try:
        report, status = collect(args)
    except Exception as exc:
        report = {"schema_version": 1, "run_id": args.run_id, "engine": args.engine, "status": "error", "error": {"type": type(exc).__name__, "message": str(exc)}}
        status = 1
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"run_id": args.run_id, "engine": args.engine, "status": report["status"], "output": str(output), "quality": report.get("quality", report.get("error"))}, ensure_ascii=False, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
