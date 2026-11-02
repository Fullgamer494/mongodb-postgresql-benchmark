#!/usr/bin/env python3
"""Unifica los 162 escenarios en un solo CSV conservando la procedencia.

Fuentes:
  * results/official-clean-20261101a/*/summary.csv   -> corridas reales
  * ../project-completado/results/completed/results.csv -> fixture offline
    (plantilla; ver su README.md: no son evidencia de los escenarios nuevos)

Regla: si un escenario tiene corrida real, se usa SIEMPRE el valor real
(incluidos error/oom/spill). Solo los escenarios sin corrida real usan la fila
fixture, marcada con data_origin=fixture_template y, cuando es posible, la
corrida real de la que se copiaron los valores (template_source_dir).

Salida (a nivel results/):
  results_unificado_162.csv        14 columnas contractuales + source_dir/template_source_dir
  results_unificado_162.audit.json auditoría de coherencia
"""
import csv
import glob
import json
import os
import statistics
import sys
from collections import Counter, defaultdict

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REAL_DIR = os.path.join(ROOT, "results", "official-clean-20261101a")
FIXTURE_CSV = os.path.abspath(os.path.join(
    ROOT, "..", "project-completado", "results", "completed", "results.csv"))
HEADER_CSV = os.path.join(ROOT, "contracts", "results_header.csv")
OUT_CSV = os.path.join(ROOT, "results", "results_unificado_162.csv")
OUT_AUDIT = os.path.join(ROOT, "results", "results_unificado_162.audit.json")
EXTRA = ["source_dir", "template_source_dir"]  # data_origin se usa internamente y en la auditoría, no se escribe en el CSV
NUM = ["throughput_qps", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
       "cpu_throttled_seconds", "max_rss_mb"]


def key(r):
    return (r["engine"], r["profile"], r["query_id"], r["concurrency"], r["run_number"])


def main():
    header = next(csv.reader(open(HEADER_CSV, newline="")))
    # results_header.csv puede venir con saltos de línea; se normaliza
    header = [h.strip() for h in ",".join(open(HEADER_CSV).read().split()).split(",") if h.strip()]

    real_by_key = defaultdict(list)
    incomplete = []
    for d in sorted(glob.glob(os.path.join(REAL_DIR, "*"))):
        f = os.path.join(d, "summary.csv")
        name = os.path.basename(d)
        if not os.path.exists(f):
            incomplete.append(name)
            continue
        for r in csv.DictReader(open(f, newline="")):
            r["_dir"] = name
            real_by_key[key(r)].append(r)

    fixture = list(csv.DictReader(open(FIXTURE_CSV, newline="")))
    assert len(fixture) == 162 and len({key(r) for r in fixture}) == 162
    assert set(real_by_key) <= {key(r) for r in fixture}, "real fuera del plan"

    # índice para rastrear de qué corrida real proviene una fila fixture
    real_rows = [r for v in real_by_key.values() for r in v]
    by_values = defaultdict(list)
    for r in real_rows:
        by_values[tuple(r[c] for c in header if c not in ("profile", "query_id", "concurrency", "run_number"))].append(r)

    out, audit_rep = [], {"real_overrode_fixture_values": [], "duplicate_real_attempts": {}}
    for fx in fixture:
        k = key(fx)
        row = {c: fx[c] for c in header}
        if k in real_by_key:
            attempts = sorted(real_by_key[k], key=lambda r: r["_dir"].split("-o-")[1])
            use = attempts[-1]  # último intento
            if len(attempts) > 1:
                audit_rep["duplicate_real_attempts"]["|".join(k)] = [
                    {"dir": a["_dir"], "status": a["status"]} for a in attempts]
            row = {c: use[c] for c in header}
            if any(use[c] != fx[c] for c in header):
                audit_rep["real_overrode_fixture_values"].append({
                    "scenario": "|".join(k), "real": {c: use[c] for c in ("throughput_qps", "status")},
                    "fixture": {c: fx[c] for c in ("throughput_qps", "status")}})
            row.update(data_origin="real", source_dir=use["_dir"], template_source_dir="")
        else:
            sig = tuple(fx[c] for c in header if c not in ("profile", "query_id", "concurrency", "run_number"))
            src = by_values.get(sig, [])
            row.update(data_origin="fixture_template", source_dir="",
                       template_source_dir=src[0]["_dir"] if src else "unknown")
        out.append(row)

    with open(OUT_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header + EXTRA, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(out)

    # ---- auditoría de coherencia ----
    real_out = [r for r in out if r["data_origin"] == "real"]
    fx_out = [r for r in out if r["data_origin"] == "fixture_template"]

    def src_row(r):
        for x in real_rows:
            if x["_dir"] == r["template_source_dir"]:
                return x

    mismatched, ts_reuse = Counter(), Counter(r["timestamp_epoch"] for r in fx_out if r["timestamp_epoch"])
    for r in fx_out:
        s = src_row(r)
        if not s:
            mismatched["sin_origen_identificado"] += 1
            continue
        for c in ("profile", "concurrency", "run_number", "query_id"):
            if s[c] != r[c]:
                mismatched[c] += 1

    def stats(rows, q, eng, prof, conc):
        v = [float(r["throughput_qps"]) for r in rows if r["throughput_qps"]
             and (r["query_id"], r["engine"], r["profile"], r["concurrency"]) == (q, eng, prof, conc)]
        return v

    # fixture dentro del rango de los reales del mismo (engine,query,profile,conc)
    outside, comparable = [], 0
    for r in fx_out:
        if not r["throughput_qps"]:
            continue
        ref = stats(real_out, r["query_id"], r["engine"], r["profile"], r["concurrency"])
        if len(ref) >= 1:
            comparable += 1
            t = float(r["throughput_qps"])
            if not (min(ref) * 0.8 <= t <= max(ref) * 1.2):
                outside.append({"scenario": "|".join(key(r)), "throughput_qps": t,
                                "real_range": [min(ref), max(ref)]})
    status_flag_bad = [
        "|".join(key(r)) for r in out
        if (r["status"] == "spill") != (r["disk_spill"] == "true") and r["status"] in ("spill", "ok")]
    audit = {
        "total_rows": len(out),
        "real_rows": len(real_out),
        "fixture_template_rows": len(fx_out),
        "real_status": dict(Counter(r["status"] for r in real_out)),
        "fixture_status": dict(Counter(r["status"] for r in fx_out)),
        "real_dirs_without_summary_(interrupted_attempts)": incomplete,
        "fixture_rows_copied_from_other_scenario": dict(mismatched),
        "fixture_rows_with_unidentified_source": mismatched.get("sin_origen_identificado", 0),
        "fixture_timestamps_reused_across_rows": sum(1 for v in ts_reuse.values() if v > 1),
        "fixture_rows_comparable_with_real_same_cell": comparable,
        "fixture_rows_outside_real_range_pm20pct": outside,
        "rows_with_spill_status_vs_disk_spill_mismatch": status_flag_bad,
        **audit_rep,
        "conclusion": (
            "Las filas fixture_template son copias de corridas reales de OTROS "
            "escenarios (otro perfil/concurrencia/repeticion) segun el README del "
            "fixture; no son evidencia de los escenarios nuevos y no deben "
            "reportarse como corridas reales."),
    }
    json.dump(audit, open(OUT_AUDIT, "w"), indent=2, ensure_ascii=False)
    print(json.dumps({k: v for k, v in audit.items()
                      if not isinstance(v, (list, dict)) or k.endswith("status")}, indent=1, ensure_ascii=False))
    print("fixture copiado de otro escenario:", dict(mismatched))
    print("override real vs fixture:", len(audit_rep["real_overrode_fixture_values"]))
    print("fuera de rango:", len(outside), "de", comparable)
    print("escrito:", OUT_CSV)


if __name__ == "__main__":
    sys.exit(main())
