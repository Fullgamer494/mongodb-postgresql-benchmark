"""Command-line interface for one concurrent benchmark cell."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from .drivers import query_path, treatment
from .runner import RESOURCE_PROFILES, ROOT, execute_run, new_run_id, plan_work_reference


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("mongodb", "postgresql"))
    parser.add_argument("--profile", required=True, choices=tuple(RESOURCE_PROFILES))
    parser.add_argument("--query", required=True, choices=("Q1", "Q2", "Q2_ALT"))
    parser.add_argument("--concurrency", required=True, type=int, choices=(1, 10, 50))
    parser.add_argument("--run-number", required=True, type=int, choices=(1, 2, 3))
    parser.add_argument("--mode", required=True, choices=("smoke", "pilot", "official"))
    parser.add_argument("--run-id", help="ID explícito (máximo 63 caracteres para conservarlo en application_name)")
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    parser.add_argument("--output-root", help="Raíz alternativa de resultados, relativa al proyecto o absoluta")
    parser.add_argument("--dry-run", action="store_true", help="Valida argumentos y muestra el plan; no conecta motores ni escribe resultados")
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.query == "Q2_ALT" and not treatment(args.engine, args.query):
        parser.error(f"No hay tratamiento Q2_ALT configurado para {args.engine}")
    if args.run_id and (len(args.run_id) > 63 or not re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id)):
        parser.error("run-id debe usar letras ASCII, números, guion/guion bajo y máximo 63 caracteres")
    query_file = query_path(args.engine, args.query)
    if not query_file.is_file():
        parser.error(f"no existe la consulta fijada: {query_file}")
    if args.prometheus_url != "http://127.0.0.1:9090":
        parser.error("el runner requiere Prometheus local en http://127.0.0.1:9090")
    if not (ROOT / "data/generated/events.jsonl.manifest.json").is_file():
        parser.error("falta el manifiesto del dataset oficial")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)
    run_id = args.run_id or new_run_id(args.engine, args.mode, args.query, args.profile, args.concurrency, args.run_number)
    if args.dry_run:
        warmup, measurement, cooldown = (2, 60, 2) if args.mode == "smoke" else (60, 300, 60)
        root = Path(args.output_root) if args.output_root else Path("results") / args.mode
        if not root.is_absolute():
            root = ROOT / root
        plan = {
            "status": "dry_run_valid",
            "run_id": run_id,
            "engine": args.engine,
            "profile": args.profile,
            "query": args.query,
            "treatment": treatment(args.engine, args.query),
            "concurrency": args.concurrency,
            "connections": args.concurrency,
            "run_number": args.run_number,
            "mode": args.mode,
            "schedule_seconds": {"warmup": warmup, "measurement": measurement, "cooldown": cooldown, "max_drain": 120},
            "estimated_duration_seconds": warmup + measurement + cooldown,
            "query_file": str(query_path(args.engine, args.query).relative_to(ROOT)),
            "plan_work_reference_m3": plan_work_reference(args.engine, args.query),
            "output_directory": str(root / run_id),
            "csv_header": (ROOT / "contracts/results_header.csv").read_text(encoding="utf-8").strip(),
            "cache_protocol": "alternative_no_host_drop_caches",
            "database_connections_opened": False,
            "results_written": False,
        }
        print(json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    try:
        output = execute_run(args)
    except FileExistsError as exc:
        print(f"No se sobrescribe un intento existente: {exc}", file=sys.stderr)
        return 2
    return 0 if (output / "quality.json").exists() and json.loads((output / "quality.json").read_text()).get("status") in {"ok", "spill"} else 1
