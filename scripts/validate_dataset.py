#!/usr/bin/env python3
"""Stream-validate a canonical JSONL dataset and its optional manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

try:
    from .synthetic_data_generator import DEFAULT_CONFIG, DEFAULT_SCHEMA, GENERATOR_VERSION, GenerationError, load_json, sha256_file, validate_config
except ImportError:  # Support direct execution as `python scripts/validate_dataset.py`.
    from synthetic_data_generator import DEFAULT_CONFIG, DEFAULT_SCHEMA, GENERATOR_VERSION, GenerationError, load_json, sha256_file, validate_config


class DatasetValidationError(Exception):
    """Raised when the dataset violates its schema or reproducibility contract."""


def parse_canonical_utc(value: Any, line_number: int) -> datetime:
    if not isinstance(value, str):
        raise DatasetValidationError(f"Línea {line_number}: timestamp no es cadena")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise DatasetValidationError(f"Línea {line_number}: fecha UTC inválida {value!r}") from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise DatasetValidationError(f"Línea {line_number}: fecha no canónica {value!r}")
    return parsed


def check_chi_square(counts: Counter[str], choices: list[str], label: str, threshold: float = 40.0) -> None:
    total = sum(counts.values())
    if total < len(choices) * 5:
        return
    expected = total / len(choices)
    statistic = sum((counts.get(choice, 0) - expected) ** 2 / expected for choice in choices)
    if statistic > threshold:
        raise DatasetValidationError(f"Distribución {label} no compatible con muestreo uniforme: chi-cuadrado={statistic:.3f} > {threshold:g}")


def validate_dataset(input_path: Path, config_path: Path = DEFAULT_CONFIG, schema_path: Path = DEFAULT_SCHEMA, manifest_path: Path | None = None) -> dict[str, Any]:
    config = load_json(config_path)
    schema = load_json(schema_path)
    try:
        countries, categories, status_probabilities, config_start, config_end = validate_config(config, schema)
    except GenerationError as exc:
        raise DatasetValidationError(str(exc)) from exc

    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    input_path = input_path.resolve()
    if manifest_path is None:
        candidate = input_path.with_suffix(input_path.suffix + ".manifest.json")
        manifest_path = candidate if candidate.exists() else None

    status_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    country_counts: Counter[str] = Counter()
    detection_counts: Counter[str] = Counter()
    event_ids: set[str] = set()
    timestamp_min: str | None = None
    timestamp_max: str | None = None
    digest = hashlib.sha256()
    bytes_read = 0
    total_detections = 0
    count = 0
    try:
        stream = input_path.open("rb")
    except OSError as exc:
        raise DatasetValidationError(f"No se puede abrir {input_path}: {exc}") from exc

    with stream:
        for line_number, raw_line in enumerate(stream, start=1):
            if not raw_line.endswith(b"\n"):
                raise DatasetValidationError(f"Línea {line_number}: JSONL debe terminar cada registro con LF")
            if not raw_line.strip():
                raise DatasetValidationError(f"Línea {line_number}: línea vacía no permitida")
            digest.update(raw_line)
            bytes_read += len(raw_line)
            try:
                event = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DatasetValidationError(f"Línea {line_number}: JSON inválido: {exc}") from exc
            if not isinstance(event, dict):
                raise DatasetValidationError(f"Línea {line_number}: el evento debe ser objeto JSON")
            errors = sorted(validator.iter_errors(event), key=lambda error: tuple(str(part) for part in error.absolute_path))
            if errors:
                error = errors[0]
                location = ".".join(str(part) for part in error.absolute_path) or "$"
                raise DatasetValidationError(f"Línea {line_number}: esquema inválido en {location}: {error.message}")

            event_id = event.get("event_id")
            try:
                parsed_uuid = uuid.UUID(event_id)
            except (ValueError, TypeError, AttributeError) as exc:
                raise DatasetValidationError(f"Línea {line_number}: event_id no es UUID válido") from exc
            if str(parsed_uuid) != event_id:
                raise DatasetValidationError(f"Línea {line_number}: event_id debe usar UUID canónico en minúsculas")
            if event_id in event_ids:
                raise DatasetValidationError(f"Línea {line_number}: UUID duplicado {event_id}")
            event_ids.add(event_id)

            timestamp = event.get("timestamp")
            parsed_time = parse_canonical_utc(timestamp, line_number)
            if parsed_time < config_start or parsed_time > config_end:
                raise DatasetValidationError(f"Línea {line_number}: timestamp fuera del rango configurado")
            timestamp_min = timestamp if timestamp_min is None else min(timestamp_min, timestamp)
            timestamp_max = timestamp if timestamp_max is None else max(timestamp_max, timestamp)

            status = event["incident"]["status"]
            if status not in dict(status_probabilities):
                raise DatasetValidationError(f"Línea {line_number}: estado {status!r} no está configurado")
            status_counts[status] += 1
            country = event["device"]["network"]["country"]
            if country not in countries:
                raise DatasetValidationError(f"Línea {line_number}: país {country!r} no está configurado")
            country_counts[country] += 1
            detections = event["detections"]
            detection_counts[str(len(detections))] += 1
            total_detections += len(detections)
            for detection in detections:
                category = detection["category"]
                if category not in categories:
                    raise DatasetValidationError(f"Línea {line_number}: categoría {category!r} no está configurada")
                category_counts[category] += 1
            count += 1

    if count == 0:
        raise DatasetValidationError("El dataset está vacío")

    detection_cfg = config["detections_count"]
    detection_values = [int(value) for value in detection_counts]
    if min(detection_values) < int(detection_cfg["min"]) or max(detection_values) > int(detection_cfg["max"]):
        raise DatasetValidationError("Cantidad de detecciones fuera del rango configurado")
    if abs(total_detections / count - float(detection_cfg.get("expected_mean", 3))) > 0.25:
        raise DatasetValidationError("Promedio de detecciones se aleja más de 0.25 del esperado")
    check_chi_square(detection_counts, [str(n) for n in range(int(detection_cfg["min"]), int(detection_cfg["max"]) + 1)], "detecciones")
    check_chi_square(country_counts, countries, "países")
    check_chi_square(category_counts, categories, "categorías")
    for status, probability in status_probabilities:
        expected = count * probability
        tolerance = max(5.0, 8.0 * math.sqrt(count * probability * (1.0 - probability)))
        if abs(status_counts.get(status, 0) - expected) > tolerance:
            raise DatasetValidationError(f"Distribución del estado {status!r} fuera de tolerancia configurada")

    report = {
        "valid": True,
        "input": str(input_path),
        "count": count,
        "size_bytes": bytes_read,
        "sha256": digest.hexdigest(),
        "timestamp_range": {"min": timestamp_min, "max": timestamp_max},
        "detection_count_distribution": dict(sorted(detection_counts.items(), key=lambda item: int(item[0]))),
        "detections": {
            "total": total_detections,
            "min_per_event": min(detection_values),
            "max_per_event": max(detection_values),
            "mean_per_event": total_detections / count,
        },
        "status_counts": dict(sorted(status_counts.items())),
        "category_counts": dict(sorted(category_counts.items())),
        "country_counts": dict(sorted(country_counts.items())),
    }

    if manifest_path is not None:
        try:
            manifest = load_json(manifest_path)
        except GenerationError as exc:
            raise DatasetValidationError(str(exc)) from exc
        comparisons = {
            "manifest_version": 1,
            "format": "jsonl",
            "generator_version": GENERATOR_VERSION,
            "generator_sha256": sha256_file(Path(__file__).with_name("synthetic_data_generator.py")),
            "config_sha256": sha256_file(config_path),
            "schema_sha256": sha256_file(schema_path),
            "count": report["count"],
            "size_bytes": report["size_bytes"],
            "sha256": report["sha256"],
            "timestamp_range": report["timestamp_range"],
            "detection_count_distribution": report["detection_count_distribution"],
            "detections": report["detections"],
            "status_counts": report["status_counts"],
            "category_counts": report["category_counts"],
            "country_counts": report["country_counts"],
        }
        for key, actual in comparisons.items():
            if manifest.get(key) != actual:
                raise DatasetValidationError(f"El manifiesto no coincide con los datos en {key}")
        report["manifest"] = str(manifest_path.resolve())
        report["seed"] = manifest.get("seed")
        report["generator_version"] = manifest.get("generator_version")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--manifest", type=Path, help="Manifiesto sidecar; por defecto busca <input>.manifest.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = validate_dataset(args.input, args.config, args.schema, args.manifest)
    except (DatasetValidationError, OSError, ValueError) as exc:
        print(f"Dataset inválido: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
