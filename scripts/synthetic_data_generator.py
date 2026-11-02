#!/usr/bin/env python3
"""Generate canonical synthetic security events as a deterministic JSONL stream."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import tempfile
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

GENERATOR_VERSION = "1.0.0"
DEFAULT_CONFIG = Path("config/dataset.json")
DEFAULT_SCHEMA = Path("contracts/event.schema.json")
DEFAULT_OUTPUT = Path("data/generated/events.jsonl")


class GenerationError(Exception):
    """Raised for invalid configuration or unsafe output replacement."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GenerationError(f"No se pudo leer JSON válido de {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GenerationError(f"La raíz de {path} debe ser un objeto JSON")
    return value


def parse_utc(value: str, field: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError) as exc:
        raise GenerationError(f"{field} debe usar el formato UTC canónico YYYY-MM-DDTHH:MM:SSZ") from exc
    return parsed


def validate_config(config: dict[str, Any], schema: dict[str, Any]) -> tuple[list[str], list[str], list[tuple[str, float]], datetime, datetime]:
    try:
        detection_cfg = config["detections_count"]
        minimum = int(detection_cfg["min"])
        maximum = int(detection_cfg["max"])
        countries = config["countries"]
        categories = config["categories"]
        status_probabilities = config["status_probabilities"]
    except (KeyError, TypeError, ValueError) as exc:
        raise GenerationError(f"Configuración incompleta o inválida: {exc}") from exc

    if detection_cfg.get("distribution") != "uniform_integer" or minimum != 1 or maximum != 5:
        raise GenerationError("El contrato de detecciones requiere uniform_integer entre 1 y 5")
    if not isinstance(countries, list) or not countries or len(set(countries)) != len(countries):
        raise GenerationError("countries debe ser una lista no vacía sin duplicados")
    if any(not isinstance(country, str) or len(country) != 2 or not country.isupper() for country in countries):
        raise GenerationError("Cada país debe ser un código sintético de dos letras mayúsculas")

    schema_categories = schema["properties"]["detections"]["items"]["properties"]["category"]["enum"]
    schema_statuses = schema["properties"]["incident"]["properties"]["status"]["enum"]
    if not isinstance(categories, list) or not categories or len(set(categories)) != len(categories):
        raise GenerationError("categories debe ser una lista no vacía sin duplicados")
    if not set(categories).issubset(schema_categories):
        raise GenerationError("categories debe ser subconjunto de las categorías permitidas por el esquema")

    if not isinstance(status_probabilities, dict) or not status_probabilities:
        raise GenerationError("status_probabilities debe ser un objeto no vacío")
    if not set(status_probabilities).issubset(schema_statuses):
        raise GenerationError("Los estados configurados deben estar permitidos por el esquema")
    probabilities: list[tuple[str, float]] = []
    for status, probability in status_probabilities.items():
        if not isinstance(probability, (int, float)) or probability < 0:
            raise GenerationError(f"Probabilidad inválida para el estado {status}")
        probabilities.append((status, float(probability)))
    if abs(sum(probability for _, probability in probabilities) - 1.0) > 1e-9:
        raise GenerationError("Las probabilidades de estado deben sumar 1")

    start = parse_utc(config.get("date_start"), "date_start")
    end = parse_utc(config.get("date_end"), "date_end")
    if end < start:
        raise GenerationError("date_end no puede ser anterior a date_start")
    return countries, categories, probabilities, start, end


def choose_weighted(rng: random.Random, values: list[tuple[str, float]]) -> str:
    point = rng.random()
    cumulative = 0.0
    for value, probability in values:
        cumulative += probability
        if point < cumulative:
            return value
    return values[-1][0]


def make_event(index: int, rng: random.Random, config: dict[str, Any], countries: list[str], categories: list[str], statuses: list[tuple[str, float]], start: datetime, date_span_seconds: int, namespace: uuid.UUID, seen_event_ids: set[str]) -> dict[str, Any]:
    collision_suffix = 0
    while True:
        name = f"event:{index}" if collision_suffix == 0 else f"event:{index}:collision:{collision_suffix}"
        event_id = str(uuid.uuid5(namespace, name))
        if event_id not in seen_event_ids:
            seen_event_ids.add(event_id)
            break
        collision_suffix += 1
    timestamp = start + timedelta(seconds=rng.randint(0, date_span_seconds))
    country = countries[rng.randrange(len(countries))]
    detection_min = int(config["detections_count"]["min"])
    detection_max = int(config["detections_count"]["max"])
    detection_count = rng.randint(detection_min, detection_max)

    detections = []
    for detection_index in range(detection_count):
        detections.append({
            "signature": f"SIG-{rng.randrange(1, 10001):05d}",
            "category": categories[rng.randrange(len(categories))],
            "occurrences": rng.randint(1, 20),
            "impact_score": rng.randint(0, 100),
            "confidence": rng.randint(0, 100),
        })

    return {
        "event_id": event_id,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "device": {
            "device_id": f"DEV-{index:06d}",
            "platform": ("linux", "windows", "android", "ios")[rng.randrange(4)],
            "network": {
                "country": country,
                "state": f"STATE-{country}-{rng.randrange(1, 9):02d}",
                "city": f"CITY-{country}-{rng.randrange(1, 33):02d}",
                "asn": rng.randint(64512, 65534),
            },
        },
        "incident": {
            "status": choose_weighted(rng, statuses),
            "risk_score": rng.randint(0, 100),
        },
        "detections": detections,
        "metadata": {
            "channel": "sensor",
            "sensor_version": "synthetic-1.0",
            "flags": {"synthetic": True},
        },
    }


def _write_manifest(path: Path, manifest: dict[str, Any]) -> Path:
    temporary = tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False)
    temp_path = Path(temporary.name)
    try:
        with temporary:
            json.dump(manifest, temporary, ensure_ascii=False, sort_keys=True, indent=2)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path


def generate_dataset(count: int, seed: int, output: Path, config_path: Path = DEFAULT_CONFIG, schema_path: Path = DEFAULT_SCHEMA, allow_overwrite: bool = False) -> tuple[Path, Path, dict[str, Any]]:
    if count < 1:
        raise GenerationError("count debe ser mayor que cero")
    config = load_json(config_path)
    schema = load_json(schema_path)
    countries, categories, statuses, date_start, date_end = validate_config(config, schema)
    output = output.resolve()
    manifest_path = output.with_suffix(output.suffix + ".manifest.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, f"synthetic-security-events:{seed}")
    seen_event_ids: set[str] = set()
    span = int((date_end - date_start).total_seconds())
    status_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    country_counts: Counter[str] = Counter()
    detection_counts: Counter[str] = Counter()
    timestamp_min: str | None = None
    timestamp_max: str | None = None
    total_detections = 0
    digest = hashlib.sha256()
    byte_count = 0
    temp_data: Path | None = None
    temp_manifest: Path | None = None

    handle = tempfile.NamedTemporaryFile("wb", dir=output.parent, prefix=f".{output.name}.", suffix=".tmp", delete=False)
    temp_data = Path(handle.name)
    try:
        with handle:
            for index in range(count):
                event = make_event(index, rng, config, countries, categories, statuses, date_start, span, namespace, seen_event_ids)
                line = (json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
                handle.write(line)
                digest.update(line)
                byte_count += len(line)
                status_counts[event["incident"]["status"]] += 1
                country_counts[event["device"]["network"]["country"]] += 1
                timestamp = event["timestamp"]
                timestamp_min = timestamp if timestamp_min is None else min(timestamp_min, timestamp)
                timestamp_max = timestamp if timestamp_max is None else max(timestamp_max, timestamp)
                detection_counts[str(len(event["detections"]))] += 1
                total_detections += len(event["detections"])
                for detection in event["detections"]:
                    category_counts[detection["category"]] += 1
            handle.flush()
            os.fsync(handle.fileno())

        data_checksum = digest.hexdigest()
        manifest = {
            "manifest_version": 1,
            "format": "jsonl",
            "generator_version": GENERATOR_VERSION,
            "generator_sha256": sha256_file(Path(__file__)),
            "config_sha256": sha256_file(config_path),
            "schema_sha256": sha256_file(schema_path),
            "seed": seed,
            "count": count,
            "size_bytes": byte_count,
            "sha256": data_checksum,
            "timestamp_range": {"min": timestamp_min, "max": timestamp_max},
            "configured_timestamp_range": {
                "min": date_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "max": date_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            "detection_count_distribution": dict(sorted(detection_counts.items(), key=lambda item: int(item[0]))),
            "detections": {
                "total": total_detections,
                "min_per_event": min(map(int, detection_counts)),
                "max_per_event": max(map(int, detection_counts)),
                "mean_per_event": total_detections / count,
            },
            "status_counts": dict(sorted(status_counts.items())),
            "configured_status_probabilities": {key: value for key, value in statuses},
            "category_counts": dict(sorted(category_counts.items())),
            "country_counts": dict(sorted(country_counts.items())),
        }
        temp_manifest = _write_manifest(manifest_path, manifest)

        existing_data_matches = output.exists() and sha256_file(output) == data_checksum
        existing_manifest_matches = manifest_path.exists() and manifest_path.read_bytes() == temp_manifest.read_bytes()
        if (output.exists() or manifest_path.exists()) and not allow_overwrite:
            if existing_data_matches and (not manifest_path.exists() or existing_manifest_matches):
                temp_data.unlink(missing_ok=True)
                temp_manifest.unlink(missing_ok=True)
                return output, manifest_path, manifest
            raise GenerationError(f"Ya existe una salida distinta o un manifiesto discordante; conserva los archivos. Usa --allow-overwrite explícitamente: {output}")

        os.replace(temp_data, output)
        os.replace(temp_manifest, manifest_path)
        temp_data = None
        temp_manifest = None
        return output, manifest_path, manifest
    finally:
        if temp_data is not None:
            temp_data.unlink(missing_ok=True)
        if temp_manifest is not None:
            temp_manifest.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, help="Cantidad de eventos (por defecto official_count de la configuración)")
    parser.add_argument("--seed", type=int, help="Semilla (por defecto la de configuración)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--allow-overwrite", action="store_true", help="Permite reemplazar una salida existente con contenido diferente")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_json(args.config)
        count = args.count if args.count is not None else int(config["official_count"])
        seed = args.seed if args.seed is not None else int(config["seed"])
        output, manifest_path, manifest = generate_dataset(count, seed, args.output, args.config, args.schema, args.allow_overwrite)
    except (GenerationError, OSError, KeyError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"output": str(output), "manifest": str(manifest_path), "count": manifest["count"], "size_bytes": manifest["size_bytes"], "sha256": manifest["sha256"], "seed": manifest["seed"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
