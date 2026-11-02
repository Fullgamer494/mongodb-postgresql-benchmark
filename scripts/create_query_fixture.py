#!/usr/bin/env python3
"""Create a small development-only fixture with hand-checkable Q1/Q2 results."""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data/development/phase04_query_fixture.jsonl"


def event(index: int) -> dict[str, Any]:
    if index == 0:
        detections = [
            {"signature": "FIX-C1", "category": "credential_attack", "occurrences": 2, "impact_score": 60, "confidence": 90},
            {"signature": "FIX-N1", "category": "network_scan", "occurrences": 1, "impact_score": 80, "confidence": 90},
            {"signature": "FIX-N2", "category": "network_scan", "occurrences": 1, "impact_score": 50, "confidence": 90},
        ]
    elif index == 1:
        detections = [
            {"signature": "FIX-C2", "category": "credential_attack", "occurrences": 3, "impact_score": 70, "confidence": 85},
        ]
    elif index == 2:
        detections = [
            {"signature": "FIX-C3", "category": "credential_attack", "occurrences": 1, "impact_score": 90, "confidence": 80},
        ]
    elif index == 3:
        detections = [
            {"signature": "FIX-C4", "category": "credential_attack", "occurrences": 4, "impact_score": 60, "confidence": 75},
        ]
    else:
        detections = [
            {"signature": "FIX-LOW", "category": "credential_attack", "occurrences": 1, "impact_score": 50, "confidence": 70},
        ]

    if index == 0:
        risk_score = 100
    elif index == 1:
        risk_score = 90
    elif index < 1002:
        risk_score = 80
    else:
        risk_score = 100
    status = "pending" if index == 1002 else "confirmed"
    timestamp = "2025-12-31T23:59:59Z" if index == 1003 else "2026-01-01T00:00:00Z"
    country = "MX" if index < 2 else "US" if index < 4 else "BR"
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"u2-phase04-fixture-{index}")),
        "timestamp": timestamp,
        "device": {
            "device_id": f"FIX-{index:04d}",
            "platform": "linux",
            "network": {"country": country, "state": "DEV", "city": "Fixture", "asn": 64512},
        },
        "incident": {"status": status, "risk_score": risk_score},
        "detections": detections,
        "metadata": {"channel": "fixture", "sensor_version": "phase04-1", "flags": {"synthetic": True}},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    expected_path = output.with_suffix(".expected.json")
    if output.exists() or expected_path.exists():
        parser.error(f"No se sobrescribe el fixture existente: {output} o {expected_path}")
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = [event(index) for index in range(1004)]
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    expected = {
        "fixture_count": len(rows),
        "fixture_sha256": digest,
        "q1": [
            {"country": "MX", "category": "credential_attack", "total_impact": "330", "avg_impact": "165", "count": 2},
            {"country": "MX", "category": "network_scan", "total_impact": "80", "avg_impact": "80", "count": 1},
            {"country": "US", "category": "credential_attack", "total_impact": "330", "avg_impact": "165", "count": 2},
        ],
        "q2": {
            "eligible_count": 1002,
            "risk_score_histogram": {"100": 1, "90": 1, "80": 1000},
            "cutoff_risk_score": 80,
            "eligible_above_cutoff": 2,
            "eligible_at_cutoff": 1000,
            "selected_at_cutoff": 998,
            "excluded_pending": 1,
            "excluded_before_timestamp": 1,
        },
    }
    expected_path.write_text(json.dumps(expected, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"fixture": str(output), "expected": str(expected_path), "count": len(rows), "sha256": digest}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
