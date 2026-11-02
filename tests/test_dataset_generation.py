from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.synthetic_data_generator import GenerationError, generate_dataset
from scripts.validate_dataset import DatasetValidationError, validate_dataset


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/dataset.json"
SCHEMA = ROOT / "contracts/event.schema.json"


class DatasetGenerationTests(unittest.TestCase):
    def test_same_seed_produces_same_jsonl_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = Path(temp_dir) / "first.jsonl"
            second = Path(temp_dir) / "second.jsonl"
            _, _, first_manifest = generate_dataset(300, 713, first, CONFIG, SCHEMA)
            _, _, second_manifest = generate_dataset(300, 713, second, CONFIG, SCHEMA)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(first_manifest, second_manifest)
            report = validate_dataset(first, CONFIG, SCHEMA)
            self.assertTrue(report["valid"])
            self.assertEqual(report["count"], 300)

    def test_different_seed_changes_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = Path(temp_dir) / "first.jsonl"
            second = Path(temp_dir) / "second.jsonl"
            generate_dataset(20, 1, first, CONFIG, SCHEMA)
            generate_dataset(20, 2, second, CONFIG, SCHEMA)
            self.assertNotEqual(first.read_bytes(), second.read_bytes())

    def test_refuses_to_replace_different_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "events.jsonl"
            generate_dataset(20, 1, output, CONFIG, SCHEMA)
            original = output.read_bytes()
            with self.assertRaises(GenerationError):
                generate_dataset(20, 2, output, CONFIG, SCHEMA)
            self.assertEqual(output.read_bytes(), original)

    def test_validator_rejects_invalid_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "invalid.jsonl"
            event = json.loads((ROOT / "data/samples/event.example.json").read_text(encoding="utf-8"))
            event["event_id"] = "not-a-uuid"
            output.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaises(DatasetValidationError):
                validate_dataset(output, CONFIG, SCHEMA)

    def test_validator_rejects_impossible_calendar_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "invalid-date.jsonl"
            event = json.loads((ROOT / "data/samples/event.example.json").read_text(encoding="utf-8"))
            event["timestamp"] = "2026-02-30T12:00:00Z"
            output.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaises(DatasetValidationError):
                validate_dataset(output, CONFIG, SCHEMA)

    def test_validator_rejects_duplicate_uuids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "duplicate.jsonl"
            _, _, manifest = generate_dataset(300, 77, output, CONFIG, SCHEMA)
            lines = output.read_text(encoding="utf-8").splitlines()
            duplicated = json.loads(lines[1])
            duplicated["event_id"] = json.loads(lines[0])["event_id"]
            lines[1] = json.dumps(duplicated, sort_keys=True, separators=(",", ":"))
            output.write_text("\n".join(lines) + "\n", encoding="utf-8")
            manifest_path = output.with_suffix(output.suffix + ".manifest.json")
            with self.assertRaises(DatasetValidationError):
                validate_dataset(output, CONFIG, SCHEMA, manifest_path)

    def test_validator_rejects_tampered_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "events.jsonl"
            generate_dataset(300, 91, output, CONFIG, SCHEMA)
            manifest_path = output.with_suffix(output.suffix + ".manifest.json")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["sha256"] = "0" * 64
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(DatasetValidationError):
                validate_dataset(output, CONFIG, SCHEMA, manifest_path)


if __name__ == "__main__":
    unittest.main()
