import csv
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from benchmark_runner.metrics import classify_run_status, completed_inside_window, csv_values, nearest_rank, write_summary
from benchmark_runner.drivers import query_path, treatment
from benchmark_runner.runner import CSV_HEADER, _run_queries, new_run_id, plan_work_reference


class BenchmarkRunnerTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        values = [90, 10, 50, 30, 70]
        self.assertEqual(nearest_rank(values, 0.50), 50)
        self.assertEqual(nearest_rank(values, 0.95), 90)
        self.assertEqual(nearest_rank(values, 0.99), 90)
        self.assertIsNone(nearest_rank([], 0.95))

    def test_measurement_window_uses_completion_and_inclusive_bounds(self):
        self.assertTrue(completed_inside_window(10, 20, 10, 20))
        self.assertFalse(completed_inside_window(9.9, 20, 10, 20))
        self.assertFalse(completed_inside_window(10, 20.1, 10, 20))

    def test_status_precedence_and_unknowns(self):
        self.assertEqual(classify_run_status(oom="positive", infrastructure_error=True, spill="positive"), "oom")
        self.assertEqual(classify_run_status(oom="negative_comprobado", infrastructure_error=True, spill="positive"), "error")
        self.assertEqual(classify_run_status(oom="negative_comprobado", infrastructure_error=False, spill="positive"), "spill")
        self.assertEqual(classify_run_status(oom="negative_comprobado", infrastructure_error=False, spill="negative_comprobado"), "ok")
        self.assertEqual(classify_run_status(oom="unknown", infrastructure_error=False, spill="negative_comprobado"), "error")

    def test_csv_has_exact_contract_header_and_unknowns_are_blank(self):
        row = csv_values(
            timestamp_epoch=100.25, engine="mongodb", profile="PROFILE_A_BASELINE", query_id="Q1",
            concurrency=1, run_number=1, measurement_valid=True, measurement_seconds=10,
            latencies_ms=[10, 20], cpu_throttled_seconds=None, max_rss_bytes=None,
            disk_spill=None, status="error",
        )
        self.assertEqual(list(row), CSV_HEADER)
        self.assertEqual(row["throughput_qps"], "0.200000000")
        self.assertEqual(row["latency_p95_ms"], "20.000000")
        self.assertEqual(row["cpu_throttled_seconds"], "")
        self.assertEqual(row["max_rss_mb"], "")
        self.assertEqual(row["disk_spill"], "")
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "summary.csv"
            write_summary(path, CSV_HEADER, row)
            with path.open(newline="") as stream:
                self.assertEqual(next(csv.reader(stream)), CSV_HEADER)
                self.assertEqual(len(next(csv.reader(stream))), len(CSV_HEADER))

    def test_generated_run_id_fits_engine_application_name_limit(self):
        ident = new_run_id("postgresql", "official", "Q2_ALT", "PROFILE_C_SEVERE", 50, 3)
        self.assertLessEqual(len(ident), 63)
        self.assertTrue(ident.isascii())

    def test_q2_alt_reuses_fixed_query_and_applies_engine_treatment(self):
        self.assertEqual(query_path("mongodb", "Q2_ALT"), query_path("mongodb", "Q2"))
        self.assertEqual(query_path("postgresql", "Q2_ALT"), query_path("postgresql", "Q2"))
        self.assertEqual(treatment("mongodb", "Q2_ALT"), {"allowDiskUse": True})
        self.assertEqual(treatment("postgresql", "Q2_ALT"), {"work_mem": "64MB"})

    def test_m3_work_estimates_use_plan_scans_not_top_k_rows(self):
        mongo_q1 = plan_work_reference("mongodb", "Q1")
        pg_q2 = plan_work_reference("postgresql", "Q2")
        self.assertEqual(mongo_q1["documents_per_query"], 500000)
        self.assertEqual(mongo_q1["detections_per_query"], 1499811)
        self.assertEqual(pg_q2["documents_per_query"], 500000)

    def test_concurrent_clients_are_not_serialized_with_fake_drivers(self):
        class FakeDriver:
            def execute(self):
                time.sleep(0.035)
                return 30

            def server_version(self):
                return "fake-1"

            def effective_settings(self):
                return {"fixture": True}

            def close(self):
                pass

            def cancel(self):
                pass

        args = SimpleNamespace(
            engine="mongodb", query="Q1", concurrency=3, warmup_seconds=0,
            measurement_seconds=0.25, drain_seconds=1, cooldown_seconds=0,
        )
        events = []
        window = {}
        with patch("benchmark_runner.runner.connect_worker", side_effect=lambda *args: FakeDriver()):
            result = _run_queries(args, {}, "fake-run", window, events)
        self.assertTrue(window["valid"])
        self.assertEqual(result["client_concurrency"]["workers_ready"], 3)
        self.assertEqual(result["client_concurrency"]["connections_opened"], 3)
        self.assertGreaterEqual(result["client_concurrency"]["max_in_flight_measurement_operations"], 2)
        self.assertGreater(result["outcomes"]["measurement_successes"], 0)
        self.assertTrue(all(event["result_count"] == 30 for event in events if event["status"] == "ok"))


if __name__ == "__main__":
    unittest.main()
