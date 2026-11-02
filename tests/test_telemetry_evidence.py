import unittest

from telemetry.collector import counter_delta, summarize_metric
from telemetry.evidence import (
    classify_mongodb_spill,
    classify_oom,
    classify_postgresql_spill,
)


class TelemetryEvidenceTests(unittest.TestCase):
    def test_counter_delta_reports_reset_as_unknown(self):
        result = counter_delta([{"value": "7"}, {"value": "2"}])
        self.assertIsNone(result["delta"])
        self.assertEqual(result["reset_count"], 1)
        self.assertEqual(result["reason"], "counter_reset")

    def test_counter_delta_preserves_measured_delta(self):
        result = counter_delta([{"value": "7"}, {"value": "12"}])
        self.assertEqual(result["delta"], 5)
        self.assertEqual(result["reset_count"], 0)

    def test_summary_does_not_turn_missing_required_metric_into_zero(self):
        result = summarize_metric([], {"kind": "counter", "required": True}, 10, 12, 1)
        self.assertFalse(result["available"])
        self.assertIsNone(result["delta"])
        self.assertFalse(result["coverage_complete"])

    def test_summary_marks_counter_reset_and_keeps_delta_unknown(self):
        rows = [{"labels": {"id": "container-a"}, "values": [
            {"timestamp": 10, "value": "5"},
            {"timestamp": 11, "value": "1"},
            {"timestamp": 12, "value": "2"},
        ]}]
        result = summarize_metric(rows, {"kind": "counter", "required": True}, 10, 12, 1)
        self.assertTrue(result["reset_detected"])
        self.assertIsNone(result["delta"])
        self.assertEqual(result["counter_series_deltas"][0]["reason"], "counter_reset")

    def test_mongodb_spill_positive_negative_and_unknown(self):
        prefix = '{"t":{"$date":"2026-11-01T00:00:00Z"},"attr":{"command":{"comment":"run-a"},"usedDisk":'
        self.assertEqual(classify_mongodb_spill(prefix + 'true}}', "run-a")["status"], "positive")
        self.assertEqual(classify_mongodb_spill(prefix + 'false}}', "run-a")["status"], "negative_comprobado")
        self.assertEqual(classify_mongodb_spill("{}", "run-a")["status"], "unknown")

    def test_mongodb_completed_aggregate_with_disk_disabled_is_negative(self):
        record = '{"msg":"Slow query","attr":{"command":{"comment":"run-a","allowDiskUse":false},"durationMillis":123}}'
        result = classify_mongodb_spill(record, "run-a")
        self.assertEqual(result["status"], "negative_comprobado")
        self.assertEqual(len(result["completed_with_spill_disabled"]), 1)

    def test_postgresql_spill_needs_run_scoped_logs(self):
        self.assertEqual(classify_postgresql_spill("app=run-a temporary file: path", "run-a")["status"], "positive")
        self.assertEqual(classify_postgresql_spill("app=run-a statement complete", "run-a")["status"], "negative_comprobado")
        self.assertEqual(classify_postgresql_spill("temporary file: unrelated", "run-a")["status"], "unknown")

    def test_postgresql_complete_temp_logging_proves_no_spill_for_completed_fast_queries(self):
        no_lines = classify_postgresql_spill("", "pg-run-fast", completed_queries=10, log_temp_files="0", logs_complete=True)
        self.assertEqual(no_lines["status"], "negative_comprobado")
        self.assertEqual(no_lines["basis"], "complete_window_logs_log_temp_files_zero_and_completed_queries")
        self.assertEqual(classify_postgresql_spill("", "pg-run-fast", completed_queries=10, log_temp_files="-1", logs_complete=True)["status"], "unknown")
        self.assertEqual(classify_postgresql_spill("", "pg-run-fast", completed_queries=0, log_temp_files="0", logs_complete=True)["status"], "unknown")

    def test_oom_requires_cgroup_or_docker_evidence(self):
        before = {"container_id": "abc", "memory_events": {"oom": 0, "oom_kill": 0}}
        after = {"container_id": "abc", "memory_events": {"oom": 0, "oom_kill": 0}, "oom_killed": False}
        self.assertEqual(classify_oom(before, after, [])["status"], "negative_comprobado")
        positive = {"container_id": "abc", "memory_events": {"oom": 0, "oom_kill": 1}, "oom_killed": False}
        self.assertEqual(classify_oom(before, positive, [])["status"], "positive")
        missing = {"container_id": "abc", "memory_events": None, "oom_killed": False}
        self.assertEqual(classify_oom(before, missing, [])["status"], "unknown")
        reset = {"container_id": "abc", "memory_events": {"oom": 0, "oom_kill": 0, "high": 0}, "oom_killed": False}
        reset_before = {"container_id": "abc", "memory_events": {"oom": 0, "oom_kill": 0, "high": 10}, "oom_killed": False}
        self.assertEqual(classify_oom(reset_before, reset, [])["status"], "unknown")


if __name__ == "__main__":
    unittest.main()
