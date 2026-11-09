import csv
import io
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bench
import monitor_keeper


def sample(second, host="a:2181", role="leader", reads=0, writes=0, snapshots=0, snapshot_ms=0, blocking_ms=0, **extra):
    return {
        "elapsed_seconds": second,
        "host": host,
        "error": "",
        "zk_server_state": role,
        "zk_znode_count": 100,
        "zk_cnt_readlatency": reads,
        "zk_sum_readlatency": reads * 2,
        "zk_cnt_updatelatency": writes,
        "zk_sum_updatelatency": writes * 3,
        "zk_snap_count": snapshots,
        "zk_snap_time_ms": snapshot_ms,
        "zk_snap_blocking_time_ms": blocking_ms,
        **extra,
    }


class ReportingTests(unittest.TestCase):
    def report(self, rows, log="", return_code=1):
        manifest = {
            "profile": "smoke",
            "root_path": "/test",
            "resolved": bench.PROFILES["smoke"],
            "drop_late_requests": False,
            "operation_timeout_ms": 35000,
            "session_timeout_ms": 3600000,
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "benchmark.log").write_text(log, encoding="utf-8")
            with (output / "keeper-metrics.csv").open("w", newline="", encoding="utf-8") as metrics:
                writer = csv.DictWriter(metrics, fieldnames=["timestamp", "elapsed_seconds", "host", "error", *monitor_keeper.METRICS])
                writer.writeheader()
                writer.writerows(rows)
            bench.generate_report(output, manifest, return_code)
            return json.loads((output / "report.json").read_text()), (output / "report.md").read_text()

    def test_subsecond_samples_do_not_multiply_throughput(self):
        rows = [sample(step / 4, reads=step * 25, writes=step * 5) for step in range(9)]
        report, _ = self.report(rows)
        self.assertEqual(report["peak_interval"]["read_qps"], 100)
        self.assertEqual(report["peak_interval"]["write_qps"], 20)

    def test_partial_endpoint_sampling_sums_endpoint_rates(self):
        rows = [sample(0, host=host) for host in ("a:2181", "b:2181", "c:2181")]
        rows += [sample(1, host=host, reads=100) for host in ("a:2181", "b:2181")]
        report, _ = self.report(rows)
        self.assertEqual(report["peak_interval"]["read_qps"], 200)

    def test_unequal_sample_durations_are_weighted_per_endpoint(self):
        rows = [
            sample(0),
            sample(0, host="b:2181"),
            sample(0.1, reads=20),
            sample(0.5, reads=50),
            sample(0.5, host="b:2181", reads=100),
        ]
        report, _ = self.report(rows)
        self.assertEqual(report["peak_interval"]["read_qps"], 300)
        self.assertEqual(report["peak_interval"]["read_average_ms"], 2)

    def test_failed_run_has_unavailable_measurements(self):
        report, markdown = self.report([])
        self.assertEqual(report["status"], "failed (rc=1)")
        self.assertIsNone(report["benchmark"]["all"])
        for label in ("Completed QPS", "Average latency", "P99 latency", "P99.9 latency", "Errors"):
            self.assertIn(f"- {label}: N/A", markdown)

    def test_missing_final_statistics_marks_successful_exit_incomplete(self):
        report, _ = self.report([], return_code=0)
        self.assertEqual(report["status"], "incomplete")

    def test_real_zero_measurements_are_preserved(self):
        log = "\n".join(
            f"Result for {kind} (time unit us) qps:0.0 errors:0 avg:0.0 p50:0.0 p90:0.0 p99:0.0 p999:0.0 cnt:0"
            for kind in ("all", "reads", "writes")
        )
        report, markdown = self.report([], log, return_code=0)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["benchmark"]["all"]["errors"], 0)
        self.assertIn("- Errors: 0", markdown)
        self.assertIn("- Average latency: 0.000 ms", markdown)

    def test_snapshot_completion_after_demotion_is_counted(self):
        report, _ = self.report([
            sample(0, snapshots=10, snapshot_ms=1000, blocking_ms=50),
            sample(1, zk_in_snapshot=1, snapshots=10, snapshot_ms=1000, blocking_ms=50),
            sample(2, role="follower", snapshots=11, snapshot_ms=1200, blocking_ms=60),
        ])
        self.assertEqual(report["snapshot_count"], 1)
        self.assertEqual(report["snapshot_time_ms"], 200)
        self.assertEqual(report["snapshot_blocking_ms"], 10)

    def test_snapshot_totals_accumulate_across_leadership_changes(self):
        report, _ = self.report([
            sample(0, snapshots=10, snapshot_ms=1000, blocking_ms=50),
            sample(0, host="b:2181", role="follower", snapshots=30, snapshot_ms=3000, blocking_ms=150),
            sample(1, snapshots=11, snapshot_ms=1200, blocking_ms=60),
            sample(1, host="b:2181", snapshots=30, snapshot_ms=3000, blocking_ms=150),
            sample(2, host="b:2181", snapshots=31, snapshot_ms=3400, blocking_ms=170),
        ])
        self.assertEqual(report["snapshot_count"], 2)
        self.assertEqual(report["snapshot_time_ms"], 600)
        self.assertEqual(report["snapshot_blocking_ms"], 30)

    def test_snapshot_counters_reset_without_losing_later_completions(self):
        report, _ = self.report([
            sample(0, snapshots=10, snapshot_ms=1000, blocking_ms=50),
            sample(1, snapshots=11, snapshot_ms=1200, blocking_ms=60),
            sample(2),
            sample(3, snapshots=1, snapshot_ms=300, blocking_ms=15),
        ])
        self.assertEqual(report["snapshot_count"], 2)
        self.assertEqual(report["snapshot_time_ms"], 500)
        self.assertEqual(report["snapshot_blocking_ms"], 25)

    def test_standalone_snapshot_and_znode_growth(self):
        report, _ = self.report([
            sample(0, role="standalone"),
            sample(1, role="standalone", snapshots=1, zk_znode_count=120),
        ])
        self.assertEqual(report["snapshot_count"], 1)
        self.assertEqual(report["first_znode_count"], 100)
        self.assertEqual(report["last_znode_count"], 120)


class SnapshotMonitorTests(unittest.TestCase):
    def wait_for_completion(self):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["timestamp", "elapsed_seconds", "host", "error", *monitor_keeper.METRICS])
        monitor_keeper.wait_for_snapshot_completion(writer, output, "a:2181", "a", 2181, 200, 0)
        return output.getvalue()

    @patch.object(monitor_keeper.time, "sleep")
    @patch.object(monitor_keeper, "send_four_letter")
    @patch.object(monitor_keeper, "read_metrics")
    def test_unrelated_snapshot_does_not_complete_requested_index(self, metrics, command, sleep):
        metrics.return_value = {"zk_snap_count": "201", "zk_server_state": "follower"}
        command.side_effect = ["last_snapshot_idx\t199\n", "last_snapshot_idx\t200\n"]
        self.wait_for_completion()
        self.assertEqual(command.call_count, 2)
        command.assert_called_with("a", 2181, "lgif")
        sleep.assert_called_once()

    @patch.object(monitor_keeper.time, "sleep")
    @patch.object(monitor_keeper, "send_four_letter")
    @patch.object(monitor_keeper, "read_metrics")
    def test_failed_completion_read_is_retried(self, metrics, command, sleep):
        metrics.return_value = {"zk_snap_count": "201"}
        command.side_effect = [OSError("unavailable"), "last_snapshot_idx\t201\n"]
        self.wait_for_completion()
        self.assertEqual(command.call_count, 2)

    @patch.object(monitor_keeper, "SNAPSHOT_COMPLETION_TIMEOUT", 0)
    @patch.object(monitor_keeper, "send_four_letter", return_value="lgif is not whitelisted")
    @patch.object(monitor_keeper, "read_metrics", return_value={"zk_snap_count": "201"})
    def test_unverifiable_completion_is_an_explicit_failure(self, metrics, command):
        with self.assertRaises(TimeoutError):
            self.wait_for_completion()

    @patch.object(monitor_keeper, "wait_for_snapshot_completion")
    @patch.object(monitor_keeper, "send_four_letter", return_value="200")
    @patch.object(monitor_keeper, "read_metrics")
    def test_failed_extra_baseline_read_cannot_skip_completion(self, metrics, command, wait):
        # The old implementation made an extra baseline read, swallowed its failure,
        # scheduled csnp, and then skipped the completion waiter entirely.
        metrics.side_effect = [{"zk_server_state": "standalone"}, OSError("baseline unavailable")]
        stop_event = threading.Event()

        def schedule_snapshot(*args, **kwargs):
            stop_event.set()
            return "200"

        command.side_effect = schedule_snapshot
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(
                servers=["a:2181"], reset_stats=False, duration=1, interval=1,
                snapshot_at=0, snapshot_server=None, output=str(Path(directory) / "metrics.csv"),
            )
            monitor_keeper.monitor(args, stop_event)
        wait.assert_called_once()
        self.assertEqual(wait.call_args.args[5], 200)


if __name__ == "__main__":
    unittest.main()
