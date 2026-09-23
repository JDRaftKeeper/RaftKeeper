import json
import os
import re
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from monitor_keeper import send_four_letter


@unittest.skipUnless(os.environ.get("KEEPER_BENCH_BINARY"), "set KEEPER_BENCH_BINARY to run isolated native tests")
class NativeBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = str(Path(os.environ["KEEPER_BENCH_BINARY"]).resolve())
        cls.directory = tempfile.TemporaryDirectory(prefix="keeper-bench-test-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.output = Path(cls.directory.name)
        sockets = [socket.socket() for _ in range(3)]
        try:
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            cls.port, forwarding_port, internal_port = [sock.getsockname()[1] for sock in sockets]
        finally:
            for sock in sockets:
                sock.close()
        cls.endpoint = f"127.0.0.1:{cls.port}"
        config = cls.output / "config.xml"
        config.write_text(
            f"""<raftkeeper>
    <logger><level>warning</level><log_to_console>true</log_to_console></logger>
    <keeper>
        <my_id>1</my_id><host>127.0.0.1</host><port>{cls.port}</port>
        <forwarding_port>{forwarding_port}</forwarding_port><internal_port>{internal_port}</internal_port>
        <parallel>4</parallel><log_dir>{cls.output}/logs</log_dir><snapshot_dir>{cls.output}/snapshots</snapshot_dir>
        <create_snapshot_on_exit>false</create_snapshot_on_exit>
        <raft_settings><nuraft_thread_size>4</nuraft_thread_size></raft_settings>
    </keeper>
</raftkeeper>
""", encoding="utf-8",
        )
        server_log = (cls.output / "server.log").open("w", encoding="utf-8")
        cls.addClassCleanup(server_log.close)
        cls.server = subprocess.Popen(
            [cls.binary, "server", "--config-file", str(config)], cwd=cls.output, stdout=server_log, stderr=subprocess.STDOUT,
        )
        cls.addClassCleanup(cls.stop_process, cls.server)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if cls.server.poll() is not None:
                raise RuntimeError((cls.output / "server.log").read_text())
            try:
                if "standalone" in send_four_letter("127.0.0.1", cls.port, "mntr", timeout=1):
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise TimeoutError("Isolated Keeper did not become ready")

    @staticmethod
    def stop_process(process):
        if process.poll() is None:
            process.send_signal(signal.SIGCONT)
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    def test_retained_requests_include_server_stall_and_pipeline_wait(self):
        command = [
            self.binary, "keeper-bench", "--server", self.endpoint,
            "--clickhouse-rmt-workload", "true", "--workload-root-name", "latency",
            "--concurrency", "1", "--pipeline-depth", "1", "--duration-sec", "3",
            "--metadata-groups", "1", "--children-per-list", "10",
            "--target-read-qps", "200", "--target-write-qps", "0", "--target-create-qps", "0",
            "--drop-late-requests", "false", "--operation-timeout-ms", "10000",
        ]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.addCleanup(self.stop_process, process)
        self.addCleanup(process.stdout.close)
        output = b""
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 30
            while b"Run benchmark for" not in output:
                if time.monotonic() >= deadline or process.poll() is not None:
                    self.fail(f"Benchmark never started: {output.decode(errors='replace')}")
                if selector.select(timeout=1):
                    output += os.read(process.stdout.fileno(), 65536)
        # Only pause the disposable server owned by this test, never an existing cluster.
        self.server.send_signal(signal.SIGSTOP)
        try:
            time.sleep(0.8)
        finally:
            self.server.send_signal(signal.SIGCONT)
        remaining, _ = process.communicate(timeout=30)
        text = (output + remaining).decode()
        self.assertEqual(process.returncode, 0, text)
        match = re.search(r"Result for all.*?errors:(\d+).*?avg:([0-9.]+).*?p99:([0-9.]+)", text)
        self.assertIsNotNone(match, text)
        self.assertEqual(int(match.group(1)), 0, text)
        # One stalled response alone barely affects the old distribution; retained
        # arrivals must also carry the accumulated schedule debt after it resumes.
        self.assertGreater(float(match.group(2)), 50000, text)
        self.assertGreater(float(match.group(3)), 400000, text)

    def test_unified_runner_waits_for_snapshot_and_generates_report(self):
        report_dir = self.output / "report"
        result = subprocess.run(
            [
                sys.executable, str(Path(__file__).resolve().parents[1] / "bench.py"),
                "--profile", "smoke", "--binary", self.binary, "--servers", self.endpoint,
                "--duration", "3", "--read-qps", "100", "--write-qps", "50",
                "--snapshot-at", "0.5", "--monitor-interval", "0.25", "--output-dir", str(report_dir),
            ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=45,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        report = json.loads((report_dir / "report.json").read_text())
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["benchmark"]["all"]["errors"], 0)
        self.assertGreaterEqual(report["snapshot_count"], 1)
        self.assertLess(report["peak_interval"]["read_qps"], 250)


if __name__ == "__main__":
    unittest.main()
