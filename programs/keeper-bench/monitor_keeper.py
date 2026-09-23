#!/usr/bin/env python3

import argparse
import csv
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime


METRICS = (
    "zk_server_state",
    "zk_avg_latency",
    "zk_max_latency",
    "zk_packets_received",
    "zk_packets_sent",
    "zk_num_alive_connections",
    "zk_outstanding_requests",
    "zk_znode_count",
    "zk_in_snapshot",
    "zk_avg_log_replication_batch_size",
    "zk_max_log_replication_batch_size",
    "zk_cnt_log_replication_batch_size",
    "zk_sum_log_replication_batch_size",
    "zk_p99_push_request_queue_time_ms",
    "zk_p50_readlatency",
    "zk_p90_readlatency",
    "zk_p99_readlatency",
    "zk_p999_readlatency",
    "zk_cnt_readlatency",
    "zk_sum_readlatency",
    "zk_p50_updatelatency",
    "zk_p90_updatelatency",
    "zk_p99_updatelatency",
    "zk_p999_updatelatency",
    "zk_cnt_updatelatency",
    "zk_sum_updatelatency",
    "zk_snap_count",
    "zk_snap_time_ms",
    "zk_snap_blocking_time_ms",
)


SNAPSHOT_COMPLETION_TIMEOUT = 300.0


def send_four_letter(host, port, command, timeout=5):
    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.sendall(command.encode("ascii"))
        connection.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = connection.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks).decode("utf-8", errors="replace")


def read_metrics(host, port):
    values = {}
    for line in send_four_letter(host, port, "mntr").splitlines():
        fields = line.split("\t", 1)
        if len(fields) == 2:
            values[fields[0]] = fields[1]
    return values


def trigger_snapshot(host, port, result):
    started = time.monotonic()
    try:
        response = send_four_letter(host, port, "csnp", timeout=120).strip()
        if not response.isdigit():
            raise RuntimeError(f"Snapshot request failed: {response or 'empty response'}")
        result["response"] = response
    except Exception as exception:
        result["error"] = repr(exception)
    finally:
        result["duration_seconds"] = time.monotonic() - started


def parse_server(value):
    host, port = value.rsplit(":", 1)
    # IPv6 endpoints arrive bracketed ([::1]:2181); sockets need the bare literal
    return host.strip("[]"), int(port)


def sample_row(endpoint, host, port, started, metrics, error):
    return {
        "timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "elapsed_seconds": f"{time.monotonic() - started:.3f}",
        "host": endpoint,
        "error": error,
        **{metric: metrics.get(metric, "") for metric in METRICS},
    }


def wait_for_snapshot_completion(writer, output, endpoint, host, port, requested_index, started):
    deadline = time.monotonic() + SNAPSHOT_COMPLETION_TIMEOUT
    while True:
        completed = False
        try:
            # A counter increment may belong to an automatic snapshot. Verify the
            # index returned by csnp on the same endpoint, even after demotion.
            log_info = dict(
                line.split("\t", 1) for line in send_four_letter(host, port, "lgif").splitlines() if "\t" in line
            )
            completed = int(log_info["last_snapshot_idx"]) >= requested_index
            metrics = read_metrics(host, port)
            error = ""
        except Exception as exception:
            metrics = {}
            error = repr(exception)
        writer.writerow(sample_row(endpoint, host, port, started, metrics, error))
        output.flush()
        if completed and not error:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Snapshot index {requested_index} on {endpoint} did not complete within "
                f"{SNAPSHOT_COMPLETION_TIMEOUT:.0f} seconds; last error: {error or 'none'}"
            )
        time.sleep(1.0)


def monitor(args, stop_event=None):
    servers = [(value, *parse_server(value)) for value in args.servers]
    if args.reset_stats:
        for _, host, port in servers:
            send_four_letter(host, port, "srst")

    fieldnames = ["timestamp", "elapsed_seconds", "host", "error", *METRICS]
    started = time.monotonic()
    next_sample = started
    snapshot_thread = None
    snapshot_result = {}

    with open(args.output, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            now = time.monotonic()
            elapsed = now - started
            if elapsed > args.duration or (stop_event is not None and stop_event.is_set()):
                break

            samples = []
            for endpoint, host, port in servers:
                metrics = {}
                error = ""
                try:
                    metrics = read_metrics(host, port)
                except Exception as exception:
                    error = repr(exception)
                samples.append(sample_row(endpoint, host, port, started, metrics, error))

            writer.writerows(samples)
            output.flush()

            if args.snapshot_at is not None and snapshot_thread is None and elapsed >= args.snapshot_at:
                if args.snapshot_server:
                    snapshot_endpoint = args.snapshot_server
                else:
                    leader = next(
                        (row for row in samples if row.get("zk_server_state") in ("leader", "standalone")), None
                    )
                    if leader is None:
                        raise RuntimeError("No leader found for snapshot")
                    snapshot_endpoint = leader["host"]
                snapshot_host, snapshot_port = parse_server(snapshot_endpoint)
                snapshot_thread = threading.Thread(
                    target=trigger_snapshot,
                    args=(snapshot_host, snapshot_port, snapshot_result),
                    daemon=True,
                )
                snapshot_thread.start()

            next_sample += args.interval
            wait_seconds = max(0, next_sample - time.monotonic())
            if stop_event is None:
                time.sleep(wait_seconds)
            elif stop_event.wait(wait_seconds):
                break

        # csnp only schedules the snapshot; keep sampling until it actually completes
        if snapshot_thread:
            snapshot_thread.join(timeout=130)
            print(f"snapshot_result={snapshot_result}")
            if snapshot_thread.is_alive():
                raise TimeoutError("Snapshot request did not finish within 130 seconds")
            if "error" in snapshot_result:
                raise RuntimeError(snapshot_result["error"])
            wait_for_snapshot_completion(
                writer, output, snapshot_endpoint, snapshot_host, snapshot_port, int(snapshot_result["response"]), started
            )
    if snapshot_thread is None and args.snapshot_at is not None:
        raise RuntimeError("Benchmark ended before the requested snapshot was triggered")


def run_benchmark(args):
    command = args.benchmark_command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ValueError("benchmark command is empty")

    stop_event = threading.Event()
    monitor_thread = None
    monitor_errors = []

    def monitor_target():
        try:
            monitor(args, stop_event)
        except Exception as exception:
            monitor_errors.append(exception)

    benchmark_output = open(args.benchmark_output, "w", encoding="utf-8") if args.benchmark_output else None
    process = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in process.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            if benchmark_output:
                benchmark_output.write(line)
                benchmark_output.flush()
            if monitor_thread is None and args.start_marker in line:
                monitor_thread = threading.Thread(target=monitor_target, daemon=True)
                monitor_thread.start()
        return_code = process.wait()
    except BaseException:
        # Never leave the benchmark issuing requests after the wrapper dies
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise
    finally:
        if benchmark_output:
            benchmark_output.close()

    stop_event.set()
    if monitor_thread:
        monitor_thread.join(timeout=args.duration + SNAPSHOT_COMPLETION_TIMEOUT + 130)
    else:
        print(f"Start marker not found: {args.start_marker!r}", file=sys.stderr)
        return return_code or 2

    if monitor_errors:
        print(f"Keeper monitor failed: {monitor_errors[0]!r}", file=sys.stderr)
        return return_code or 2
    return return_code


def main():
    parser = argparse.ArgumentParser(description="Sample Keeper metrics while running an optional benchmark command")
    parser.add_argument("--servers", nargs="+", required=True, help="Keeper endpoints in host:port form")
    parser.add_argument("--duration", type=float, required=True, help="Maximum sampling duration in seconds")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds")
    parser.add_argument("--snapshot-at", type=float, help="Trigger csnp this many seconds after sampling starts")
    parser.add_argument("--snapshot-server", help="Endpoint receiving csnp; defaults to the current leader")
    parser.add_argument("--reset-stats", action="store_true", help="Send srst to every endpoint before sampling")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--benchmark-output", help="Optional combined stdout/stderr log for the benchmark")
    parser.add_argument("--start-marker", default="Run benchmark for", help="Output marker that starts metric sampling")
    parser.add_argument("benchmark_command", nargs=argparse.REMAINDER, help="Benchmark command after --")
    args = parser.parse_args()

    if args.benchmark_command:
        return run_benchmark(args)
    monitor(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
