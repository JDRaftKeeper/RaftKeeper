#!/usr/bin/env python3

import argparse
import csv
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from monitor_keeper import parse_server, read_metrics, run_benchmark


PROFILES = {
    "smoke": {
        "duration": 10,
        "concurrency": 4,
        "pipeline_depth": 4,
        "read_qps": 1000,
        "write_qps": 500,
        "create_qps": 0,
        "metadata_groups": 1,
        "children_per_list": 100,
        "multi_size": 5,
        "snapshot_at": None,
    },
    "production": {
        "duration": 600,
        "concurrency": 226,
        "pipeline_depth": 128,
        "read_qps": 30000,
        "write_qps": 15000,
        "create_qps": 3600,
        "metadata_groups": 1242,
        "children_per_list": 1000,
        "multi_size": 5,
        "snapshot_at": 300,
    },
}


def boolean(value):
    return "true" if value else "false"


def run_and_tee(command, output_path):
    with output_path.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        try:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                output.write(line)
                output.flush()
        except BaseException:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise
        return process.wait()


def collect_state(servers):
    state = {}
    for endpoint in servers:
        host, port = parse_server(endpoint)
        try:
            state[endpoint] = read_metrics(host, port)
        except Exception as exception:
            state[endpoint] = {"error": repr(exception)}
    return state


def new_interval():
    return {"seconds": 0.0, "read_count": 0, "read_sum_ms": 0, "write_count": 0, "write_sum_ms": 0}


def format_measurement(result, name, spec, scale=1, unit=""):
    value = result.get(name)
    return "N/A" if value is None else f"{format(value / scale if scale != 1 else value, spec)}{unit}"


def generate_report(output_dir, manifest, return_code):
    benchmark_text = (output_dir / "benchmark.log").read_text(encoding="utf-8")
    state_before_path = output_dir / "state-before.json"
    state_before = json.loads(state_before_path.read_text(encoding="utf-8")) if state_before_path.exists() else {}
    result_pattern = re.compile(
        r"Result for (all|reads|writes).*?qps:([0-9.]+).*?avg:([0-9.]+).*?p50:([0-9.]+).*?"
        r"p90:([0-9.]+).*?p99:([0-9.]+).*?p999:([0-9.]+).*?cnt:([0-9]+)"
    )
    benchmark_results = {
        match.group(1): {
            "qps": float(match.group(2)),
            "average_us": float(match.group(3)),
            "p50_us": float(match.group(4)),
            "p90_us": float(match.group(5)),
            "p99_us": float(match.group(6)),
            "p999_us": float(match.group(7)),
            "requests": int(match.group(8)),
        }
        for match in result_pattern.finditer(benchmark_text)
    }
    errors_match = re.search(r"Result for all.*?errors:([0-9]+)", benchmark_text)
    if "all" in benchmark_results:
        benchmark_results["all"]["errors"] = int(errors_match.group(1)) if errors_match else None
    for kind in ("all", "reads", "writes"):
        benchmark_results.setdefault(kind, None)
    offered_match = re.search(
        r"Offered requests qps:([0-9.]+).*?issued:([0-9]+).*?target_qps:([0-9]+).*?"
        r"drop_late_requests:(true|false).*?dropped_slots:([0-9]+).*?max_schedule_lag_us:([0-9]+)",
        benchmark_text,
    )
    offered = None
    if offered_match:
        offered = {
            "qps": float(offered_match.group(1)),
            "issued": int(offered_match.group(2)),
            "target_qps": int(offered_match.group(3)),
            "drop_late_requests": offered_match.group(4) == "true",
            "dropped_slots": int(offered_match.group(5)),
            "max_schedule_lag_us": int(offered_match.group(6)),
        }

    previous = {}
    intervals = {}
    snapshot_start = None
    snapshot_end = None
    snapshot_count = 0
    snapshot_time_ms = 0
    snapshot_blocking_ms = 0
    previous_snapshots = {}
    first_znode_count = None
    last_znode_count = None
    node_samples = {}
    with (output_dir / "keeper-metrics.csv").open(encoding="utf-8") as metrics_file:
        for row in csv.DictReader(metrics_file):
            host = row["host"]
            required_counters = (
                "zk_cnt_readlatency",
                "zk_sum_readlatency",
                "zk_cnt_updatelatency",
                "zk_sum_updatelatency",
            )
            if row["error"]:
                continue
            elapsed = float(row["elapsed_seconds"])
            znode_count = int(row["zk_znode_count"] or 0)
            sample = node_samples.setdefault(host, {"state": "", "max_connections": 0})
            sample["state"] = row["zk_server_state"] or sample["state"]
            sample["max_connections"] = max(sample["max_connections"], int(row["zk_num_alive_connections"] or 0))
            if row["zk_server_state"] in ("leader", "standalone"):
                first_znode_count = znode_count if first_znode_count is None else first_znode_count
                last_znode_count = znode_count

            # Snapshot counters are node-local. Keep following every endpoint after
            # role changes and sum increments, never historical counters across nodes.
            snapshot_counters = ("zk_snap_count", "zk_snap_time_ms", "zk_snap_blocking_time_ms")
            if all(row[name] for name in snapshot_counters):
                snapshot_values = tuple(int(row[name]) for name in snapshot_counters)
                old_snapshots = previous_snapshots.get(host, snapshot_values)
                if all(value >= old for value, old in zip(snapshot_values, old_snapshots)):
                    snapshot_count += snapshot_values[0] - old_snapshots[0]
                    snapshot_time_ms += snapshot_values[1] - old_snapshots[1]
                    snapshot_blocking_ms += snapshot_values[2] - old_snapshots[2]
                previous_snapshots[host] = snapshot_values
            if row["zk_in_snapshot"] == "1":
                snapshot_start = elapsed if snapshot_start is None else snapshot_start
                snapshot_end = elapsed

            if any(not row[name] for name in required_counters):
                continue

            if host in previous:
                old = previous[host]
                delta_seconds = elapsed - float(old["elapsed_seconds"])
                counters_reset = any(int(row[name]) < int(old[name]) for name in required_counters)
                if delta_seconds > 0 and not counters_reset:
                    bucket = int(elapsed)
                    interval = intervals.setdefault(bucket, {}).setdefault(host, new_interval())
                    # Combine samples within each endpoint before summing endpoint rates.
                    read_delta = int(row["zk_cnt_readlatency"] or 0) - int(old["zk_cnt_readlatency"] or 0)
                    write_delta = int(row["zk_cnt_updatelatency"] or 0) - int(old["zk_cnt_updatelatency"] or 0)
                    read_sum_delta = int(row["zk_sum_readlatency"] or 0) - int(old["zk_sum_readlatency"] or 0)
                    write_sum_delta = int(row["zk_sum_updatelatency"] or 0) - int(old["zk_sum_updatelatency"] or 0)
                    interval["seconds"] += delta_seconds
                    interval["read_count"] += read_delta
                    interval["read_sum_ms"] += read_sum_delta
                    interval["write_count"] += write_delta
                    interval["write_sum_ms"] += write_sum_delta
            previous[host] = row

    interval_results = []
    for second, endpoints in sorted(intervals.items()):
        values = {name: sum(endpoint[name] for endpoint in endpoints.values()) for name in new_interval()}
        if values["read_count"] == 0 and values["write_count"] == 0:
            continue
        interval_results.append(
            {
                "second": second,
                "read_qps": sum(endpoint["read_count"] / endpoint["seconds"] for endpoint in endpoints.values()),
                "write_qps": sum(endpoint["write_count"] / endpoint["seconds"] for endpoint in endpoints.values()),
                "read_average_ms": values["read_sum_ms"] / max(1, values["read_count"]),
                "write_average_ms": values["write_sum_ms"] / max(1, values["write_count"]),
            }
        )

    peak = max(interval_results, key=lambda item: item["read_average_ms"] + item["write_average_ms"], default=None)
    nodes = []
    for endpoint, state in state_before.items():
        sample = node_samples.get(endpoint, {})
        nodes.append(
            {
                "endpoint": endpoint,
                "state": sample.get("state") or state.get("zk_server_state", ""),
                "version": state.get("zk_version", ""),
                "znode_count": int(state.get("zk_znode_count", 0)),
                "approximate_data_size": int(state.get("zk_approximate_data_size", 0)),
                "max_connections": sample.get("max_connections", 0),
            }
        )
    parameters = {
        **manifest["resolved"],
        "max_in_flight": manifest["resolved"]["concurrency"] * manifest["resolved"]["pipeline_depth"],
        "drop_late_requests": manifest["drop_late_requests"],
        "operation_timeout_ms": manifest["operation_timeout_ms"],
        "session_timeout_ms": manifest["session_timeout_ms"],
    }
    statistics_complete = all(benchmark_results.values()) and errors_match is not None
    status = ("complete" if statistics_complete else "incomplete") if return_code == 0 else f"failed (rc={return_code})"
    report = {
        "status": status,
        "root_path": manifest["root_path"],
        "profile": manifest["profile"],
        "parameters": parameters,
        "keeper_nodes": nodes,
        "benchmark": benchmark_results,
        "offered": offered,
        "first_znode_count": first_znode_count,
        "last_znode_count": last_znode_count,
        "snapshot_start_second": snapshot_start,
        "snapshot_end_second": snapshot_end,
        "snapshot_count": snapshot_count,
        "snapshot_time_ms": snapshot_time_ms,
        "snapshot_blocking_ms": snapshot_blocking_ms,
        "peak_interval": peak,
    }
    (output_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    all_result = benchmark_results["all"] or {}
    versions = sorted({node["version"] for node in nodes if node["version"]})
    markdown = [
        "# RaftKeeper benchmark report",
        "",
        f"- Status: {status}",
        f"- Root: `{manifest['root_path']}`",
        f"- Profile: `{manifest['profile']}`",
        f"- Keeper version: `{', '.join(versions)}`",
        "",
        "## Keeper nodes",
        "",
        "| Endpoint | Role | Znode count | Data size | Max connections |",
        "|---|---|---:|---:|---:|",
    ]
    for node in nodes:
        markdown.append(
            f"| {node['endpoint']} | {node['state']} | {node['znode_count']:,} | "
            f"{node['approximate_data_size']:,} B | {node['max_connections']:,} |"
        )
    markdown.extend(
        [
            "",
            "## Run configuration",
            "",
            "| Parameter | Value |",
            "|---|---:|",
            f"| Duration | {parameters['duration']} s |",
            f"| Concurrency | {parameters['concurrency']:,} |",
            f"| Pipeline depth | {parameters['pipeline_depth']:,} |",
            f"| Maximum in-flight | {parameters['max_in_flight']:,} |",
            f"| Target read QPS | {parameters['read_qps']:,} |",
            f"| Target write QPS | {parameters['write_qps']:,} |",
            f"| Target create QPS | {parameters['create_qps']:,} |",
            f"| Metadata groups | {parameters['metadata_groups']:,} |",
            f"| Children per list | {parameters['children_per_list']:,} |",
            f"| Multi size | {parameters['multi_size']:,} |",
            f"| Drop late requests | {str(parameters['drop_late_requests']).lower()} |",
            f"| Operation timeout | {parameters['operation_timeout_ms']:,} ms |",
            f"| Session timeout | {parameters['session_timeout_ms']:,} ms |",
            f"| Snapshot at | {parameters['snapshot_at']} s |",
            "",
            "## Results",
            "",
            f"- Completed QPS: {format_measurement(all_result, 'qps', ',.1f')}",
            f"- Average latency: {format_measurement(all_result, 'average_us', ',.3f', 1000, ' ms')}",
            f"- P99 latency: {format_measurement(all_result, 'p99_us', ',.3f', 1000, ' ms')}",
            f"- P99.9 latency: {format_measurement(all_result, 'p999_us', ',.3f', 1000, ' ms')}",
            f"- Errors: {format_measurement(all_result, 'errors', ',')}",
            "- Snapshot totals: sum across monitored nodes",
            f"- Snapshots completed: {snapshot_count:,}",
            f"- Snapshot time: {snapshot_time_ms:,} ms",
            f"- Snapshot blocking time: {snapshot_blocking_ms:,} ms",
        ]
    )
    if offered:
        markdown.extend(
            [
                f"- Offered QPS: {offered['qps']:,.1f} / {offered['target_qps']:,}",
                f"- Dropped slots: {offered['dropped_slots']:,}",
                f"- Maximum schedule lag: {offered['max_schedule_lag_us'] / 1000:,.3f} ms",
            ]
        )
    if peak:
        markdown.extend(
            [
                "",
                "## Highest-latency one-second interval",
                "",
                f"- Second: {peak['second']}",
                f"- Read QPS / average: {peak['read_qps']:,.1f} / {peak['read_average_ms']:,.3f} ms",
                f"- Write QPS / average: {peak['write_qps']:,.1f} / {peak['write_average_ms']:,.3f} ms",
            ]
        )
    (output_dir / "report.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")


def main():
    repository_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Run a complete RaftKeeper benchmark lifecycle")
    parser.add_argument("--profile", choices=PROFILES, required=True)
    parser.add_argument("--binary", default=str(repository_root / "build/programs/raftkeeper"))
    parser.add_argument("--servers", nargs="+", required=True)
    parser.add_argument("--bench-path", default="/raftkeeper_bench")
    parser.add_argument("--root-name", help="Stable workload root name; generated when omitted")
    parser.add_argument("--output-dir", help="Directory for manifest, logs, and metrics")
    parser.add_argument("--reuse-existing", action="store_true", help="Skip prefill and reuse the named workload root")
    parser.add_argument("--setup-only", action="store_true", help="Prefill the workload tree and exit")
    parser.add_argument("--duration", type=int)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--pipeline-depth", type=int)
    parser.add_argument("--read-qps", type=int)
    parser.add_argument("--write-qps", type=int)
    parser.add_argument("--create-qps", type=int)
    parser.add_argument("--metadata-groups", type=int)
    parser.add_argument("--children-per-list", type=int)
    parser.add_argument("--multi-size", type=int)
    parser.add_argument("--snapshot-at", type=float, help="Negative disables the profile snapshot")
    parser.add_argument("--snapshot-server", help="Optional leader endpoint for csnp")
    parser.add_argument("--monitor-interval", type=float, default=1.0)
    parser.add_argument("--operation-timeout-ms", type=int, default=35000)
    parser.add_argument("--session-timeout-ms", type=int, default=3600000)
    parser.add_argument("--drop-late-requests", action="store_true", help="Discard offered-load slots after a stall")
    parser.add_argument("--no-reset-stats", dest="reset_stats", action="store_false")
    parser.set_defaults(reset_stats=True)
    args = parser.parse_args()

    profile = PROFILES[args.profile]
    resolved = {
        key: getattr(args, key) if getattr(args, key) is not None else value
        for key, value in profile.items()
    }
    if resolved["snapshot_at"] is not None and resolved["snapshot_at"] < 0:
        resolved["snapshot_at"] = None

    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    root_name = args.root_name or f"{args.profile}_{timestamp}"
    if args.reuse_existing and not args.root_name:
        parser.error("--reuse-existing requires --root-name")

    output_dir = Path(args.output_dir or f"benchmark-results/{root_name}/{timestamp}").resolve()
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        parser.error(f"output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    common = [
        args.binary,
        "keeper-bench",
        "--server",
        *args.servers,
        "--bench-path",
        args.bench_path,
        "--workload-root-name",
        root_name,
        "--clickhouse-rmt-workload",
        "true",
        "--shared-keeper",
        "false",
        "--metadata-groups",
        str(resolved["metadata_groups"]),
        "--children-per-list",
        str(resolved["children_per_list"]),
        "--multi-size",
        str(resolved["multi_size"]),
        "--operation-timeout-ms",
        str(args.operation_timeout_ms),
        "--session-timeout-ms",
        str(args.session_timeout_ms),
    ]

    setup_command = [*common, "--setup-only", "true", "--duration-sec", "1"]
    workload_command = [
        *common,
        "--skip-setup",
        "true",
        "--concurrency",
        str(resolved["concurrency"]),
        "--pipeline-depth",
        str(resolved["pipeline_depth"]),
        "--target-read-qps",
        str(resolved["read_qps"]),
        "--target-write-qps",
        str(resolved["write_qps"]),
        "--target-create-qps",
        str(resolved["create_qps"]),
        "--drop-late-requests",
        boolean(args.drop_late_requests),
        "--duration-sec",
        str(resolved["duration"]),
        "--data-size",
        "97",
    ]

    manifest = {
        "profile": args.profile,
        "root_path": f"{args.bench_path}/{root_name}",
        "servers": args.servers,
        "resolved": resolved,
        "drop_late_requests": args.drop_late_requests,
        "operation_timeout_ms": args.operation_timeout_ms,
        "session_timeout_ms": args.session_timeout_ms,
        "setup_command": setup_command,
        "workload_command": workload_command,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    if not args.reuse_existing:
        setup_return_code = run_and_tee(setup_command, output_dir / "setup.log")
        if setup_return_code != 0:
            return setup_return_code
    if args.setup_only:
        print(f"setup_complete root={manifest['root_path']} output={output_dir}")
        return 0

    (output_dir / "state-before.json").write_text(json.dumps(collect_state(args.servers), indent=2), encoding="utf-8")
    monitor_args = SimpleNamespace(
        servers=args.servers,
        duration=resolved["duration"] + 120,
        interval=args.monitor_interval,
        snapshot_at=resolved["snapshot_at"],
        snapshot_server=args.snapshot_server,
        reset_stats=args.reset_stats,
        output=str(output_dir / "keeper-metrics.csv"),
        benchmark_output=str(output_dir / "benchmark.log"),
        start_marker="Run benchmark for",
        benchmark_command=workload_command,
    )
    return_code = run_benchmark(monitor_args)
    (output_dir / "state-after.json").write_text(json.dumps(collect_state(args.servers), indent=2), encoding="utf-8")
    if (output_dir / "keeper-metrics.csv").exists():
        generate_report(output_dir, manifest, return_code)
    print(f"benchmark_complete rc={return_code} root={manifest['root_path']} output={output_dir}")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
