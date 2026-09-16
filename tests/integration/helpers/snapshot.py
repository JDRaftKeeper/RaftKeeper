import json
import time
import xml.etree.ElementTree as ET


def wait_until(check, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except Exception as error:
            last = error
        time.sleep(0.2)
    raise AssertionError(f"Timed out waiting for snapshot condition: {last}")


def stop_server(node, kill=False):
    node.stop_raftkeeper(kill=kill)
    name = "raftkeeper_old" if node.use_old_bin else "raftkeeper"
    wait_until(lambda: node.get_process_pid(name) is None, timeout=40)


def configure_snapshot(node, root, codec="none", asynchronous=False, version=None,
                       server_id=1, servers=None, snapshot_dir=None, log_dir=None):
    document = ET.Element("raftkeeper")
    keeper = ET.SubElement(document, "keeper")
    for key, value in {
        "my_id": server_id, "host": node.name, "parallel": 4,
        "snapshot_dir": snapshot_dir or f"{root}/snapshots",
        "log_dir": log_dir or f"{root}/logs",
        "snapshot_create_interval": 1, "create_snapshot_on_exit": "false",
    }.items():
        ET.SubElement(keeper, key).text = str(value)
    settings = ET.SubElement(keeper, "raft_settings")
    for key, value in {
        "snapshot_compression": codec, "async_snapshot": str(asynchronous).lower(),
        "snapshot_distance": 1000000, "max_stored_snapshots": 1 if servers else 5,
        "nuraft_thread_size": 4, "shutdown_timeout": 5000,
    }.items():
        ET.SubElement(settings, key).text = str(value)
    if version is not None:
        ET.SubElement(settings, "snapshot_format_version").text = str(version)
    if servers:
        # Compaction removes whole segments. Force rotation so this small fixture
        # actually makes append_entries insufficient for the stopped follower.
        ET.SubElement(settings, "max_log_segment_file_size").text = "4096"
        cluster = ET.SubElement(keeper, "cluster")
        for number, host in enumerate(servers, 1):
            server = ET.SubElement(cluster, "server")
            ET.SubElement(server, "id").text = str(number)
            ET.SubElement(server, "host").text = host
    node.exec_in_container([
        "python3", "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).write_text(sys.argv[2])",
        "/etc/raftkeeper-server/config.d/snapshot.xml", ET.tostring(document, encoding="unicode"),
    ])


def manifest(node, directory):
    script = """
import hashlib, json, sys
from pathlib import Path
result = {}
for path in Path(sys.argv[1]).glob('snapshot_*'):
    data = path.read_bytes()
    result[path.name] = {'header': list(data[:16]), 'tail': data[-12:-4].decode('ascii', 'replace'),
                         'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
print(json.dumps(result))
"""
    return json.loads(node.exec_in_container(["python3", "-c", script, directory]))


def assert_snapshot_files(files, version, codec):
    assert files, "snapshot has no objects"
    groups = {}
    for name, record in files.items():
        prefix, number = name.rsplit("_", 1)
        groups.setdefault(prefix, []).append(int(number))
        assert bytes(record["header"][:8]) == b"SnapHead"
        assert record["header"][8] == version
        assert record["tail"] == "SnapTail"
        if version == 4:
            assert record["header"][9] == (1 if codec == "zstd" else 0)
            assert record["header"][10:16] == [0] * 6
    for numbers in groups.values():
        # These integration fixtures fit into one data object.
        assert sorted(numbers) == list(range(1, 3 if version == 4 else 5))
    return groups


def force_snapshot(node, directory, codec="none", version=4):
    def schedule():
        response = node.send_4lw_cmd("csnp").strip()
        return int(response) if response.isdigit() and int(response) > 0 else None
    index = wait_until(schedule)

    def complete():
        files = manifest(node, directory)
        selected = {name: record for name, record in files.items()
                    if int(name.rsplit("_", 2)[1]) >= index}
        groups = assert_snapshot_files(selected, version, codec)
        return max(groups, key=lambda prefix: int(prefix.rsplit("_", 1)[1]))
    return wait_until(complete)


def converter(node, arguments, file_size_limit=0):
    script = """
import json, resource, signal, subprocess, sys
limit = int(sys.argv[1])
def limit_output():
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
p = subprocess.run(['/usr/bin/raftkeeper', 'converter'] + sys.argv[2:],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                   preexec_fn=limit_output if limit else None)
print(json.dumps({'code': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr}))
"""
    return json.loads(node.exec_in_container(["python3", "-c", script, str(file_size_limit)] + arguments))


def assert_no_output(node, destination):
    script = """
from pathlib import Path
import sys
output = Path(sys.argv[1])
assert not output.exists() and not output.is_symlink(), str(output)
leftovers = list(output.parent.glob(output.name + '.tmp.*'))
assert not leftovers, str(leftovers)
"""
    node.exec_in_container(["python3", "-c", script, destination])


def snapshot_log_position(node):
    return int(node.exec_in_container([
        "python3", "-c", "from pathlib import Path; print(Path('/var/log/raftkeeper-server/raftkeeper-server.log').stat().st_size)",
    ]).strip())


def applied_snapshot_since(node, position, minimum_index):
    script = """
import re, sys
with open('/var/log/raftkeeper-server/raftkeeper-server.log', 'rb') as log:
    log.seek(int(sys.argv[1]))
    indices = re.findall(rb'Applied snapshot, now the last log index is (\\d+)', log.read())
print(int(any(int(index) >= int(sys.argv[2]) for index in indices)))
"""
    return node.exec_in_container(["python3", "-c", script, str(position), str(minimum_index)]).strip() == "1"


def log_info(node):
    return {key: int(value) for key, value in
            (line.split("\t", 1) for line in node.send_4lw_cmd("lgif").strip().splitlines())}
