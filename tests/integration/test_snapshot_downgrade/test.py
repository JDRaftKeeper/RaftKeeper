"""Default V4 snapshots, CLI downgrades, and real 2.1.1 snapshot-only recovery.

Run with --old-binary pointing to the official 2.1.1 ZooKeeper-mode binary.
This suite deliberately fails if that binary is missing or has another version.
"""
import re
import uuid

import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import NoAuthError
from kazoo.security import make_digest_acl

from helpers.cluster_service import RaftKeeperCluster
from helpers.snapshot import (assert_no_output, assert_snapshot_files, configure_snapshot, converter,
                              force_snapshot, log_info, manifest, stop_server, wait_until)

cluster = RaftKeeperCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/snapshot.xml"], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
def fresh_root(started_cluster):
    stop_server(node)
    node.use_old_bin = False
    return "/var/lib/raftkeeper/snapshot-tests/" + uuid.uuid4().hex


def start(codec, asynchronous, root, snapshot_dir=None, log_dir=None, old=False, version=None):
    configure_snapshot(node, root, codec, asynchronous, version=version, snapshot_dir=snapshot_dir, log_dir=log_dir)
    node.use_old_bin = old
    node.start_raftkeeper(start_wait=True)
    node.wait_for_join_cluster()


def resume(session_id):
    client = KazooClient(hosts=cluster.get_instance_ip("node") + ":8101", timeout=300, client_id=session_id)
    try:
        client.start(timeout=60)
        assert client.client_id[0] == session_id[0], "Session was replaced instead of restored"
    except Exception:
        client.stop()
        client.close()
        raise
    return client


def require_old_binary():
    version = node.exec_in_container(["/usr/bin/raftkeeper_old", "server", "--version"])
    assert re.fullmatch(r"RaftKeeper v2\.1\.1\.\s*", version), f"Expected genuine 2.1.1 binary, got: {version}"


@pytest.mark.parametrize("codec", ["none", "zstd"])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_default_v4_downgrade_and_old_binary_restore(fresh_root, codec, asynchronous):
    require_old_binary()
    root = fresh_root
    start(codec, asynchronous, root)
    client = node.get_fake_zk(session_timeout=300)
    try:
        client.add_auth("digest", "snapshot-user:password")
        acl = [make_digest_acl("snapshot-user", "password", all=True)]
        client.create("/private", b"private", acl=acl)
        client.set("/private", b"updated private")
        client.create("/ephemeral", b"session data", ephemeral=True)
        for i in range(40):
            client.create(f"/key{i}", (f"value{i}" * 20).encode())
        session_id = client.client_id
        expected_stat = client.get("/private")[1]
        expected_acl = client.get_acls("/private")[0]
        prefix = force_snapshot(node, root + "/snapshots", codec)
        stop_server(node)
        client.stop()
        client.close()
        client = None
        original = manifest(node, root + "/snapshots")

        def convert(source, target, version, extra=None):
            result = converter(node, ["--raftkeeper-snapshots-dir", source, "--output-dir", target,
                                      "--target-snapshot-version", str(version)] + (extra or []))
            assert result["code"] == 0, result
            assert "Snapshot conversion does not convert Raft logs" in result["stdout"]
            assert_snapshot_files(manifest(node, target), version, "zstd" if version == 3 else "none")

        convert(root + "/snapshots", root + "/v2", 2, ["--snapshot-prefix", prefix])
        convert(root + "/snapshots", root + "/v3", 3)
        convert(root + "/v3", root + "/v3-to-v2", 2)
        assert manifest(node, root + "/snapshots") == original

        # New binary must restore V4 and V3; genuine old binary must restore converted V2.
        # Every run gets an empty log directory, so no Raft log can hide snapshot data loss.
        for label, old in [("snapshots", False), ("v3", False), ("v2", True), ("v3-to-v2", True)]:
            start(codec, asynchronous, root, snapshot_dir=root + "/" + label,
                  log_dir=root + "/restore-logs-" + label, old=old)
            client = resume(session_id)
            assert client.get("/private") == (b"updated private", expected_stat)
            assert client.get_acls("/private")[0] == expected_acl
            assert client.get("/ephemeral")[0] == b"session data"
            assert client.get("/ephemeral")[1].ephemeralOwner == session_id[0]
            unauthorized = node.get_fake_zk(session_timeout=30)
            try:
                with pytest.raises(NoAuthError):
                    unauthorized.get("/private")
            finally:
                unauthorized.stop()
                unauthorized.close()
            for i in range(40):
                assert client.get(f"/key{i}")[0] == (f"value{i}" * 20).encode()
            stop_server(node)
            client.stop()
            client.close()
            client = None
        assert manifest(node, root + "/snapshots") == original
    finally:
        if client is not None:
            client.stop()
            client.close()


def test_converter_cli_errors_and_latest_selection(fresh_root):
    root = fresh_root
    start("none", False, root)
    client = node.get_fake_zk(session_timeout=300)
    try:
        client.create("/value", b"first")
        first_stat = client.get("/value")[1]
        first = force_snapshot(node, root + "/snapshots")
        client.set("/value", b"latest")
        latest_stat = client.get("/value")[1]
        latest = force_snapshot(node, root + "/snapshots")
        assert latest != first
        stop_server(node)
        client.stop()
        client.close()
        original = manifest(node, root + "/snapshots")
        base = ["--raftkeeper-snapshots-dir", root + "/snapshots"]
        result = converter(node, base + ["--target-snapshot-version", "2", "--output-dir", root + "/latest/"])
        assert result["code"] == 0, result
        assert latest in result["stdout"]
        result = converter(node, base + ["--target-snapshot-version", "2", "--output-dir", root + "/first///", "--snapshot-prefix", first])
        assert result["code"] == 0, result
        assert first in result["stdout"]
        node.exec_in_container([
            "python3", "-c",
            "from pathlib import Path; import sys; root=Path(sys.argv[1]); "
            "(root/'empty-existing').mkdir(); (root/'source-alias').symlink_to(root/'snapshots', target_is_directory=True); "
            "(root/'dangling').symlink_to(root/'absent', target_is_directory=True)", root,
        ])
        bad_arguments = [
            [], ["--not-an-option"], base, base + ["--output-dir", root + "/missing-target"],
            base + ["--target-snapshot-version", "0", "--output-dir", root + "/invalid"],
            base + ["--target-snapshot-version", "4", "--output-dir", root + "/same-version"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/latest"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/latest/"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/snapshots/child"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/source-alias/child"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/empty-existing"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/dangling"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/dangling/"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/missing-parent/output"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/missing-prefix", "--snapshot-prefix", "missing"],
            base + ["--target-snapshot-version", "2", "--output-dir", root + "/mixed", "--zookeeper-logs-dir", root],
            ["--target-snapshot-version", "2", "--output-dir", root + "/no-source"],
            ["--raftkeeper-snapshots-dir", root + "/latest", "--target-snapshot-version", "3", "--output-dir", root + "/upgrade"],
            ["--raftkeeper-snapshots-dir", root + "/latest", "--target-snapshot-version", "2", "--output-dir", root + "/same-v2"],
        ]
        for args in bad_arguments:
            result = converter(node, args)
            assert result["code"] != 0, (args, result)
            assert result["stderr"], result
        # Force a real mid-write EFBIG after the staging directory has been created.
        result = converter(node, base + ["--target-snapshot-version", "2", "--output-dir", root + "/write-failure"],
                           file_size_limit=128)
        assert result["code"] != 0, result
        assert_no_output(node, root + "/write-failure")
        assert converter(node, ["--help"])["code"] == 0
        assert manifest(node, root + "/snapshots") == original
        # Check the selected contents and recovery index, not just the printed prefix.
        for label, prefix, value, stat in [("first", first, b"first", first_stat),
                                           ("latest", latest, b"latest", latest_stat)]:
            start("none", False, root, snapshot_dir=root + "/" + label, log_dir=root + "/selection-logs-" + label)
            client = node.get_fake_zk(session_timeout=30)
            assert client.get("/value") == (value, stat)
            assert log_info(node)["last_snapshot_idx"] == int(prefix.rsplit("_", 1)[1])
            stop_server(node)
            client.stop()
            client.close()
    finally:
        client.stop()
        client.close()


@pytest.mark.parametrize("codec", ["none", "zstd"])
def test_corrupt_latest_snapshot_never_falls_back(fresh_root, codec):
    root = fresh_root
    start(codec, True, root)
    client = node.get_fake_zk(session_timeout=300)
    try:
        client.create("/value", b"earlier")
        earlier = force_snapshot(node, root + "/snapshots", codec)
        client.set("/value", b"latest")
        latest = force_snapshot(node, root + "/snapshots", codec)
        assert latest != earlier
        stop_server(node)
        client.stop()
        client.close()
        original = manifest(node, root + "/snapshots")
        faults = [
            ("version", "Unsupported snapshot version"),
            ("codec", "Unsupported snapshot codec"),
            ("flags", "Unsupported snapshot flags"),
            ("reserved", "Unsupported snapshot flags"),
            ("header", "Cannot read all data"),  # CLI rejects it during header preflight
            ("metadata_crc", "crc not match"),
            ("data_crc", "crc not match"),
            ("tail", "load magic error"),
            ("missing_metadata", "Loading snapshot objects error"),
            ("missing_data", "Loading snapshot objects error"),
            ("mixed_versions", "Mixed snapshot versions"),
            ("duplicate", "Duplicate snapshot object"),
            ("ambiguous", "Ambiguous latest snapshot"),
        ]
        # Recompute valid checksums after removing individual records. Compressed
        # missing-record inputs are also covered by SnapshotFormatTest unit tests.
        if codec == "none":
            faults += [("missing_zxid", "does not contain ZXID"),
                       ("missing_sessionid", "does not contain SESSIONID"),
                       ("missing_objectcount", "does not contain OBJECTCOUNT"),
                       ("missing_root", "does not contain root node /")]
        damage_script = """
from pathlib import Path
import shutil, struct, sys, zlib
source, destination, prefix, fault = sys.argv[1:]
shutil.copytree(source, destination)
root = Path(destination)
metadata, data = root / (prefix + '_1'), root / (prefix + '_2')
if fault == 'missing_metadata':
    metadata.unlink()
elif fault == 'missing_data':
    data.unlink()
elif fault == 'duplicate':
    shutil.copyfile(metadata, root / (prefix + '_01'))
elif fault == 'ambiguous':
    parts = prefix.split('_')
    parts[1] = '20990101010101'
    alternate = '_'.join(parts)
    for path in list(root.glob(prefix + '_*')):
        shutil.copyfile(path, root / path.name.replace(prefix, alternate, 1))
elif fault in ('missing_zxid', 'missing_sessionid', 'missing_objectcount', 'missing_root'):
    path = data if fault == 'missing_root' else metadata
    content = path.read_bytes()
    assert content[:10] == b'SnapHead\\x04\\x00'
    rewritten = bytearray(content[:16])
    offset, checksum, removed = 16, 0, 0
    expected_type = 0 if fault == 'missing_root' else 6
    key = b'/' if fault == 'missing_root' else fault[len('missing_'):].upper().encode()
    while offset < len(content) - 12:
        length, original_crc = struct.unpack_from('<II', content, offset)
        body = content[offset + 8:offset + 8 + length]
        assert zlib.crc32(body, 0xffffffff) == original_crc
        batch_type, count = struct.unpack_from('<ii', body)
        position, elements = 8, []
        for _ in range(count):
            size, = struct.unpack_from('<i', body, position)
            element = body[position + 4:position + 4 + size]
            position += 4 + size
            if batch_type == expected_type and element.startswith(struct.pack('>i', len(key)) + key):
                removed += 1
            else:
                elements.append(element)
        assert position == len(body)
        body = struct.pack('<ii', batch_type, len(elements))
        body += b''.join(struct.pack('<i', len(element)) + element for element in elements)
        crc = zlib.crc32(body, 0xffffffff)
        rewritten += struct.pack('<II', len(body), crc) + body
        checksum = zlib.crc32(struct.pack('<II', checksum, crc), 0xffffffff)
        offset += 8 + length
    assert removed == 1 and offset == len(content) - 12
    rewritten += b'SnapTail' + struct.pack('<I', checksum)
    path.write_bytes(rewritten)
else:
    path = data if fault in ('data_crc', 'mixed_versions') else metadata
    content = bytearray(path.read_bytes())
    if fault == 'header':
        content = content[:15]
    elif fault == 'tail':
        content = content[:-5]
    elif fault == 'mixed_versions':
        content[8] = 3
    else:
        offset = {'version': 8, 'codec': 9, 'flags': 10, 'reserved': 12,
                  'metadata_crc': 24, 'data_crc': 24}[fault]
        content[offset] ^= 255
    path.write_bytes(content)
"""
        for fault, diagnostic in faults:
            source = root + "/fault-" + fault
            output = root + "/output-" + fault
            node.exec_in_container(["python3", "-c", damage_script, root + "/snapshots", source, latest, fault])
            damaged = manifest(node, source)
            args = ["--raftkeeper-snapshots-dir", source, "--output-dir", output, "--target-snapshot-version", "2"]
            result = converter(node, args)
            assert result["code"] != 0, (fault, result)
            assert diagnostic in result["stderr"], (fault, result)
            assert_no_output(node, output)
            assert manifest(node, source) == damaged, fault
            if fault in ("version", "missing_data", "ambiguous"):
                explicit = converter(node, ["--raftkeeper-snapshots-dir", source, "--output-dir", output,
                                            "--target-snapshot-version", "2", "--snapshot-prefix", earlier])
                assert explicit["code"] == 0, (fault, explicit)
                assert earlier in explicit["stdout"]
                assert_snapshot_files(manifest(node, output), 2, "none")
        assert manifest(node, root + "/snapshots") == original
    finally:
        client.stop()
        client.close()


@pytest.mark.parametrize("codec", ["none", "zstd"])
def test_old_binary_can_write_and_restart_after_downgrade(fresh_root, codec):
    require_old_binary()
    root = fresh_root
    start(codec, True, root)
    client = node.get_fake_zk(session_timeout=300)
    newcomer = None
    try:
        client.add_auth("digest", "writer:password")
        client.create("/private", b"before", acl=[make_digest_acl("writer", "password", all=True)])
        client.create("/ephemeral", b"original session", ephemeral=True)
        client.create("/seq", b"")
        removed = client.create("/seq/n-", b"remove", sequence=True)
        remaining = client.create("/seq/n-", b"before", sequence=True)
        client.delete(removed)
        client.set(remaining, b"changed")
        session_id = client.client_id
        force_snapshot(node, root + "/snapshots", codec)
        stop_server(node)
        client.stop()
        client.close()
        client = None
        original = manifest(node, root + "/snapshots")
        result = converter(node, ["--raftkeeper-snapshots-dir", root + "/snapshots", "--output-dir", root + "/v2",
                                  "--target-snapshot-version", "2"])
        assert result["code"] == 0, result
        start("none", False, root, snapshot_dir=root + "/v2", log_dir=root + "/old-logs", old=True)
        client = resume(session_id)
        assert client.get("/private")[0] == b"before"  # persisted auth, without add_auth
        assert client.create("/seq/n-", b"after", sequence=True) == "/seq/n-0000000002"
        transaction = client.transaction()
        transaction.set_data("/private", b"after", version=0)
        transaction.create("/committed", b"atomic")
        results = transaction.commit()
        assert len(results) == 2 and not any(isinstance(value, Exception) for value in results), results

        newcomer = node.get_fake_zk(session_timeout=30)
        assert newcomer.client_id[0] > session_id[0], "Session counter was not restored"
        with pytest.raises(NoAuthError):
            newcomer.get("/private")
        newcomer.create("/new-session", b"temporary", ephemeral=True)
        newcomer.stop()
        newcomer.close()
        newcomer = None
        wait_until(lambda: client.exists("/new-session") is None, timeout=60)

        # Keep the converted snapshot unchanged: recovery must replay the old binary's new log tail.
        converted = manifest(node, root + "/v2")
        stop_server(node)
        client.stop()
        client.close()
        client = None
        assert manifest(node, root + "/v2") == converted
        start("none", False, root, snapshot_dir=root + "/v2", log_dir=root + "/old-logs", old=True)
        client = resume(session_id)
        assert client.get("/private")[0] == b"after"
        assert client.get("/private")[1].version == 1
        assert client.get("/committed")[0] == b"atomic"
        assert client.get(remaining)[0] == b"changed"
        assert client.exists(removed) is None
        assert client.get("/ephemeral")[1].ephemeralOwner == session_id[0]
        assert client.create("/seq/n-", b"after restart", sequence=True) == "/seq/n-0000000003"
        assert manifest(node, root + "/snapshots") == original
    finally:
        for connection in (client, newcomer):
            if connection is not None:
                connection.stop()
                connection.close()


@pytest.mark.parametrize("legacy_codec", ["none", "zstd"])
def test_legacy_to_default_v4_codec_switch_and_hard_restart(fresh_root, legacy_codec):
    root = fresh_root
    start(legacy_codec, False, root, version=2)
    client = node.get_fake_zk(session_timeout=300)
    try:
        client.create("/value", b"legacy")
        legacy_stat = client.get("/value")[1]
        legacy_version = 2 if legacy_codec == "none" else 3
        legacy_prefix = force_snapshot(node, root + "/snapshots", legacy_codec, version=legacy_version)
        stop_server(node)
        client.stop()
        client.close()
        legacy_files = manifest(node, root + "/snapshots")
        codec = "zstd" if legacy_codec == "none" else "none"
        # No version override: V4 must be the actual default, even while older snapshots coexist.
        start(codec, True, root)
        client = node.get_fake_zk(session_timeout=300)
        assert client.get("/value") == (b"legacy", legacy_stat)
        client.set("/value", b"v4")
        expected_stat = client.get("/value")[1]
        prefix = force_snapshot(node, root + "/snapshots", codec)
        assert prefix != legacy_prefix
        files = manifest(node, root + "/snapshots")
        for name, record in legacy_files.items():
            assert files[name] == record
        stop_server(node, kill=True)
        client.stop()
        client.close()
        # Change the configured codec again and start with no logs: reads must use the file's codec.
        start(legacy_codec, True, root, log_dir=root + "/empty-restart-logs")
        client = node.get_fake_zk(session_timeout=300)
        assert client.get("/value") == (b"v4", expected_stat)
        assert log_info(node)["last_snapshot_idx"] == int(prefix.rsplit("_", 1)[1])
    finally:
        client.stop()
        client.close()
