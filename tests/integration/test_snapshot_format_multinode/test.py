"""Exercise real NuRaft snapshot installation after the leader compacts the log."""
from concurrent.futures import ThreadPoolExecutor
import uuid

import pytest
from kazoo.exceptions import NoAuthError
from kazoo.security import make_digest_acl

from helpers.cluster_service import RaftKeeperCluster
from helpers.snapshot import (applied_snapshot_since, assert_snapshot_files, configure_snapshot, force_snapshot,
                              log_info, manifest, snapshot_log_position, stop_server, wait_until)

cluster = RaftKeeperCluster(__file__)
nodes = [cluster.add_instance(f"node{i}", main_configs=[f"configs/node{i}/snapshot.xml"], stay_alive=True)
         for i in range(1, 4)]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("codec", ["none", "zstd"])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_v4_snapshot_install_after_log_compaction(started_cluster, codec, asynchronous):
    root = "/var/lib/raftkeeper/snapshot-multinode/" + uuid.uuid4().hex
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(stop_server, nodes))
    for i, node in enumerate(nodes, 1):
        configure_snapshot(node, root, codec, asynchronous, server_id=i, servers=[n.name for n in nodes])
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda node: node.start_raftkeeper(start_wait=True), nodes))
    for node in nodes:
        node.wait_for_join_cluster()

    def elected():
        leaders = [node for node in nodes if node.is_leader()]
        return leaders[0] if len(leaders) == 1 else None
    leader = wait_until(elected)
    lagging = next(node for node in reversed(nodes) if node != leader)
    stopped_index = log_info(lagging)["last_committed_log_idx"]
    stop_server(lagging)
    log_position = snapshot_log_position(lagging)
    client = leader.get_fake_zk(session_timeout=120)
    reader = None
    try:
        client.add_auth("digest", "cluster-user:password")
        client.create("/private", b"protected", acl=[make_digest_acl("cluster-user", "password", all=True)])
        client.create("/ephemeral", b"owner", ephemeral=True)
        private_stat = client.get("/private")[1]
        private_acl = client.get_acls("/private")[0]
        for i in range(100):
            client.create(f"/key{i}", (f"value{i}" * 100).encode())
        prefix = force_snapshot(leader, root + "/snapshots", codec)
        snapshot_index = int(prefix.rsplit("_", 1)[1])
        # This proves the follower cannot catch up solely through append_entries.
        wait_until(lambda: log_info(leader)["first_log_idx"] > stopped_index)
        client.set("/key0", b"updated after snapshot")
        client.create("/after-snapshot", b"log tail")
        lagging.start_raftkeeper(start_wait=True)
        lagging.wait_for_join_cluster()
        reader = lagging.get_fake_zk(session_timeout=120)
        wait_until(lambda: reader.exists("/after-snapshot"))
        for i in range(100):
            expected = b"updated after snapshot" if i == 0 else (f"value{i}" * 100).encode()
            assert reader.get(f"/key{i}")[0] == expected
        assert reader.get("/after-snapshot")[0] == b"log tail"
        with pytest.raises(NoAuthError):
            reader.get("/private")
        reader.add_auth("digest", "cluster-user:password")
        assert reader.get("/private") == (b"protected", private_stat)
        assert reader.get_acls("/private")[0] == private_acl
        assert reader.get("/ephemeral")[1].ephemeralOwner == client.client_id[0]
        wait_until(lambda: applied_snapshot_since(lagging, log_position, snapshot_index))
        assert log_info(lagging)["last_snapshot_idx"] >= snapshot_index
        assert_snapshot_files(manifest(lagging, root + "/snapshots"), 4, codec)
        client.stop()
        client.close()
        client = None
        # The restored ephemeral map must drive cleanup, not just expose the owner in Stat.
        wait_until(lambda: reader.exists("/ephemeral") is None)
    finally:
        if client is not None:
            client.stop()
            client.close()
        if reader is not None:
            reader.stop()
            reader.close()
