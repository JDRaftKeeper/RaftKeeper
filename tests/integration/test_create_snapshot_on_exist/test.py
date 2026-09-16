import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml'],
                              stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()
        yield cluster1
    finally:
        cluster1.shutdown()


def check_snapshot_dir(node):
    cmd = 'ls /var/lib/raftkeeper/data/raft_snapshot | wc -l'
    return node.exec_in_container(['bash', '-c', cmd])


def test_create_snapshot_on_exist(started_cluster):
    node1_zk = node2_zk = node3_zk = restarted_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.create("/test_create_snapshot_on_exist")
        for i in range(40):
            node2_zk.create(f"/test_create_snapshot_on_exist/node{i}", b"value" * 100)
        node2_zk.sync("/test_create_snapshot_on_exist")
        node3_zk.sync("/test_create_snapshot_on_exist")

        close_zk_clients([node1_zk])
        node1_zk = None
        node1.stop_raftkeeper()
        assert check_snapshot_dir(node1) != '0'
        # Shutdown must drain compaction scheduled by the final snapshot callback.
        node1.exec_in_container(['test', '!', '-e', '/var/lib/raftkeeper/data/raft_log/compacted_to'])
        node1.start_raftkeeper()
        node1.wait_for_join_cluster()
        restarted_zk = node1.get_fake_zk()
        for i in range(40):
            assert restarted_zk.get(f"/test_create_snapshot_on_exist/node{i}")[0] == b"value" * 100

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk, restarted_zk])


def snapshot_groups(node):
    groups = {}
    for name in node.list_path('/var/lib/raftkeeper/data/raft_snapshot').split():
        parts = name.split('_')
        if len(parts) == 5 and parts[0] == 'snapshot':
            groups.setdefault(int(parts[3]), []).append(name)
    return groups


def test_retained_snapshot_survives_aggressive_compaction(started_cluster):
    client = restarted = None
    try:
        client = node1.get_fake_zk()
        root = '/retained_snapshot'
        client.create(root)
        for batch in range(3):
            for i in range(10):
                client.create(f'{root}/node{batch * 10 + i}', b'retained' * 100)
            target = int(node1.send_4lw_cmd(cmd='csnp').strip())
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                groups = snapshot_groups(node1)
                info = dict(line.split() for line in node1.send_4lw_cmd(cmd='lgif').splitlines())
                if target in groups and int(info['first_log_idx']) >= target:
                    break
                time.sleep(0.1)
            else:
                pytest.fail('Snapshot and logical compaction did not complete')

        close_zk_clients([client])
        client = None
        node1.stop_raftkeeper()
        groups = snapshot_groups(node1)
        assert len(groups) >= 2
        # The latest snapshot is unavailable; all data after an older snapshot must
        # still be recoverable despite reserved_log_items=1.
        for name in groups[max(groups)]:
            if name.endswith('_1'):
                node1.exec_in_container(['truncate', '-s', '0', '/var/lib/raftkeeper/data/raft_snapshot/' + name])
        node1.start_raftkeeper()
        node1.wait_for_join_cluster()
        restarted = node1.get_fake_zk()
        for i in range(30):
            assert restarted.get(f'{root}/node{i}')[0] == b'retained' * 100
    finally:
        close_zk_clients([client, restarted])
