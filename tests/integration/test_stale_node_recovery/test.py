import pytest
import socket
import time

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml', 'configs/log_conf.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()
        yield cluster1
    finally:
        cluster1.shutdown()



def wait_node(node):
    node.wait_for_join_cluster()


def wait_nodes():
    for node in [node1, node2, node3]:
        wait_node(node)


def assert_eq_stats(stat1, stat2):
    assert stat1.version == stat2.version
    assert stat1.cversion == stat2.cversion
    assert stat1.aversion == stat2.aversion
    assert stat1.aversion == stat2.aversion
    assert stat1.dataLength == stat2.dataLength
    assert stat1.numChildren == stat2.numChildren
    assert stat1.ctime == stat2.ctime
    assert stat1.mtime == stat2.mtime


def get_keeper_socket(node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 8101))
    return client


def test_stale_node_recovery(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()

        node1_zk.create("/test_stale_node_recovery")

        for child_node in range(10):
            node1_zk.create("/test_stale_node_recovery/" + str(child_node))

        # stop node3
        node3_zk = node3.get_fake_zk()
        node3.stop_raftkeeper(kill=True)

        # make node1 leader
        data = node1.send_4lw_cmd(cmd='rqld')
        assert data == "Sent leadership request to leader."

        if not node1.is_leader():
            # pull wait to become leader
            retry = 0
            # TODO not a restrict way
            while not node1.is_leader() and retry < 30:
                time.sleep(1)
                retry += 1
            if retry == 30:
                print(
                    node1.name
                    + " does not become leader after 30s, maybe there is something wrong."
                )
        assert node1.is_leader()

        # delete some node
        node1_zk.delete("/test_stale_node_recovery/0")

        # compact log for node1(leader), so node1 has no node "/test_stale_node_recovery/0"
        last_snapshot_idx = node1.send_4lw_cmd(cmd='csnp')
        data = node1.send_4lw_cmd(cmd='lgif')
        if f"last_snapshot_idx\t{last_snapshot_idx}" not in data:
            retry = 0
            while f"last_snapshot_idx\t {last_snapshot_idx}" not in data and retry < 30:
                time.sleep(1)
                retry += 1
            if retry == 30:
                print(
                    node1.name
                    + " does not complete snapshot in 30s, maybe there is something wrong."
                )
                assert False

        # start node3
        node3.start_raftkeeper()
        node3.wait_for_join_cluster()

        # node3 should not have "/test_stale_node_recovery/0"
        node3_zk = node3.get_fake_zk()
        assert node3_zk.exists("/test_stale_node_recovery/0") is None

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])
