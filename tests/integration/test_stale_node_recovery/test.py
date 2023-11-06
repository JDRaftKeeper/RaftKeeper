import pytest
import socket
import time

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml', 'configs/log_conf.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_stale_node_recovery(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()

        node1_zk.create("/test_stale_node_recovery")

        for child_node in range(10):
            node1_zk.create("/test_stale_node_recovery/" + str(child_node))

        # make node1 leader
        data = node1.send_4lw_cmd(cmd='rqld')
        assert data == "Sent leadership request to leader."
        # pull wait to become leader
        retry = 0
        while not node1.is_leader() and retry < 30:
            time.sleep(1)
            retry += 1
        if retry == 30:
            print("node1 does not become leader in 30s, maybe there is something wrong.")
        assert node1.is_leader()

        # stop node3
        node3_zk = node3.get_fake_zk()
        node3.stop_raftkeeper(kill=True)

        # delete some node
        node1_zk.delete("/test_stale_node_recovery/0")

        # creating snapshot which will compact log, so node1(leader) has no log for deleting "/test_stale_node_recovery/0"
        last_snapshot_idx = node1.send_4lw_cmd(cmd='csnp')
        data = node1.send_4lw_cmd(cmd='lgif')
        retry = 0
        while f"last_snapshot_idx\t{last_snapshot_idx}" not in data and retry < 30:
            time.sleep(1)
            retry += 1
            data = node1.send_4lw_cmd(cmd='lgif')
        if f"last_snapshot_idx\t{last_snapshot_idx}" not in data:
            print("node1 does not complete snapshot in 30s, maybe there is something wrong.")
            assert False

        # start node3, node3 will load snapshot from node1
        node3.start_raftkeeper()
        node3.wait_for_join_cluster()

        # node3 should not have "/test_stale_node_recovery/0"
        node3_zk = node3.get_fake_zk()
        assert node3_zk.exists("/test_stale_node_recovery/0") is None

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])
