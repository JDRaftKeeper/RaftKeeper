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
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.create("/test_create_snapshot_on_exist")
        node2_zk.sync("/test_create_snapshot_on_exist")
        node3_zk.sync("/test_create_snapshot_on_exist")

        node1.stop_raftkeeper()
        assert check_snapshot_dir(node1) != '0'

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])

