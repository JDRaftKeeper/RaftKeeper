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


def smaller_exception(ex):
    return '\n'.join(str(ex).split('\n')[0:2])


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


def test_between_servers(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.create("/test_between_servers")
        for child_node in range(1000):
            node1_zk.create("/test_between_servers/" + str(child_node))

        for child_node in range(1000):
            node1_zk.set("/test_between_servers/" + str(child_node), b"somevalue")

        for child_node in range(1000):
            stats1 = node1_zk.exists("/test_between_servers/" + str(child_node))
            stats2 = node2_zk.exists("/test_between_servers/" + str(child_node))
            stats3 = node3_zk.exists("/test_between_servers/" + str(child_node))
            assert_eq_stats(stats1, stats2)
            assert_eq_stats(stats2, stats3)

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])


def test_server_restart(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()

        node1_zk.create("/test_server_restart")
        for child_node in range(1000):
            node1_zk.create("/test_server_restart/" + str(child_node))

        for child_node in range(1000):
            node1_zk.set("/test_server_restart/" + str(child_node), b"somevalue")

        node3.restart_raftkeeper(kill=True)
        node3.wait_for_join_cluster()

        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        for child_node in range(1000):
            stats1 = node1_zk.exists("/test_server_restart/" + str(child_node))
            stats2 = node2_zk.exists("/test_server_restart/" + str(child_node))
            stats3 = node3_zk.exists("/test_server_restart/" + str(child_node))
            assert_eq_stats(stats1, stats2)
            assert_eq_stats(stats2, stats3)

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])
