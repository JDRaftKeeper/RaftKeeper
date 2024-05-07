#!/usr/bin/env python3
import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients
from multiprocessing.dummy import Pool

cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml'], stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml'], stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml'], stay_alive=True)


def start_raftkeeper(node):
    node.start_raftkeeper(60)
    node.wait_for_join_cluster()

def set_async_snapshot_true():
    for index, node in [(1, node1), (2, node2), (3, node3)]:
        node.stop_raftkeeper()
        node.replace_in_config(f'/etc/raftkeeper-server/config.d/enable_keeper{index}.xml', '<async_snapshot>false', '<async_snapshot>true')
        node.exec_in_container(
            ['bash', '-c', 'rm -fr /var/lib/raftkeeper/data/raft_log/* /var/lib/raftkeeper/data/raft_snapshot/*'])

    p = Pool(3)
    result = p.map_async(start_raftkeeper, [node1, node2, node3])
    result.wait()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize('async_snapshot', [False, True])
def test_restart_multinode(started_cluster, async_snapshot):
    node1_zk = node2_zk = node3_zk = None
    try:
        if async_snapshot:
            set_async_snapshot_true()

        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        for i in range(100):
            node1_zk.create("/test_read_write_multinode_node" + str(i), ("somedata" + str(i)).encode())

        for i in range(100):
            if i % 10 == 0:
                node1_zk.delete("/test_read_write_multinode_node" + str(i))

        node2_zk.sync("/test_read_write_multinode_node0")
        node3_zk.sync("/test_read_write_multinode_node0")

        for i in range(100):
            if i % 10 != 0:
                assert node2_zk.get("/test_read_write_multinode_node" + str(i))[0] == ("somedata" + str(i)).encode()
                assert node3_zk.get("/test_read_write_multinode_node" + str(i))[0] == ("somedata" + str(i)).encode()
            else:
                assert node2_zk.exists("/test_read_write_multinode_node" + str(i)) is None
                assert node3_zk.exists("/test_read_write_multinode_node" + str(i)) is None

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])

    node1.restart_raftkeeper(kill=True)
    node2.restart_raftkeeper(kill=True)
    node3.restart_raftkeeper(kill=True)

    node1.wait_for_join_cluster()
    node2.wait_for_join_cluster()
    node3.wait_for_join_cluster()

    for i in range(100):
        try:
            node1_zk = node1.get_fake_zk()
            node2_zk = node2.get_fake_zk()
            node3_zk = node3.get_fake_zk()
            for i in range(100):
                if i % 10 != 0:
                    assert node1_zk.get("/test_read_write_multinode_node" + str(i))[0] == ("somedata" + str(i)).encode()
                    assert node2_zk.get("/test_read_write_multinode_node" + str(i))[0] == ("somedata" + str(i)).encode()
                    assert node3_zk.get("/test_read_write_multinode_node" + str(i))[0] == ("somedata" + str(i)).encode()
                else:
                    assert node1_zk.exists("/test_read_write_multinode_node" + str(i)) is None
                    assert node2_zk.exists("/test_read_write_multinode_node" + str(i)) is None
                    assert node3_zk.exists("/test_read_write_multinode_node" + str(i)) is None
            break
        except Exception as ex:
            print("Got exception as ex", ex)
        finally:
            close_zk_clients([node1_zk, node2_zk, node3_zk])
