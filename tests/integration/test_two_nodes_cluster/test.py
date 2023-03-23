#!/usr/bin/env python3

import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.network import PartitionManager
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/log_conf.xml'],
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
    for node in [node1, node2]:
        wait_node(node)


def test_read_write_two_nodes():
    node1_zk = node2_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()

        node1_zk.create("/test_read_write_multinode_node1", b"somedata1")
        node2_zk.create("/test_read_write_multinode_node2", b"somedata2")

        # stale reads are allowed
        while node1_zk.exists("/test_read_write_multinode_node2") is None:
            time.sleep(0.1)

        # stale reads are allowed
        while node2_zk.exists("/test_read_write_multinode_node1") is None:
            time.sleep(0.1)

        assert node2_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node1_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"

        assert node2_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node1_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"

    finally:
        close_zk_clients([node1_zk, node2_zk])


def test_read_write_two_nodes_with_blocade():
    node1_zk = node2_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()

        print("Blocking nodes")
        with PartitionManager() as pm:
            pm.partition_instances(node2, node1)

            # We will respond conection loss but process this query
            # after blocade will be removed
            with pytest.raises(Exception):
                node1_zk.create("/test_read_write_blocked_node1", b"somedata1")

            # This node is not leader and will not process anything
            with pytest.raises(Exception):
                node2_zk.create("/test_read_write_blocked_node2", b"somedata2")

        print("Nodes unblocked")

        node1.wait_for_join_cluster()
        node2.wait_for_join_cluster()

        for i in range(10):
            try:
                node1_zk = node1.get_fake_zk()
                node2_zk = node2.get_fake_zk()
                break
            except:
                close_zk_clients([node1_zk, node2_zk])
                time.sleep(0.5)

        for i in range(100):
            try:
                node1_zk.create("/test_after_block1", b"somedata12")
                break
            except:
                time.sleep(0.1)
        else:
            raise Exception("node1 cannot recover after blockade")

        print("Node1 created it's value")

        for i in range(100):
            try:
                node2_zk.create("/test_after_block2", b"somedata12")
                break
            except:
                time.sleep(0.1)
        else:
            raise Exception("node2 cannot recover after blockade")

        print("Node2 created it's value")

        # stale reads are allowed
        while node1_zk.exists("/test_after_block2") is None:
            time.sleep(0.1)

        # stale reads are allowed
        while node2_zk.exists("/test_after_block1") is None:
            time.sleep(0.1)

        assert node1_zk.exists("/test_after_block1") is not None
        assert node1_zk.exists("/test_after_block2") is not None
        assert node2_zk.exists("/test_after_block1") is not None
        assert node2_zk.exists("/test_after_block2") is not None

    finally:
        close_zk_clients([node1_zk, node2_zk])
