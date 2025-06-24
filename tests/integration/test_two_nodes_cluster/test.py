#!/usr/bin/env python3

import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.network import PartitionManager
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_keeper1.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_keeper2.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()
        yield cluster1
    finally:
        cluster1.shutdown()


def test_read_write_two_nodes(started_cluster):
    node1_zk = node2_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()

        node1_zk.create("/test_read_write_multi_node_node1", b"some_data1")
        node2_zk.create("/test_read_write_multi_node_node2", b"some_data2")

        # stale reads are allowed
        while node1_zk.exists("/test_read_write_multi_node_node2") is None:
            time.sleep(0.1)

        # stale reads are allowed
        while node2_zk.exists("/test_read_write_multi_node_node1") is None:
            time.sleep(0.1)

        assert node2_zk.get("/test_read_write_multi_node_node1")[0] == b"some_data1"
        assert node1_zk.get("/test_read_write_multi_node_node1")[0] == b"some_data1"

        assert node2_zk.get("/test_read_write_multi_node_node2")[0] == b"some_data2"
        assert node1_zk.get("/test_read_write_multi_node_node2")[0] == b"some_data2"

    finally:
        close_zk_clients([node1_zk, node2_zk])


def test_read_write_two_nodes_with_blocked(started_cluster):
    node1_zk = node2_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()

        print("Blocking nodes")
        with PartitionManager() as pm:
            pm.partition_instances(node2, node1)

            # We will respond connection loss but process this query
            # after blocked will be removed
            with pytest.raises(Exception):
                node1_zk.create("/test_read_write_blocked_node1", b"some_data1")

            # This node is not leader and will not process anything
            with pytest.raises(Exception):
                node2_zk.create("/test_read_write_blocked_node2", b"some_data2")

        print("Nodes unblocked")

        # After net partition, we should wait new cluster initialized
        node1.wait_for_join_cluster()
        node2.wait_for_join_cluster()

        # renew connection
        close_zk_clients([node1_zk, node2_zk])
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()

        node1_zk.create("/test_after_block1", b"some_data12")
        node2_zk.create("/test_after_block2", b"some_data12")

        assert node1_zk.exists("/test_after_block1") is not None
        assert node1_zk.exists("/test_after_block2") is not None
        assert node2_zk.exists("/test_after_block1") is not None
        assert node2_zk.exists("/test_after_block2") is not None

    finally:
        close_zk_clients([node1_zk, node2_zk])
