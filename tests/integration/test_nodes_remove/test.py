#!/usr/bin/env python3

import os

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients, close_zk_client

cluster = RaftKeeperCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs')

node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml'], stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml'], stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml'], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_nodes_remove(started_cluster):
    zk_conn = node1.get_fake_zk()

    for i in range(100):
        zk_conn.create("/test_two_" + str(i), b"somedata")

    zk_conn2 = node3.get_fake_zk()
    zk_conn2.sync("/test_two_0")

    zk_conn3 = node3.get_fake_zk()
    zk_conn3.sync("/test_two_0")

    for i in range(100):
        assert zk_conn2.exists("test_two_" + str(i)) is not None
        assert zk_conn3.exists("test_two_" + str(i)) is not None

    node1.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper1.xml")
    node2.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_2.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper2.xml")
    node3.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_3.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper3.xml")

    # if node3 is leader, we should wait new leader appear.
    node1.wait_for_join_cluster()
    node2.wait_for_join_cluster()
    zk_conn2 = node2.get_fake_zk()

    for i in range(100):
        assert zk_conn2.exists("test_two_" + str(i)) is not None
        zk_conn2.create("/test_two_" + str(100 + i), b"otherdata")

    zk_conn = node1.get_fake_zk()
    zk_conn.sync("/test_two_0")

    for i in range(100):
        assert zk_conn.exists("test_two_" + str(i)) is not None
        assert zk_conn.exists("test_two_" + str(100 + i)) is not None

    node1.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_single_keeper1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper1.xml")
    node2.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_single_keeper1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper2.xml")

    node1.wait_for_join_cluster()
    node2.wait_for_join_cluster()

    # renew connection
    close_zk_client(zk_conn)
    zk_conn = node1.get_fake_zk()

    zk_conn.sync("/test_two_0")

    for i in range(100):
        assert zk_conn.exists("test_two_" + str(i)) is not None
        assert zk_conn.exists("test_two_" + str(100 + i)) is not None

    close_zk_clients([zk_conn, zk_conn2, zk_conn3])
