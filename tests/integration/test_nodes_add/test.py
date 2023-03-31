#!/usr/bin/env python3

import os
import time
from multiprocessing.dummy import Pool

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


def start(node):
    node.start_raftkeeper(start_wait=True)


def test_nodes_add(started_cluster):
    zk_conn = node1.get_fake_zk()

    for i in range(100):
        zk_conn.create("/test_two_" + str(i), b"somedata")

    p = Pool(3)
    node2.stop_raftkeeper()
    node2.exec_in_container(
        ['bash', '-c', 'rm -fr /var/lib/raftkeeper/data/raft_log/* /var/lib/raftkeeper/data/raft_snapshot/*'])
    node2.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_2.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper2.xml")
    waiter = p.apply_async(start, (node2,))
    node1.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper1.xml")

    # sleep at least 3s, for ConfigReloader monitor config file change every 2s
    time.sleep(3)

    waiter.wait()
    zk_conn2 = node2.get_fake_zk()

    for i in range(100):
        assert zk_conn2.exists("/test_two_" + str(i)) is not None

    close_zk_client(zk_conn)
    zk_conn = node1.get_fake_zk()

    for i in range(100):
        zk_conn.create("/test_three_" + str(i), b"somedata")

    node3.stop_raftkeeper()

    node3.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_3.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper3.xml")
    waiter = p.apply_async(start, (node3,))
    node2.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_2.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper2.xml")
    node1.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper1.xml")

    # sleep at least 3s, for ConfigReloader monitor config file change every 2s
    time.sleep(3)

    waiter.wait()
    zk_conn3 = node3.get_fake_zk()

    for i in range(100):
        assert zk_conn3.exists("/test_three_" + str(i)) is not None

    # test forward connections is fine
    for i in range(100):
        zk_conn.create("/test_nodes_add1_" + str(i), b"somedata")
        zk_conn2.create("/test_nodes_add2_" + str(i), b"somedata")
        zk_conn3.create("/test_nodes_add3_" + str(i), b"somedata")

    for i in range(100):
        assert zk_conn3.exists("/test_nodes_add1_" + str(i)) is not None
        assert zk_conn3.exists("/test_nodes_add2_" + str(i)) is not None
        assert zk_conn3.exists("/test_nodes_add3_" + str(i)) is not None

    close_zk_clients([zk_conn, zk_conn2, zk_conn3])
