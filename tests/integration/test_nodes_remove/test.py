#!/usr/bin/env python3

import os
import time

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

    node2.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_2.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper2.xml")
    node1.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper1.xml")

    # The configuration update of 3 here is because 3 may be the leader at this time, and deletion of 3 requires
    # leader participation, and then 3 triggers yield_leadership re-election. In the future, when the real online
    # operation is performed, 3 may be the faulty node, and the leader should be 1 and 2. At this time,
    # the configuration of 3 does not need to be replaced.
    node3.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_3.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper3.xml")

    time.sleep(10)
    zk_conn2 = node2.get_fake_zk()

    for i in range(100):
        assert zk_conn2.exists("test_two_" + str(i)) is not None
        zk_conn2.create("/test_two_" + str(100 + i), b"otherdata")

    zk_conn = node1.get_fake_zk()
    zk_conn.sync("/test_two_0")

    for i in range(100):
        assert zk_conn.exists("test_two_" + str(i)) is not None
        assert zk_conn.exists("test_two_" + str(100 + i)) is not None

    with pytest.raises(Exception):
        zk_conn3 = node3.get_fake_zk()
        zk_conn3.sync("/test_two_0")

    node1.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_single_keeper1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper1.xml")
    node2.copy_file_to_container(os.path.join(CONFIG_DIR, "enable_single_keeper1.xml"),
                                 "/etc/raftkeeper-server/config.d/enable_keeper2.xml")

    time.sleep(10)
    close_zk_client(zk_conn)

    zk_conn = node3.get_fake_zk()
    zk_conn.sync("/test_two_0")

    for i in range(100):
        assert zk_conn.exists("test_two_" + str(i)) is not None
        assert zk_conn.exists("test_two_" + str(100 + i)) is not None

    with pytest.raises(Exception):
        zk_conn2 = node2.get_fake_zk()
        zk_conn2.sync("/test_two_0")

    close_zk_clients([zk_conn, zk_conn2, zk_conn3])
