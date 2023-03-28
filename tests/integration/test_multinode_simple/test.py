import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.network import PartitionManager
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


def test_read_write_multi_node(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node2.get_fake_zk()

        node1_zk.create("/ttest_read_write_multi_node_node1", b"somedata1")
        node2_zk.create("/ttest_read_write_multi_node_node2", b"somedata2")
        node3_zk.create("/ttest_read_write_multi_node_node3", b"somedata3")

        # stale reads are allowed
        while node1_zk.exists("/ttest_read_write_multi_node_node2") is None:
            time.sleep(0.1)

        while node1_zk.exists("/ttest_read_write_multi_node_node3") is None:
            time.sleep(0.1)

        while node2_zk.exists("/ttest_read_write_multi_node_node3") is None:
            time.sleep(0.1)

        assert node3_zk.get("/ttest_read_write_multi_node_node1")[0] == b"somedata1"
        assert node2_zk.get("/ttest_read_write_multi_node_node1")[0] == b"somedata1"
        assert node1_zk.get("/ttest_read_write_multi_node_node1")[0] == b"somedata1"

        assert node3_zk.get("/ttest_read_write_multi_node_node2")[0] == b"somedata2"
        assert node2_zk.get("/ttest_read_write_multi_node_node2")[0] == b"somedata2"
        assert node1_zk.get("/ttest_read_write_multi_node_node2")[0] == b"somedata2"

        assert node3_zk.get("/ttest_read_write_multi_node_node3")[0] == b"somedata3"
        assert node2_zk.get("/ttest_read_write_multi_node_node3")[0] == b"somedata3"
        assert node1_zk.get("/ttest_read_write_multi_node_node3")[0] == b"somedata3"

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])


def test_watch_on_follower(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.create("/test_data_watches")
        node2_zk.set("/test_data_watches", b"hello")
        node3_zk.set("/test_data_watches", b"world")

        node1_data = None

        def node1_callback(event):
            print("node1 data watch called")
            nonlocal node1_data
            node1_data = event

        node1_zk.get("/test_data_watches", watch=node1_callback)

        node2_data = None

        def node2_callback(event):
            print("node2 data watch called")
            nonlocal node2_data
            node2_data = event

        node2_zk.get("/test_data_watches", watch=node2_callback)

        node3_data = None

        def node3_callback(event):
            print("node3 data watch called")
            nonlocal node3_data
            node3_data = event

        node3_zk.get("/test_data_watches", watch=node3_callback)

        node1_zk.set("/test_data_watches", b"somevalue")
        time.sleep(3)

        print(node1_data)
        print(node2_data)
        print(node3_data)

        assert node1_data == node2_data
        assert node3_data == node2_data

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])


def test_session_expiration(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk(session_timeout=3)

        print("Node3 session id", node3_zk._session_id)

        node3_zk.create("/test_ephemeral_node", b"world", ephemeral=True)
        time.sleep(1)

        with PartitionManager() as pm:
            pm.partition_instances(node3, node2)
            pm.partition_instances(node3, node1)

            # node3 maybe leader, we should wait a new leader
            node1.wait_for_join_cluster()
            node2.wait_for_join_cluster()

            # sleep 3s and node3_zk will expire
            time.sleep(3)

            # renew connection
            close_zk_clients([node1_zk, node2_zk])
            node1_zk = node1.get_fake_zk()
            node2_zk = node2.get_fake_zk()

            print("Node1 exists", node1_zk.exists("/test_ephemeral_node"))
            print("Node2 exists", node2_zk.exists("/test_ephemeral_node"))

        assert node1_zk.exists("/test_ephemeral_node") is None
        assert node2_zk.exists("/test_ephemeral_node") is None

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])


def test_follower_restart(started_cluster):
    node1_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node1_zk.create("/test_restart_node", b"hello")

        node3.restart_raftkeeper(kill=True)
        node3.wait_for_join_cluster()

        node3_zk = node3.get_fake_zk()

        # got data from log
        assert node3_zk.get("/test_restart_node")[0] == b"hello"

    finally:
        close_zk_clients([node1_zk, node3_zk])


def test_simple_sleep_test(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node2.get_fake_zk(session_timeout=3)

        print("Node3 session id", node3_zk._session_id)

        node2_zk.get("/")
        node2_zk.create("/persistent_node", b"", ephemeral=False)
        node2_zk.get("/persistent_node")
        node2_zk.get("/")
        node2_zk.create("/persistent_node1", b"123", ephemeral=False)

        time.sleep(1)

        node1_zk.exists("/persistent_node1")
        node2_zk.exists("/persistent_node1")

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])
