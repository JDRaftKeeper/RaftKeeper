import pytest
from helpers.cluster_service import RaftKeeperCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager


from kazoo.client import KazooClient, KazooState

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml', 'configs/log_conf.xml'], stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml', 'configs/log_conf.xml'], stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml', 'configs/log_conf.xml'], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()

        yield cluster1

    finally:
        cluster1.shutdown()

def smaller_exception(ex):
    return '\n'.join(str(ex).split('\n')[0:2])

def wait_node(cluster1, node):
    for _ in range(100):
        zk = None
        try:
            # node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(cluster1, node.name, timeout=30.0)
            zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            if zk:
                zk.stop()
                zk.close()
    else:
        raise Exception("Can't wait node", node.name, "to become ready")

def wait_nodes(cluster1, node1, node2, node3):
    for node in [node1, node2, node3]:
        wait_node(cluster1, node)


def get_fake_zk(cluster1, nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster1.get_instance_ip(nodename) + ":8101", timeout=timeout)
    def reset_listener(state):
        nonlocal _fake_zk_instance
        print("Fake zk callback called for state", state)
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance

def test_read_write_multinode(started_cluster):
    try:

        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")
        node2_zk = get_fake_zk(cluster1, "node2")
        node3_zk = get_fake_zk(cluster1, "node3")

        node1_zk.create("/test_read_write_multinode_node1", b"somedata1")
        node2_zk.create("/test_read_write_multinode_node2", b"somedata2")
        node3_zk.create("/test_read_write_multinode_node3", b"somedata3")

        # stale reads are allowed
        while node1_zk.exists("/test_read_write_multinode_node2") is None:
            time.sleep(0.1)

        while node1_zk.exists("/test_read_write_multinode_node3") is None:
            time.sleep(0.1)

        while node2_zk.exists("/test_read_write_multinode_node3") is None:
            time.sleep(0.1)

        assert node3_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node2_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node1_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"

        assert node3_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node2_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node1_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"

        assert node3_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"
        assert node2_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"
        assert node1_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_watch_on_follower(started_cluster):

    try:

        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")
        node2_zk = get_fake_zk(cluster1, "node2")
        node3_zk = get_fake_zk(cluster1, "node3")

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
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_session_expiration(started_cluster):

    try:

        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")
        node2_zk = get_fake_zk(cluster1, "node2")
        node3_zk = get_fake_zk(cluster1, "node3", timeout=3.0)
        print("Node3 session id", node3_zk._session_id)

        node3_zk.create("/test_ephemeral_node", b"world", ephemeral=True)

        with PartitionManager() as pm:
            pm.partition_instances(node3, node2)
            pm.partition_instances(node3, node1)
            node3_zk.stop()
            node3_zk.close()
            time.sleep(3)
            for _ in range(100):
                if node1_zk.exists("/test_ephemeral_node") is None and node2_zk.exists("/test_ephemeral_node") is None:
                    break
                print("Node1 exists", node1_zk.exists("/test_ephemeral_node"))
                print("Node2 exists", node2_zk.exists("/test_ephemeral_node"))
                time.sleep(0.1)
                node1_zk.sync("/")
                node2_zk.sync("/")

        assert node1_zk.exists("/test_ephemeral_node") is None
        assert node2_zk.exists("/test_ephemeral_node") is None

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass


def test_follower_restart(started_cluster):

    try:
        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")

        node1_zk.create("/test_restart_node", b"hello")

        node3.restart_raftkeeper()
        wait_node(cluster1, node3)

        node3_zk = get_fake_zk(cluster1, "node3")

        # got data from log
        assert node3_zk.get("/test_restart_node")[0] == b"hello"

    finally:
        try:
            for zk_conn in [node1_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass

def test_simple_sleep_test(started_cluster):
    try:

        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")
        node2_zk = get_fake_zk(cluster1, "node2")
        node3_zk = get_fake_zk(cluster1, "node3", timeout=3.0)
        print("Node3 session id", node3_zk._session_id)

        node2_zk.get("/")
        node2_zk.create("/persistent_node", b"", ephemeral=False)
        node2_zk.get("/persistent_node")
        node2_zk.get("/")
        node2_zk.create("/persistent_node1", b"123", ephemeral=False)

        time.sleep(150)

        node1_zk.exists("/persistent_node1")
        node2_zk.exists("/persistent_node1")

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass
