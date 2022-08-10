import pytest
from helpers.cluster import ClickHouseCluster
from helpers.cluster_service import ClickHouseServiceCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager


from kazoo.client import KazooClient, KazooState

cluster1 = ClickHouseServiceCluster(__file__)
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
    _fake_zk_instance = KazooClient(hosts=cluster1.get_instance_ip(nodename) + ":5102", timeout=timeout)
    def reset_listener(state):
        nonlocal _fake_zk_instance
        print("Fake zk callback called for state", state)
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance

def test_restart(started_cluster):

    try:
        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")

        node1_zk.create("/test_restart_node", b"hello")

        for i in range(10000):
            node1_zk.create("/test_restart_node/" + str(i), b"hello")

        for i in range(10000):
            node1_zk.set("/test_restart_node/" + str(i), b"hello111")

        for i in range(100):
            node1_zk.delete("/test_restart_node/" + str(i))

        node1_zk.create("/test_restart_node1", b"hello")

        for i in range(10000):
            node1_zk.create("/test_restart_node1/" + str(i), b"hello")

        node1_zk.create("/test_restart_node2", b"hello")

        for i in range(10000):
            t = node1_zk.transaction()
            t.create("/test_restart_node2/q" + str(i))
            t.delete("/test_restart_node2/a" + str(i))
            t.create("/test_restart_node2/x" + str(i))
            t.commit()

        node1.stop_clickhouse()
        node2.stop_clickhouse()
        node3.stop_clickhouse()

        node1.start_clickhouse(start_wait=False)
        node2.start_clickhouse(start_wait=False)
        node3.start_clickhouse(start_wait=False)

        wait_nodes(cluster1, node1, node2, node3)

        node3_zk = get_fake_zk(cluster1, "node1")

        for i in range(9900):
            assert node3_zk.get("/test_restart_node/" + str(i + 100))[0] == b"hello111"

        for i in range(10000):
            assert node3_zk.get("/test_restart_node1/" + str(i))[0] == b"hello"

        children = node3_zk.get_children("/test_restart_node2")

        assert children == []

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
