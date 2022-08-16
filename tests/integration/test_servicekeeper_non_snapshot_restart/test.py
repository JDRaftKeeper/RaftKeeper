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
            zk.get("/")
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

def compare_stats(stat1, stat2, path):
    assert stat1.czxid == stat2.czxid, "path " + path + " cxzids not equal for stats: " + str(stat1.czxid) + " != " + str(stat2.czxid)
    assert stat1.mzxid == stat2.mzxid, "path " + path + " mxzids not equal for stats: " + str(stat1.mzxid) + " != " + str(stat2.mzxid)
    assert stat1.version == stat2.version, "path " + path + " versions not equal for stats: " + str(stat1.version) + " != " + str(stat2.version)
    assert stat1.cversion == stat2.cversion, "path " + path + " cversions not equal for stats: " + str(stat1.cversion) + " != " + str(stat2.cversion)
    # assert stat1.aversion == stat2.aversion, "path " + path + " aversions not equal for stats: " + str(stat1.aversion) + " != " + str(stat2.aversion)  ACL
    assert stat1.ephemeralOwner == stat2.ephemeralOwner,"path " + path + " ephemeralOwners not equal for stats: " + str(stat1.ephemeralOwner) + " != " + str(stat2.ephemeralOwner)
    assert stat1.dataLength == stat2.dataLength , "path " + path + " ephemeralOwners not equal for stats: " + str(stat1.dataLength) + " != " + str(stat2.dataLength)
    assert stat1.numChildren == stat2.numChildren, "path " + path + " numChildren not equal for stats: " + str(stat1.numChildren) + " != " + str(stat2.numChildren)
    # assert stat1.pzxid == stat2.pzxid, "path " + path + " pzxid not equal for stats: " + str(stat1.pzxid) + " != " + str(stat2.pzxid) from fuzzy snapshot

def dump_states(zk1, d, path="/"):
    data1, stat1 = zk1.get(path)

    d[path] = (data1, stat1)

    first_children = list(sorted(zk1.get_children(path)))

    for children in first_children:
        dump_states(zk1, d, os.path.join(path, children))


def test_restart(started_cluster):

    try:
        wait_nodes(cluster1, node1, node2, node3)

        node1_zk = get_fake_zk(cluster1, "node1")

        node1_zk.create("/test_restart_node", b"hello")

        for i in range(10000):
            node1_zk.create("/test_restart_node/" + str(i), b"hello")

        get_fake_zk(cluster1, "node1")

        for i in range(10000):
            node1_zk.set("/test_restart_node/" + str(i), b"hello111")

        get_fake_zk(cluster1, "node1")

        for i in range(100):
            node1_zk.delete("/test_restart_node/" + str(i))

        get_fake_zk(cluster1, "node1")

        node1_zk.create("/test_restart_node1", b"hello")

        for i in range(10000):
            node1_zk.create("/test_restart_node1/" + str(i), b"hello")

        get_fake_zk(cluster1, "node1")

        node1_zk.create("/test_restart_node2", b"hello")

        for i in range(10000):
            t = node1_zk.transaction()
            t.create("/test_restart_node2/q" + str(i))
            t.delete("/test_restart_node2/a" + str(i))
            t.create("/test_restart_node2/x" + str(i))
            t.commit()

        d = {}
        dump_states(node1_zk, d)

        node1.stop_clickhouse()
        node2.stop_clickhouse()
        node3.stop_clickhouse()

        node1.start_clickhouse(start_wait=False)
        node2.start_clickhouse(start_wait=False)
        node3.start_clickhouse(start_wait=False)

        wait_nodes(cluster1, node1, node2, node3)

        node3_zk = get_fake_zk(cluster1, "node3")

        for i in range(9900):
            assert node3_zk.get("/test_restart_node/" + str(i + 100))[0] == b"hello111"

        for i in range(10000):
            assert node3_zk.get("/test_restart_node1/" + str(i))[0] == b"hello"

        children = node3_zk.get_children("/test_restart_node2")

        assert children == []

        dd = {}
        dump_states(node3_zk, dd)

        node2_zk = get_fake_zk(cluster1, "node2")

        ddd = {}
        dump_states(node2_zk, ddd)

        assert len(d) == len(dd)
        assert len(d) == len(ddd)
        for k,v in d.items():
            if k not in ("/"): # / not same ?
                assert v[0] == dd[k][0]
                assert v[0] == ddd[k][0]
                compare_stats(v[1], dd[k][1], k)
                compare_stats(v[1], ddd[k][1], k)

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
