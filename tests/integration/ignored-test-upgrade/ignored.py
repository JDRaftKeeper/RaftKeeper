import pytest
from helpers.cluster_service import RaftKeeperCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager


from kazoo.client import KazooClient, KazooState

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node', main_configs=['configs/enable_test_keeper_old.xml', 'configs/log_conf.xml'], stay_alive=True, use_old_bin=True)


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


def get_fake_zk(cluster, nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":8101", timeout=timeout)
    def reset_listener(state):
        nonlocal _fake_zk_instance
        print("Fake zk callback called for state", state)
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance


def compare_stats(stat1, stat2, path):
    # new version getSessionId add zxid
    # assert stat1.czxid == stat2.czxid, "path " + path + " cxzids not equal for stats: " + str(stat1.czxid) + " != " + str(stat2.czxid)
    # assert stat1.mzxid == stat2.mzxid, "path " + path + " mxzids not equal for stats: " + str(stat1.mzxid) + " != " + str(stat2.mzxid)
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

def test_data(started_cluster):

    try:
        # cluster1.start()
        wait_node(node1)
        node1_zk = get_fake_zk(cluster1, "node")

        node1_zk.create("/test_restart_node", b"hello")

        for i in range(10000):
            node1_zk.create("/test_restart_node/" + str(i), b"hello")

        get_fake_zk(cluster1, "node")

        for i in range(10000):
            node1_zk.set("/test_restart_node/" + str(i), b"hello111")

        get_fake_zk(cluster1, "node")

        for i in range(100):
            node1_zk.delete("/test_restart_node/" + str(i))

        get_fake_zk(cluster1, "node")

        node1_zk.create("/test_restart_node1", b"hello")

        for i in range(10000):
            node1_zk.create("/test_restart_node1/" + str(i), b"hello")

        get_fake_zk(cluster1, "node")

        node1_zk.create("/test_restart_node2", b"hello")

        for i in range(10000):
            t = node1_zk.transaction()
            t.create("/test_restart_node2/q" + str(i))
            t.delete("/test_restart_node2/a" + str(i))
            t.create("/test_restart_node2/x" + str(i))
            t.commit()

        d = {}
        dump_states(node1_zk, d)

        output = node1.exec_in_container(["bash", "-c", "ps ax | grep raftkeeper"], user='root')
        print("ps ax | grep raftkeeper output: ", output)

        output1 = node1.exec_in_container(["bash", "-c", "md5sum /usr/bin/raftkeeper_old"], user='root')
        print("ps ax | grep raftkeeper output: ", output1)

        node1.stop_raftkeeper()
        node1.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/enable_test_keeper.xml"), '/etc/raftkeeper-server/config.xml')

        node1.use_old_bin=False

        node1.start_raftkeeper()
        wait_node(node1)

        output2 = node1.exec_in_container(["bash", "-c", "ps ax | grep raftkeeper"], user='root')
        print("ps ax | grep raftkeeper output: ", output2)

        output3 = node1.exec_in_container(["bash", "-c", "md5sum /usr/bin/raftkeeper"], user='root')
        print("ps ax | grep raftkeeper output: ", output3)

        node2_zk = get_fake_zk(cluster1, "node")

        dd = {}
        dump_states(node2_zk, dd)

        assert len(d) == len(dd)
        for k,v in d.items():
            if k not in ("/"): # / not same ?
                assert v[0] == dd[k][0]
                compare_stats(v[1], dd[k][1], k)

    finally:
        cluster1.shutdown()
