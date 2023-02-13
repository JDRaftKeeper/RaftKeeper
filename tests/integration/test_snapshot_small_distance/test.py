#!/usr/bin/env python3
##!/usr/bin/env python3
import pytest
from helpers.cluster_service import RaftKeeperCluster
from multiprocessing.dummy import Pool
from kazoo.client import KazooClient, KazooState
import random
import string
import os
import time

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/log_conf.xml'], stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/log_conf.xml'], stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/log_conf.xml'], stay_alive=True)

def start_zookeeper(node):
    node1.exec_in_container(['bash', '-c', '/opt/zookeeper/bin/zkServer.sh start'])

def stop_zookeeper(node):
    node.exec_in_container(['bash', '-c', '/opt/zookeeper/bin/zkServer.sh stop'])

def clear_zookeeper(node):
    node.exec_in_container(['bash', '-c', 'rm -fr /zookeeper/*'])

def restart_and_clear_zookeeper(node):
    stop_zookeeper(node)
    clear_zookeeper(node)
    start_zookeeper(node)

def clear_raftkeeper_data(node):
    node.exec_in_container(['bash', '-c', 'rm -fr /var/lib/raftkeeper/data/raft_log/* /var/lib/raftkeeper/data/raft_snapshot/*'])

def convert_zookeeper_data(node):
    cmd = '/usr/bin/raftkeeper converter --zookeeper-logs-dir /zookeeper/version-2/ --zookeeper-snapshots-dir  /zookeeper/version-2/ --output-dir /var/lib/raftkeeper/data/raft_snapshot'
    node.exec_in_container(['bash', '-c', cmd])
    return os.path.join('/var/lib/raftkeeper/data/raft_snapshot', node.exec_in_container(['bash', '-c', 'ls /var/lib/raftkeeper/data/raft_snapshot']).strip())

def stop_raftkeeper(node):
    node.stop_raftkeeper()

def start_raftkeeper(node):
    node.start_raftkeeper(60)

def copy_zookeeper_data(make_zk_snapshots, node):
    stop_zookeeper(node)

    if make_zk_snapshots: # force zookeeper to create snapshot
        start_zookeeper(node)
        stop_zookeeper(node)

    stop_raftkeeper(node)
    clear_raftkeeper_data(node)
    convert_zookeeper_data(node)
    start_zookeeper(node)
    start_raftkeeper(node)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()

        yield cluster1

    finally:
        cluster1.shutdown()

def get_fake_zk(node, timeout=60.0):
    _fake_zk_instance = KazooClient(hosts=cluster1.get_instance_ip(node.name) + ":8101", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance

# def get_fake_zk(node, timeout=60.0):
#     print(node.name, cluster1.get_instance_ip("node"))
#     _fake_zk_instance =  KazooClient(hosts=cluster1.get_instance_ip(node.name) + ":8101", timeout=60.0)
#     def reset_last_zxid_listener(state):
#         print("Fake zk callback called for state", state)
#         nonlocal _fake_zk_instance
#         if state != KazooState.CONNECTED:
#             _fake_zk_instance._reset()
#
#     _fake_zk_instance.add_listener(reset_last_zxid_listener)
#     _fake_zk_instance.start()
#     return _fake_zk_instance

def get_genuine_zk(node, timeout=30.0):
    _genuine_zk_instance = KazooClient(hosts=cluster1.get_instance_ip(node.name) + ":2181", timeout=timeout)
    _genuine_zk_instance.start()
    return _genuine_zk_instance


def test_snapshot_and_load(started_cluster):
    restart_and_clear_zookeeper(node1)
    genuine_connection = get_genuine_zk(node1)
    for node in [node1, node2, node3]:
        print("Stopping and clearing", node.name, "with dockerid", node.docker_id)
        stop_raftkeeper(node)
        clear_raftkeeper_data(node)

    for i in range(1000):
        genuine_connection.create("/test" + str(i), b"data")

    print("Data loaded to zookeeper")

    stop_zookeeper(node1)
    start_zookeeper(node1)
    stop_zookeeper(node1)

    print("Data copied to node1")
    resulted_path = convert_zookeeper_data(node1)
    print("Resulted path", resulted_path)
    for node in [node2, node3]:
        print("Copy snapshot from", node1.name, "to", node.name)
        cluster1.copy_file_from_container_to_container(node1, '/var/lib/raftkeeper/data/raft_snapshot', node, '/var/lib/raftkeeper/data')

    print("Starting raftkeepers")

    p = Pool(3)
    result = p.map_async(start_raftkeeper, [node1, node2, node3])
    result.wait()

    print("Loading additional data")
    fake_zks = [get_fake_zk(node) for node in [node1, node2, node3]]
    for i in range(1000):
        fake_zk = random.choice(fake_zks)
        try:
            fake_zk.create("/test" + str(i + 1000), b"data")
        except Exception as ex:
            print("Got exception:" + str(ex))

    print("Final")
    fake_zks[0].create("/test10000", b"data")
