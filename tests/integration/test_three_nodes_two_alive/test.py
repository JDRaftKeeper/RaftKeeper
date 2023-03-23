#!/usr/bin/env python3
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_client

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/log_conf.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()
        yield cluster1
    finally:
        cluster1.shutdown()


def start(node):
    node.start_raftkeeper(start_wait=True)


def delete_with_retry(node, path):
    for _ in range(30):
        fake_zk = None
        try:
            fake_zk = node.get_fake_zk()
            fake_zk.delete(path)
            return
        except:
            close_zk_client(fake_zk)
            time.sleep(0.5)
    raise Exception(f"Cannot delete {path} from node {node.name}")


def test_start_offline(started_cluster):
    p = Pool(3)
    node1_zk = node2_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node1_zk.create("/test_alive", b"aaaa")

        node1.stop_raftkeeper()
        node2.stop_raftkeeper()
        node3.stop_raftkeeper()

        time.sleep(3)
        p.map(start, [node2, node3])

        node2_zk = node2.get_fake_zk()
        node2_zk.create("/c", b"data")

    finally:
        p.map(start, [node1, node2, node3])
        delete_with_retry(node1, "/test_alive")
        close_zk_client(node1_zk)
        close_zk_client(node2_zk)


def test_start_non_existing(started_cluster):
    p = Pool(3)
    node2_zk = None
    try:
        node1.stop_raftkeeper()
        node2.stop_raftkeeper()
        node3.stop_raftkeeper()

        node1.replace_in_config('/etc/raftkeeper-server/config.d/enable_keeper1.xml', 'node3', 'non_existing_node')
        node2.replace_in_config('/etc/raftkeeper-server/config.d/enable_keeper2.xml', 'node3', 'non_existing_node')

        time.sleep(3)
        p.map(start, [node2, node1])

        node2_zk = node2.get_fake_zk()
        node2_zk.create("/test_non_exising", b"data")
    finally:
        node1.replace_in_config('/etc/raftkeeper-server/config.d/enable_keeper1.xml', 'non_existing_node', 'node3')
        node2.replace_in_config('/etc/raftkeeper-server/config.d/enable_keeper2.xml', 'non_existing_node', 'node3')
        p.map(start, [node1, node2, node3])
        delete_with_retry(node2, "/test_non_exising")
        close_zk_client(node2_zk)


def test_restart_third_node(started_cluster):
    node1_zk = node1.get_fake_zk()
    node1_zk.create("/test_restart", b"aaaa")

    node3.restart_raftkeeper()

    node1_zk.delete("/test_restart")
    close_zk_client(node1_zk)
