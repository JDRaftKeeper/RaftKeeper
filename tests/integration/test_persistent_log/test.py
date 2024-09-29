#!/usr/bin/env python3
import os
import random
import string

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster = RaftKeeperCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/enable_keeper.xml'], stay_alive=True)


def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def create_random_path(prefix="", depth=1):
    if depth == 0:
        return prefix
    return create_random_path(os.path.join(prefix, random_string(3)), depth - 1)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_open_and_closed_log_segment(started_cluster):
    node_zk = None
    try:
        node_zk = node.get_fake_zk(session_timeout=120)

        node_zk.create("/test_open_and_closed_log_segment", b"")
        # Will generate some closed log segment files and an opened one
        for i in range(10):
            node_zk.create("/test_open_and_closed_log_segment/node" + str(i), b"")

        close_zk_clients([node_zk])

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        # test node can successfully start
        node_zk = node.get_fake_zk(session_timeout=120)
    finally:
        close_zk_clients([node_zk])


def test_state_after_restart(started_cluster):
    node_zk = node_zk2 = None
    try:
        # use a long session timeout
        node_zk = node.get_fake_zk(session_timeout=120)

        node_zk.create("/test_state_after_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_state_after_restart/node" + str(i), strs[i])

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_state_after_restart/node" + str(i))

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk2 = node.get_fake_zk()

        assert node_zk2.get("/test_state_after_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/test_state_after_restart/node" + str(i)) is None
            else:
                assert len(node_zk2.get("/test_state_after_restart/node" + str(i))[0]) == 123
                assert node_zk2.get("/test_state_after_restart/node" + str(i))[0] == strs[i]
    finally:
        close_zk_clients([node_zk, node_zk2])


def test_state_duplicate_restart(started_cluster):
    node_zk = node_zk2 = node_zk3 = None
    try:
        node_zk = node.get_fake_zk(session_timeout=120)

        node_zk.create("/test_state_duplicated_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_state_duplicated_restart/node" + str(i), strs[i])

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_state_duplicated_restart/node" + str(i))

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk2 = node.get_fake_zk()

        node_zk2.create("/test_state_duplicated_restart/just_test1")
        node_zk2.create("/test_state_duplicated_restart/just_test2")
        node_zk2.create("/test_state_duplicated_restart/just_test3")

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk3 = node.get_fake_zk()

        assert node_zk3.get("/test_state_duplicated_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk3.exists("/test_state_duplicated_restart/node" + str(i)) is None
            else:
                assert len(node_zk3.get("/test_state_duplicated_restart/node" + str(i))[0]) == 123
                assert node_zk3.get("/test_state_duplicated_restart/node" + str(i))[0] == strs[i]
    finally:
        close_zk_clients([node_zk, node_zk2, node_zk3])


# http://zookeeper-user.578899.n2.nabble.com/Why-are-ephemeral-nodes-written-to-disk-tp7583403p7583418.html
def test_ephemeral_after_restart(started_cluster):
    node_zk = node_zk2 = None
    try:
        node_zk = node.get_fake_zk(session_timeout=120)

        node_zk.create("/test_ephemeral_after_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_ephemeral_after_restart/node" + str(i), strs[i], ephemeral=True)

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_ephemeral_after_restart/node" + str(i))

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk2 = node.get_fake_zk()

        assert node_zk2.get("/test_ephemeral_after_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/test_ephemeral_after_restart/node" + str(i)) is None
            else:
                assert len(node_zk2.get("/test_ephemeral_after_restart/node" + str(i))[0]) == 123
                assert node_zk2.get("/test_ephemeral_after_restart/node" + str(i))[0] == strs[i]
    finally:
        close_zk_clients([node_zk, node_zk2])

