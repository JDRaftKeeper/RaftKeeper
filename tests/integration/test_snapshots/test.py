import os
import random
import string
import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster = RaftKeeperCluster(__file__)

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


@pytest.mark.parametrize(
    'node',
    [
        cluster.add_instance('node1', main_configs=['configs/enable_keeper.xml'], with_zookeeper=True, stay_alive=True),
        cluster.add_instance('node2', main_configs=['configs/enable_async_snapshot_keeper.xml'], with_zookeeper=True, stay_alive=True)
    ]
)
def test_state_after_restart(started_cluster, node):
    node_zk = node_zk2 = None
    try:
        node_zk = node.get_fake_zk()

        node_zk.create("/test_state_after_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_state_after_restart/node" + str(i), strs[i])

        existing_children = []
        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_state_after_restart/node" + str(i))
            else:
                existing_children.append("node" + str(i))

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk2 = node.get_fake_zk()

        assert node_zk2.get("/test_state_after_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/test_state_after_restart/node" + str(i)) is None
            else:
                data, stat = node_zk2.get("/test_state_after_restart/node" + str(i))
                assert len(data) == 123
                assert data == strs[i]
                assert stat.ephemeralOwner == 0

        assert list(sorted(existing_children)) == list(sorted(node_zk2.get_children("/test_state_after_restart")))
    finally:
        close_zk_clients([node_zk, node_zk2])


@pytest.mark.parametrize(
    'node',
    [
        cluster.add_instance('node3', main_configs=['configs/enable_keeper.xml'], with_zookeeper=True, stay_alive=True),
        cluster.add_instance('node4', main_configs=['configs/enable_async_snapshot_keeper.xml'], with_zookeeper=True, stay_alive=True)
    ]
)
def test_ephemeral_after_restart(started_cluster, node):
    node_zk = node_zk2 = None
    try:
        node_zk = node.get_fake_zk()

        session_id = node_zk._session_id
        node_zk.create("/test_ephemeral_after_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_ephemeral_after_restart/node" + str(i), strs[i], ephemeral=True)

        existing_children = []
        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_ephemeral_after_restart/node" + str(i))
            else:
                existing_children.append("node" + str(i))

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk2 = node.get_fake_zk()

        assert node_zk2.get("/test_ephemeral_after_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/test_ephemeral_after_restart/node" + str(i)) is None
            else:
                data, stat = node_zk2.get("/test_ephemeral_after_restart/node" + str(i))
                assert len(data) == 123
                assert data == strs[i]
                assert stat.ephemeralOwner == session_id
        assert list(sorted(existing_children)) == list(sorted(node_zk2.get_children("/test_ephemeral_after_restart")))
    finally:
        close_zk_clients([node_zk, node_zk2])


@pytest.mark.parametrize(
    'node',
    [
        cluster.add_instance('node5', main_configs=['configs/enable_keeper.xml'], with_zookeeper=True, stay_alive=True),
        cluster.add_instance('node6', main_configs=['configs/enable_async_snapshot_keeper.xml'], with_zookeeper=True, stay_alive=True)
    ]
)
def test_restart_with_no_log(started_cluster, node):
    node_zk = node_zk2 = None
    try:
        node_zk = node.get_fake_zk()
        node_zk.create("/test_restart_with_no_log", b"somevalue")

        node.send_4lw_cmd(cmd="csnp")
        time.sleep(1) # wait for snapshot to be taken

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        node_zk2 = node.get_fake_zk()
        assert node_zk2.exists("/test_restart_with_no_log")
    finally:
        close_zk_clients([node_zk, node_zk2])
