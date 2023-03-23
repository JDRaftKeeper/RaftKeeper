import socket
import pytest
from helpers.cluster_service import RaftKeeperCluster
import time

cluster = RaftKeeperCluster(__file__, name="test_keeper_4lw_white_list")
node1 = cluster.add_instance('node1', main_configs=['configs/keeper_config_with_white_list.xml', 'configs/logs_conf.xml'], stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/keeper_config_without_white_list.xml', 'configs/logs_conf.xml'], stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/keeper_config_with_white_list_all.xml', 'configs/logs_conf.xml'], stay_alive=True)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_node(node):
    node.wait_for_join_cluster()


def wait_nodes():
    for n in [node1, node2, node3]:
        wait_node(n)


def get_keeper_socket(nodename):
    hosts = cluster.get_instance_ip(nodename)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 8101))
    return client


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":8101", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance


def close_keeper_socket(cli):
    if cli is not None:
        print("close socket")
        cli.close()


def send_cmd(node_name, command = "ruok"):
    client = None
    try:
        wait_nodes()
        client = get_keeper_socket(node_name)
        client.send(command.encode())
        data = client.recv(4)
        return data.decode()
    finally:
        close_keeper_socket(client)


def test_white_list(started_cluster):
    client = None
    try:
        wait_nodes()
        assert send_cmd(node1.name) == 'imok'
        assert send_cmd(node1.name, command = 'mntr') == ''
        assert send_cmd(node2.name) == 'imok'
        assert send_cmd(node3.name) == 'imok'
    finally:
        close_keeper_socket(client)
