import socket
import pytest
import subprocess
from helpers.cluster_service import ClickHouseServiceCluster
from helpers.network import PartitionManager
import time

from kazoo import client
from kazoo.retry import KazooRetry

cluster = ClickHouseServiceCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import ZookeeperError


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
    for _ in range(20):
        zk = None
        try:
            zk = get_fake_zk(node.name, timeout=3.0)
            # zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(1)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            destroy_zk_client(zk)
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def wait_nodes():
    for n in [node1, node2, node3]:
        wait_node(n)


def get_fake_zk(node_name, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(node_name) + ":5102", timeout=timeout)
    _fake_zk_instance.retry = KazooRetry(ignore_expire=False, max_delay=1.0, max_tries=1)
    _fake_zk_instance.start()
    return _fake_zk_instance


def restart_cluster(zk, first_session_id):
    print("Restarting cluster, client previous session id is ", first_session_id)
    node1.kill_clickhouse(stop_start_wait_sec=0.1)
    node2.kill_clickhouse(stop_start_wait_sec=0.1)
    node3.kill_clickhouse(stop_start_wait_sec=0.1)
    time.sleep(3)
    node1.restore_clickhouse()
    node2.restore_clickhouse()
    node3.restore_clickhouse()

    wait_nodes()
    print("Cluster started client session id is ", zk._session_id)


watch_triggered = False


def test_reconnection(started_cluster):
    wait_nodes()
    zk = None
    try:
        zk = get_fake_zk(node1.name, timeout=100)
        first_session_id = zk._session_id
        print("Client session id", first_session_id)
        print("Client session timeout", zk._session_timeout)

        zk.create("/test_reconnection", b"hello")
        zk.create("/test_reconnection_ephemeral", b"I_am_ephemeral_node")

        # @client.DataWatch(client=zk, path='/test_reconnection')
        def data_watch(event_data, _):
            global watch_triggered
            watch_triggered = True
            print("Watch for /test_reconnection triggered, value is %s" % event_data)

        print('Register a data watch to /test_reconnection')
        zk.get(path='/test_reconnection', watch=data_watch)

        restart_cluster(zk, first_session_id)

        data, stat = zk.get("/test_reconnection")
        assert data == b"hello"

        data, stat = zk.get("/test_reconnection_ephemeral")
        assert data == b"I_am_ephemeral_node"

        zk.set("/test_reconnection", b"world")
        assert watch_triggered

        assert zk._session_id == first_session_id

    finally:
        if zk is not None:
            zk.stop()
            zk.close()


def test_session_expired(started_cluster):
    wait_nodes()
    zk = None
    try:
        zk = get_fake_zk(node1.name, timeout=2)
        first_session_id = zk._session_id
        print("Client session id", first_session_id)
        print("Client session timeout", zk._session_timeout)
        zk.create("/test_session_expired", b"hello")

        restart_cluster(zk, first_session_id)

        try:
            data, _ = zk.get("/test_session_expired")
        except ZookeeperError as ex:
            print("Session expired ", ex)
            assert True
        else:
            print("data ", data)
            assert False

        zk = get_fake_zk(node1.name, timeout=2)
        data, stat = zk.get("/test_session_expired")
        assert data == b"hello"
        assert zk._session_id > first_session_id
    finally:
        if zk is not None:
            zk.stop()
            zk.close()

# network partition does not work.
# def test_reconnection_with_partition(started_cluster):
#     wait_nodes()
#     zk = None
#     try:
#         zk = get_fake_zk(node1.name, timeout=10)
#         first_session_id = zk._session_id
#         print("Client session id", first_session_id)
#         zk.create("/test_reconnection_with_partition", b"hello")
#
#         with PartitionManager() as pm:
#             # ip = local_ip()
#             # pm.partition_instances_by_ip(node1.ip_address, ip)
#             # pm.partition_instances_by_ip(node2.ip_address, ip)
#             # pm.partition_instances_by_ip(node3.ip_address, ip)
#             print("Partition with zks")
#             pm.drop_port_connections(port=5102)
#
#             pm.drop_instance_zk_connections(instance=node1)
#             pm.drop_instance_zk_connections(instance=node2)
#             pm.drop_instance_zk_connections(instance=node3)
#
#             pm.drop_raft_connections(instance=node1, port=5102)
#             pm.drop_raft_connections(instance=node2, port=5102)
#             pm.drop_raft_connections(instance=node3, port=5102)
#
#             res = subprocess.run('iptables -L', env=None, shell=True, capture_output=True)
#             print(res.stdout.decode(), res.stderr.decode())
#
#             # session timeout is 10, operation timeout is 1s
#             time.sleep(1)
#             try:
#                 data = zk.get("/test_reconnection_with_partition")
#                 print("data", data)
#             except Exception as ex:
#                 print("Lose zk connection", ex)
#             else:
#                 print("Network partition not work!")
#                 assert False
#             pm.restore_port_connections(port=5102)
#             pm.restore_raft_connections(instance=node1, port=5102)
#             pm.restore_raft_connections(instance=node2, port=5102)
#             pm.restore_raft_connections(instance=node3, port=5102)
#             time.sleep(1)
#
#             assert zk._session_id == first_session_id
#             assert zk.exists("/test_reconnection_with_partition") is not None
#     finally:
#         if zk is not None:
#             zk.stop()
#             zk.close()
