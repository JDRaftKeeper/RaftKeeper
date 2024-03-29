import pytest
from kazoo.retry import KazooRetry

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_client

cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)

from kazoo.client import KazooClient


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_node(node):
    node.wait_for_join_cluster()


def wait_nodes():
    for n in [node1, node2, node3]:
        wait_node(n)


# get client with retry policy
def get_fake_zk(node_name, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(node_name) + ":8101", timeout=timeout)
    _fake_zk_instance.retry = KazooRetry(ignore_expire=False, max_delay=60, max_tries=1000)
    _fake_zk_instance.start()
    return _fake_zk_instance


def restart_cluster(zk, first_session_id):
    print("Restarting cluster, client previous session id is ", first_session_id)
    node1.restart_raftkeeper()
    node2.restart_raftkeeper()
    node3.restart_raftkeeper()

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
        zk.create("/test_reconnection_ephemeral", b"I_am_ephemeral_node", ephemeral=True)

        print('Register a data watch to /test_reconnection')

        def data_watch(event_data, event_stat):
            global watch_triggered
            watch_triggered = True
            print(f"Watch for /test_reconnection triggered, value is {event_data}, stat is {event_stat}")

        zk.get(path='/test_reconnection', watch=data_watch)

        restart_cluster(zk, first_session_id)

        zk.set("/test_reconnection", b"world")
        data, stat = zk.get("/test_reconnection")
        assert data == b"world"

        data, stat = zk.get("/test_reconnection_ephemeral")
        assert data == b"I_am_ephemeral_node"

        global watch_triggered
        assert not watch_triggered

        assert zk._session_id == first_session_id

    finally:
        close_zk_client(zk)

# def test_session_expired(started_cluster):
#     wait_nodes()
#     zk = None
#     try:
#         zk = get_fake_zk(node1.name, timeout=2)
#         first_session_id = zk._session_id
#         print("Client session id", first_session_id)
#         print("Client session timeout", zk._session_timeout)
#         zk.create("/test_session_expired", b"hello")
#         zk.create("/test_session_expired_ephemeral", b"I_am_ephemeral_node", ephemeral=True)
#
#         restart_cluster(zk, first_session_id)
#
#         try:
#             data, _ = zk.get("/test_session_expired")
#         except ZookeeperError as ex:
#             print("Session expired ", ex)
#             assert True
#         else:
#             print("data ", data)
#             assert False
#
#         zk = get_fake_zk(node1.name, timeout=2)
#         data, _ = zk.get("/test_session_expired")
#         assert data == b"hello"
#
#         try:
#             data, _ = zk.get("/test_session_expired_ephemeral")
#         except NoNodeError:
#             assert True
#         else:
#             assert False
#
#         assert zk._session_id > first_session_id
#     finally:
#         if zk is not None:
#             zk.stop()
#             zk.close()

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
#             pm.drop_port_connections(port=8101)
#
#             pm.drop_instance_zk_connections(instance=node1)
#             pm.drop_instance_zk_connections(instance=node2)
#             pm.drop_instance_zk_connections(instance=node3)
#
#             pm.drop_raft_connections(instance=node1, port=8101)
#             pm.drop_raft_connections(instance=node2, port=8101)
#             pm.drop_raft_connections(instance=node3, port=8101)
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
#             pm.restore_port_connections(port=8101)
#             pm.restore_raft_connections(instance=node1, port=8101)
#             pm.restore_raft_connections(instance=node2, port=8101)
#             pm.restore_raft_connections(instance=node3, port=8101)
#             time.sleep(1)
#
#             assert zk._session_id == first_session_id
#             assert zk.exists("/test_reconnection_with_partition") is not None
#     finally:
#         if zk is not None:
#             zk.stop()
#             zk.close()
