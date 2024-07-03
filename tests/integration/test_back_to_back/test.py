import socket
import pytest
import random
import string
import os
import time
import threading
from multiprocessing.dummy import Pool
from kazoo.client import KazooClient, KazooState
from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients, close_zk_client

from helpers.utils import KeeperFeatureClient
from kazoo.protocol.states import WatchedEvent

cluster = RaftKeeperCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper_three_nodes_1.xml'],
                             with_zookeeper=True, stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper_three_nodes_2.xml'],
                             with_zookeeper=True, stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper_three_nodes_3.xml'],
                             with_zookeeper=True, stay_alive=True)


def get_genuine_zk(use_keeper_feature_client=False):
    print("Zoo1", cluster.get_instance_ip("zoo1"))
    if use_keeper_feature_client:
        return cluster.get_keeper_feature_client('zoo1')
    else:
        return cluster.get_kazoo_client('zoo1')


def get_fake_zk(use_keeper_feature_client=False):
    print("node1", cluster.get_instance_ip("node1"))
    if use_keeper_feature_client:
        _fake_zk_instance = KeeperFeatureClient(hosts=cluster.get_instance_ip("node1") + ":8101", timeout=60.0)
    else:
        _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip("node1") + ":8101", timeout=60.0)

    def reset_last_zxid_listener(state):
        print("Fake zk callback called for state", state)
        nonlocal _fake_zk_instance
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_last_zxid_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance


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


def test_simple_commands(started_cluster):
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()

        for zk in [genuine_zk, fake_zk]:
            zk.create("/test_simple_commands", b"")
            zk.create("/test_simple_commands/somenode1", b"hello")
            zk.set("/test_simple_commands/somenode1", b"world")

        for zk in [genuine_zk, fake_zk]:
            assert zk.exists("/test_simple_commands")
            assert zk.exists("/test_simple_commands/somenode1")
            print(zk.get("/test_simple_commands/somenode1"))
            assert zk.get("/test_simple_commands/somenode1")[0] == b"world"
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def test_sequential_nodes(started_cluster):
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        genuine_zk.create("/test_sequential_nodes")
        fake_zk.create("/test_sequential_nodes")
        for i in range(1, 11):
            genuine_zk.create("/test_sequential_nodes/" + ("a" * i) + "-", sequence=True)
            genuine_zk.create("/test_sequential_nodes/" + ("b" * i))
            fake_zk.create("/test_sequential_nodes/" + ("a" * i) + "-", sequence=True)
            fake_zk.create("/test_sequential_nodes/" + ("b" * i))

        genuine_childs = list(sorted(genuine_zk.get_children("/test_sequential_nodes")))
        fake_childs = list(sorted(fake_zk.get_children("/test_sequential_nodes")))
        assert genuine_childs == fake_childs

        genuine_zk.create("/test_sequential_nodes_1")
        fake_zk.create("/test_sequential_nodes_1")

        genuine_zk.create("/test_sequential_nodes_1/a", sequence=True)
        fake_zk.create("/test_sequential_nodes_1/a", sequence=True)

        genuine_zk.create("/test_sequential_nodes_1/a0000000002")
        fake_zk.create("/test_sequential_nodes_1/a0000000002")

        genuine_throw = False
        fake_throw = False
        try:
            genuine_zk.create("/test_sequential_nodes_1/a", sequence=True)
        except Exception as ex:
            genuine_throw = True

        try:
            fake_zk.create("/test_sequential_nodes_1/a", sequence=True)
        except Exception as ex:
            fake_throw = True

        assert genuine_throw == fake_throw

        genuine_childs_1 = list(sorted(genuine_zk.get_children("/test_sequential_nodes_1")))
        fake_childs_1 = list(sorted(fake_zk.get_children("/test_sequential_nodes_1")))
        assert genuine_childs_1 == fake_childs_1

        genuine_zk.create("/test_sequential_nodes_2")
        fake_zk.create("/test_sequential_nodes_2")

        genuine_zk.create("/test_sequential_nodes_2/node")
        fake_zk.create("/test_sequential_nodes_2/node")
        genuine_zk.create("/test_sequential_nodes_2/node", sequence=True)
        fake_zk.create("/test_sequential_nodes_2/node", sequence=True)

        genuine_childs_2 = list(sorted(genuine_zk.get_children("/test_sequential_nodes_2")))
        fake_childs_2 = list(sorted(fake_zk.get_children("/test_sequential_nodes_2")))
        assert genuine_childs_2 == fake_childs_2
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def assert_eq_stats(stat1, stat2):
    assert stat1.version == stat2.version
    assert stat1.cversion == stat2.cversion
    assert stat1.aversion == stat2.aversion
    assert stat1.aversion == stat2.aversion
    assert stat1.dataLength == stat2.dataLength
    assert stat1.numChildren == stat2.numChildren


def test_stats(started_cluster):
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        genuine_zk.create("/test_stats_nodes")
        fake_zk.create("/test_stats_nodes")
        genuine_stats = genuine_zk.exists("/test_stats_nodes")
        fake_stats = fake_zk.exists("/test_stats_nodes")
        assert_eq_stats(genuine_stats, fake_stats)
        for i in range(1, 11):
            genuine_zk.create("/test_stats_nodes/" + ("a" * i) + "-", sequence=True)
            genuine_zk.create("/test_stats_nodes/" + ("b" * i))
            fake_zk.create("/test_stats_nodes/" + ("a" * i) + "-", sequence=True)
            fake_zk.create("/test_stats_nodes/" + ("b" * i))

        genuine_stats = genuine_zk.exists("/test_stats_nodes")
        fake_stats = fake_zk.exists("/test_stats_nodes")
        assert_eq_stats(genuine_stats, fake_stats)
        for i in range(1, 11):
            print("/test_stats_nodes/" + ("a" * i) + "-" + "{:010d}".format((i - 1) * 2))
            genuine_zk.delete("/test_stats_nodes/" + ("a" * i) + "-" + "{:010d}".format((i - 1) * 2))
            genuine_zk.delete("/test_stats_nodes/" + ("b" * i))
            fake_zk.delete("/test_stats_nodes/" + ("a" * i) + "-" + "{:010d}".format((i - 1) * 2))
            fake_zk.delete("/test_stats_nodes/" + ("b" * i))

        genuine_stats = genuine_zk.exists("/test_stats_nodes")
        fake_stats = fake_zk.exists("/test_stats_nodes")
        print(genuine_stats)
        print(fake_stats)
        assert_eq_stats(genuine_stats, fake_stats)
        for i in range(100):
            genuine_zk.set("/test_stats_nodes", ("q" * i).encode())
            fake_zk.set("/test_stats_nodes", ("q" * i).encode())

        genuine_stats = genuine_zk.exists("/test_stats_nodes")
        fake_stats = fake_zk.exists("/test_stats_nodes")
        print(genuine_stats)
        print(fake_stats)
        assert_eq_stats(genuine_stats, fake_stats)
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def test_watchers(started_cluster):
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()


        # test data watch
        datas = [None, None]
        for index, zk in enumerate([genuine_zk, fake_zk]):
            zk.create("/test_data_watches")

            def watch_data_callback(data):
                print("data watch called")
                datas[index] = data

            zk.get("/test_data_watches", watch=watch_data_callback)
            print("Calling set data")
            zk.set("/test_data_watches", b"a")
            time.sleep(3)

        print("Genuine data", datas[0])
        print("Fake data", datas[1])
        assert datas[0] == datas[1]


        # test child watch
        child_lists = [None, None]
        for index, zk in enumerate([genuine_zk, fake_zk]):

            def watch_children_callback(children):
                print("children watch called")
                child_lists[index] = children

            zk.get_children("/test_data_watches", watch=watch_children_callback)

            print("Calling create child")
            zk.create("/test_data_watches/child", b"b")
            time.sleep(3)

        print("Genuine child", child_lists[0])
        print("Fake child", child_lists[1])
        assert child_lists[0] == child_lists[1]


        # test watcher in multi writes ops
        datas = [None, None]
        child_lists = [None, None]
        for index, zk in enumerate([genuine_zk, fake_zk]):
            zk.create("/test_multi_request_watches")

            def watch_data_callback(data):
                print("data watch called")
                datas[index] = data

            def watch_children_callback(children):
                print("children watch called")
                child_lists[index] = children

            zk.get("/test_multi_request_watches", watch=watch_data_callback)
            zk.get_children("/test_multi_request_watches", watch=watch_children_callback)

            t = zk.transaction()
            t.set_data("/test_multi_request_watches", b"a")
            t.create("/test_multi_request_watches/node-", sequence=True)
            t.commit()
            time.sleep(3)

        print("Genuine child", datas[0])
        print("Fake child", datas[1])
        assert datas[0] == datas[1]

        print("Genuine child", child_lists[0])
        print("Fake child", child_lists[1])
        assert child_lists[0] == child_lists[1]
        
        
        # test watcher in multi writes ops failed
        datas = [None, None]
        child_lists = [None, None]
        for index, zk in enumerate([genuine_zk, fake_zk]):
            zk.create("/test_multi_request_failed_watches")
            
            zk.create("/test_multi_request_failed_watches/exists_node", b"a")

            def watch_data_callback(data):
                print("data watch called")
                datas[index] = data

            def watch_children_callback(children):
                print("children watch called")
                child_lists[index] = children

            zk.get("/test_multi_request_failed_watches", watch=watch_data_callback)
            zk.get_children("/test_multi_request_failed_watches", watch=watch_children_callback)

            t = zk.transaction()
            t.set_data("/test_multi_request_failed_watches", b"a")
            t.create("/test_multi_request_failed_watches/exists_node", b"a")
            t.create("/test_multi_request_failed_watches/node-", sequence=True)
            t.commit()
            time.sleep(3)

        print("Genuine child", datas[0])
        print("Fake child", datas[1])
        assert datas[0] == datas[1]
        
        print("Genuine child", child_lists[0])
        print("Fake child", child_lists[1])
        assert child_lists[0] == child_lists[1]

    finally:
        close_zk_clients([genuine_zk, fake_zk])


def test_multi_transactions(started_cluster):
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        for zk in [genuine_zk, fake_zk]:
            zk.create('/test_multitransactions')
            t = zk.transaction()
            t.create('/test_multitransactions/freddy')
            t.create('/test_multitransactions/fred', ephemeral=True)
            t.create('/test_multitransactions/smith', sequence=True)
            results = t.commit()
            assert len(results) == 3
            assert results[0] == '/test_multitransactions/freddy'
            assert results[2].startswith('/test_multitransactions/smith0') is True

        from kazoo.exceptions import RolledBackError, NoNodeError
        for i, zk in enumerate([genuine_zk, fake_zk]):
            print("Processing ZK", i)
            t = zk.transaction()
            t.create('/test_multitransactions/q')
            t.delete('/test_multitransactions/a')
            t.create('/test_multitransactions/x')
            results = t.commit()
            print("Results", results)
            assert results[0].__class__ == RolledBackError
            assert results[1].__class__ == NoNodeError
            assert zk.exists('/test_multitransactions/q') is None
            assert zk.exists('/test_multitransactions/a') is None
            assert zk.exists('/test_multitransactions/x') is None
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def test_filtered_list():

    fake_zk = None
    try:
        fake_zk = get_fake_zk(True)
        fake_zk.create('/test_filteredList')
        ephemerals = [f'ephemeral{i}' for i in range(3)]
        persistents = [f'persistent{i}' for i in range(3)]

        _event = None

        def watch_children_callback(event):
            print("children watch called")
            nonlocal _event
            _event = event

        fake_zk.get_filtered_children('/test_filteredList', list_type=0, watch=watch_children_callback)

        for node in ephemerals:
            fake_zk.create("/test_filteredList/" + node, ephemeral=True)
        for node in persistents:
            fake_zk.create("/test_filteredList/" + node, ephemeral=False)

        assert(sorted(fake_zk.get_filtered_children('/test_filteredList', list_type=0)[0]) == sorted(ephemerals + persistents))
        assert(sorted(fake_zk.get_filtered_children('/test_filteredList', list_type=1)[0]) == sorted(persistents))
        assert(sorted(fake_zk.get_filtered_children('/test_filteredList', list_type=2)[0]) == sorted(ephemerals))

        assert(_event == WatchedEvent(type='CHILD', state='CONNECTED', path='/test_filteredList'))
    finally:
        close_zk_clients([fake_zk])


def test_multi_read():
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk(True)
        fake_zk = get_fake_zk(True)
        for multi_zk in [genuine_zk, fake_zk]:
            multi_zk.create('/test_multi_read')
            t = multi_zk.transaction()
            t.create('/test_multi_read/freddy', b"rico")
            t.create('/test_multi_read/fred', ephemeral=True)
            t.create('/test_multi_read/smith')
            results = t.commit()
            assert len(results) == 3
            assert results[0] == '/test_multi_read/freddy'
            assert results[2] == '/test_multi_read/smith'

            t = multi_zk.multi_read()
            t.get_children('/test_multi_read', None)
            t.get_children('/test_multi_read/fre', None)
            t.get('/test_multi_read/smit', None)
            t.get('/test_multi_read/freddy', None)
            results = t.commit()
            assert len(results) == 4
            print(results)
            assert 'smith' in results[0]
            assert 'freddy' in results[0]
            from kazoo.exceptions import NoNodeError
            assert results[1].__class__ == NoNodeError
            assert results[2].__class__ == NoNodeError
            assert results[3][0] == b"rico"

        # test filtered_list in multi
        t = fake_zk.multi_read()
        t.get_children3('/test_multi_read', 0, None)
        t.get_children3('/test_multi_read', 1, None)
        t.get_children3('/test_multi_read', 2, None)
        t.get_children3('/test_multi_read/fre', 1,None)

        results = t.commit()
        assert len(results) == 4
        assert sorted(results[0][0]) == ['fred', 'freddy', 'smith']
        assert sorted(results[1][0]) == ['freddy', 'smith']
        assert results[2][0] == ['fred']
        from kazoo.exceptions import NoNodeError
        assert results[3].__class__ == NoNodeError
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def exists(zk, path):
    result = zk.exists(path)
    return result is not None


def get(zk, path):
    result = zk.get(path)
    return result[0]


def get_children(zk, path):
    return [elem for elem in list(sorted(zk.get_children(path))) if elem not in 'zookeeper']


READ_REQUESTS = [
    ("exists", exists),
    ("get", get),
    ("get_children", get_children),
]


def create(zk, path, data):
    zk.create(path, data.encode())


def set_data(zk, path, data):
    zk.set(path, data.encode())


WRITE_REQUESTS = [
    ("create", create),
    ("set_data", set_data),
]


def delete(zk, path):
    zk.delete(path)


DELETE_REQUESTS = [
    ("delete", delete)
]


class Request(object):
    def __init__(self, name, arguments, callback, is_return):
        self.name = name
        self.arguments = arguments
        self.callback = callback
        self.is_return = is_return

    def __str__(self):
        arg_str = ', '.join([str(k) + "=" + str(v) for k, v in self.arguments.items()])
        return "ZKRequest name {} with arguments {}".format(self.name, arg_str)


def generate_requests(prefix="/", iters=1):
    requests = []
    existing_paths = []
    for i in range(iters):
        for _ in range(100):
            rand_length = random.randint(0, 10)
            path = prefix
            for j in range(1, rand_length):
                path = create_random_path(path, 1)
                existing_paths.append(path)
                value = random_string(1000)
                request = Request("create", {"path": path, "value": value[0:10]},
                                  lambda zk, path=path, value=value: create(zk, path, value), False)
                requests.append(request)

        for _ in range(100):
            path = random.choice(existing_paths)
            value = random_string(100)
            request = Request("set", {"path": path, "value": value[0:10]},
                              lambda zk, _path=path, _value=value: set_data(zk, _path, _value), False)
            requests.append(request)

        for _ in range(100):
            path = random.choice(existing_paths)
            callback = random.choice(READ_REQUESTS)

            def read_func1(zk, path=path, callback=callback):
                return callback[1](zk, path)

            request = Request(callback[0], {"path": path}, read_func1, True)
            requests.append(request)

        for _ in range(30):
            path = random.choice(existing_paths)
            request = Request("delete", {"path": path}, lambda zk, path=path: delete(zk, path), False)

        for _ in range(100):
            path = random.choice(existing_paths)
            callback = random.choice(READ_REQUESTS)

            def read_func2(zk, path=path, callback=callback):
                return callback[1](zk, path)

            request = Request(callback[0], {"path": path}, read_func2, True)
            requests.append(request)
    return requests


def test_random_requests(started_cluster):
    genuine_zk = fake_zk = None
    try:
        requests = generate_requests("/test_random_requests", 10)
        print("Generated", len(requests), "requests")
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        genuine_zk.create("/test_random_requests")
        fake_zk.create("/test_random_requests")
        for i, request in enumerate(requests):
            genuine_throw = False
            fake_throw = False
            fake_result = None
            genuine_result = None
            try:
                genuine_result = request.callback(genuine_zk)
            except Exception as ex:
                print("Genuine, i", i, "request", request)
                print("Genuine exception", str(ex))
                print("Genuine exception end")
                genuine_throw = True

            try:
                fake_result = request.callback(fake_zk)
            except Exception as ex:
                print("Fake, i", i, "request", request)
                print("Fake exception", str(ex))
                print("Fake exception end")
                fake_throw = True

            assert fake_throw == genuine_throw, "Fake throw genuine not or vise versa request {}"
            assert fake_result == genuine_result, "Zookeeper results differ"
        root_children_genuine = [elem for elem in list(sorted(genuine_zk.get_children("/test_random_requests"))) if
                                 elem not in ('clickhouse', 'zookeeper')]
        root_children_fake = [elem for elem in list(sorted(fake_zk.get_children("/test_random_requests"))) if
                              elem not in ('clickhouse', 'zookeeper')]
        assert root_children_fake == root_children_genuine
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def test_end_of_session(started_cluster):
    fake_zk1 = None
    fake_zk2 = None
    genuine_zk1 = None
    genuine_zk2 = None

    try:
        fake_zk1 = KazooClient(hosts=cluster.get_instance_ip("node1") + ":8101")
        fake_zk1.start()
        fake_zk2 = KazooClient(hosts=cluster.get_instance_ip("node1") + ":8101")
        fake_zk2.start()
        genuine_zk1 = cluster.get_kazoo_client('zoo1')
        genuine_zk1.start()
        genuine_zk2 = cluster.get_kazoo_client('zoo1')
        genuine_zk2.start()

        fake_zk1.create("/test_end_of_session")
        genuine_zk1.create("/test_end_of_session")
        fake_ephemeral_event = None

        def fake_ephemeral_callback(event):
            print("Fake watch triggered")
            nonlocal fake_ephemeral_event
            fake_ephemeral_event = event

        genuine_ephemeral_event = None

        def genuine_ephemeral_callback(event):
            print("Genuine watch triggered")
            nonlocal genuine_ephemeral_event
            genuine_ephemeral_event = event

        assert fake_zk2.exists("/test_end_of_session") is not None
        assert genuine_zk2.exists("/test_end_of_session") is not None

        fake_zk1.create("/test_end_of_session/ephemeral_node", ephemeral=True)
        genuine_zk1.create("/test_end_of_session/ephemeral_node", ephemeral=True)

        assert fake_zk2.exists("/test_end_of_session/ephemeral_node", watch=fake_ephemeral_callback) is not None
        assert genuine_zk2.exists("/test_end_of_session/ephemeral_node", watch=genuine_ephemeral_callback) is not None

        print("Stopping genuine zk")
        close_zk_client(genuine_zk1)

        print("Stopping fake zk")
        close_zk_client(fake_zk1)

        assert fake_zk2.exists("/test_end_of_session/ephemeral_node") is None
        assert genuine_zk2.exists("/test_end_of_session/ephemeral_node") is None

        assert fake_ephemeral_event == genuine_ephemeral_event

    finally:
        close_zk_clients([fake_zk1, fake_zk2, genuine_zk1, genuine_zk2])


def test_end_of_watches_session(started_cluster):
    fake_zk1 = None
    fake_zk2 = None
    try:
        fake_zk1 = KazooClient(hosts=cluster.get_instance_ip("node1") + ":8101")
        fake_zk1.start()

        fake_zk2 = KazooClient(hosts=cluster.get_instance_ip("node1") + ":8101")
        fake_zk2.start()

        fake_zk1.create("/test_end_of_watches_session")

        dummy_set = 0

        def dummy_callback(event):
            nonlocal dummy_set
            dummy_set += 1
            print(event)

        for child_node in range(100):
            fake_zk1.create("/test_end_of_watches_session/" + str(child_node))
            fake_zk1.get_children("/test_end_of_watches_session/" + str(child_node), watch=dummy_callback)

        fake_zk2.get_children("/test_end_of_watches_session/" + str(0), watch=dummy_callback)
        fake_zk2.get_children("/test_end_of_watches_session/" + str(1), watch=dummy_callback)

        close_zk_client(fake_zk1)

        for child_node in range(100):
            fake_zk2.create("/test_end_of_watches_session/" + str(child_node) + "/" + str(child_node), b"somebytes")

        assert dummy_set == 2
    finally:
        close_zk_clients([fake_zk1, fake_zk2])


def test_concurrent_watches(started_cluster):
    global_path = "/test_concurrent_watches_0"
    dumb_watch_triggered_counter = 0
    all_paths_triggered = []
    existing_path = []
    all_paths_created = []
    watches_created = 0
    trigger_called = 0

    lock = threading.Lock()

    fake_zk = get_fake_zk()
    fake_zk.start()

    def create_path_and_watch(zk, i):
        nonlocal watches_created
        nonlocal all_paths_created
        nonlocal existing_path
        nonlocal dumb_watch_triggered_counter
        nonlocal all_paths_triggered
        global_path = "/test_concurrent_watches_0"
        node_path = global_path + "/" + str(i)

        zk.ensure_path(node_path)

        def dumb_watch(event):
            nonlocal dumb_watch_triggered_counter
            nonlocal all_paths_triggered
            with lock:
                dumb_watch_triggered_counter += 1
                all_paths_triggered.append(event.path)

        zk.get(global_path + "/" + str(i), watch=dumb_watch)
        with lock:
            all_paths_created.append(global_path + "/" + str(i))
            watches_created += 1
            existing_path.append(i)

    def trigger_watch(fake_zk, i):
        nonlocal trigger_called
        nonlocal existing_path
        global_path = "/test_concurrent_watches_0"

        with lock:
            trigger_called += 1
        fake_zk.set(global_path + "/" + str(i), b"somevalue")
        try:
            existing_path.remove(i)
        except:
            pass

    def call(total):
        thread_zk = get_fake_zk()
        thread_zk.start()
        for i in range(total):
            create_path_and_watch(thread_zk, random.randint(0, 1000))
            time.sleep(0.1)
            try:
                with lock:
                    rand_num = random.choice(existing_path)
                trigger_watch(thread_zk, rand_num)
            except:
                pass
        while existing_path:
            try:
                with lock:
                    rand_num = random.choice(existing_path)
                trigger_watch(thread_zk, rand_num)
            except:
                pass
        close_zk_client(thread_zk)

    global_path = "/test_concurrent_watches_0"

    fake_zk.delete(global_path, recursive=True)
    fake_zk.create(global_path)

    threads = []
    num_threads = 10
    calls_per_thread = 100

    for _ in range(num_threads):
        t = threading.Thread(target=call, args=(calls_per_thread,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # waiting for late watches
    for i in range(50):
        if dumb_watch_triggered_counter == calls_per_thread * num_threads:
            break

        time.sleep(0.1)

    with lock:
        assert watches_created == calls_per_thread * num_threads
        assert trigger_called >= calls_per_thread * num_threads
        assert len(existing_path) == 0
    if dumb_watch_triggered_counter != calls_per_thread * num_threads:
        print("All created paths", all_paths_created)
        print("All triggered paths", all_paths_triggered)
        print("All paths len", len(all_paths_created))
        print("All triggered len", len(all_paths_triggered))
        print("Diff", list(set(all_paths_created) - set(all_paths_triggered)))

    assert dumb_watch_triggered_counter == calls_per_thread * num_threads
    close_zk_client(fake_zk)


def test_system_nodes(started_cluster):
    genuine_zk = fake_zk = None
    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        cluster_config = (b'server.1=node1:8103:participant\nserver.2=node2:8103:participant\nserver.3=node3:8103'
                          b':participant\nversion=0')
        assert fake_zk.get('/zookeeper/config')[0] == cluster_config
    finally:
        close_zk_clients([genuine_zk, fake_zk])


def test_unregister_watch(started_cluster):
    genuine_zk = fake_zk = None
    datas = [[] for i in range(2)]
    
    def send_4lw_cmd(host, port, cmd):
        client = None
        try:
            client = socket.socket()
            client.settimeout(10)
            client.connect((host, port))

            client.send(cmd.encode())
            data = client.recv(100_000)
            data = data.decode()
            return data
        finally:
            if client is not None:
                client.close()

    try:
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        hosts = [[cluster.get_instance_ip("zoo1"), 2181], [cluster.get_instance_ip("node1"), 8101]]
        for index, zk in enumerate([genuine_zk, fake_zk]):
            def exists_watch(event):
                print(f"Exists watch triggered! Event: {event}")

            def list_watch(event):
                print(f"List watch triggered! Event: {event}")

            znode_path = "/test_unregister_watch"

            zk.create(znode_path, b"initial data")

            if zk.exists(znode_path, watch=exists_watch):
                print(f"Znode {znode_path} exists.")
            else:
                print(f"Znode {znode_path} does not exist. Watch set for creation or deletion.")
                
            zk.get_children(znode_path, watch=list_watch)
            
            datas[index].append(send_4lw_cmd(hosts[index][0], hosts[index][1], cmd='wchc'))
            
            zk.create(f"{znode_path}/child", b"initial data")

            time.sleep(2)
            datas[index].append(send_4lw_cmd(hosts[index][0], hosts[index][1], cmd='wchc'))
            
            zk.set(znode_path, b"new data")

            time.sleep(2)
            datas[index].append(send_4lw_cmd(hosts[index][0], hosts[index][1], cmd='wchc'))
            
        for index, data in enumerate(datas):
            print(f"Data {index}: {data}")
            
        for data0, data1 in zip(datas[0], datas[1]):
            assert len(data0[0].splitlines()) == len(data1[1].splitlines())  
    finally:
        close_zk_clients([genuine_zk, fake_zk])