import csv
import re
import socket
import time

import pytest
from kazoo.client import KazooClient

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_client

cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def clear_znodes():
    zk = None
    try:
        zk = get_fake_zk(node3.name, timeout=30.0)
        nodes = zk.get_children('/')
        for node in [n for n in nodes if 'test_4lw_' in n]:
            zk.delete('/' + node)
    finally:
        close_zk_client(zk)


def wait_node(node):
    node.wait_for_join_cluster()


def wait_nodes():
    for n in [node1, node2, node3]:
        wait_node(n)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":8101", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_keeper_socket(node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 8101))
    return client


def close_keeper_socket(cli):
    if cli is not None:
        cli.close()


def reset_node_stats(node_name=node1.name):
    client = None
    try:
        client = get_keeper_socket(node_name)
        client.send(b'srst')
        client.recv(10)
    finally:
        if client is not None:
            client.close()


def reset_conn_stats(node_name=node1.name):
    client = None
    try:
        client = get_keeper_socket(node_name)
        client.send(b'crst')
        client.recv(10_000)
    finally:
        if client is not None:
            client.close()


def test_cmd_ruok(started_cluster):
    client = None
    try:
        wait_nodes()
        data = node1.send_4lw_cmd(cmd='ruok')
        assert data == 'imok'
    finally:
        close_keeper_socket(client)


def do_some_action(zk, create_cnt=0, get_cnt=0, set_cnt=0, ephemeral_cnt=0, watch_cnt=0, delete_cnt=0):
    assert create_cnt >= get_cnt
    assert create_cnt >= set_cnt
    assert create_cnt >= watch_cnt
    assert create_cnt >= delete_cnt
    # ensure not delete watched node
    assert create_cnt >= (delete_cnt + watch_cnt)

    for i in range(create_cnt):
        zk.create("/test_4lw_normal_node_" + str(i), b"")

    for i in range(get_cnt):
        zk.get("/test_4lw_normal_node_" + str(i))

    for i in range(set_cnt):
        zk.set("/test_4lw_normal_node_" + str(i), b"new-value")

    for i in range(ephemeral_cnt):
        zk.create("/test_4lw_ephemeral_node_" + str(i), ephemeral=True)

    fake_ephemeral_event = None

    def fake_ephemeral_callback(event):
        print("Fake watch triggered")
        nonlocal fake_ephemeral_event
        fake_ephemeral_event = event

    for i in range(watch_cnt):
        zk.exists("/test_4lw_normal_node_" + str(i), watch=fake_ephemeral_callback)

    for i in range(create_cnt - delete_cnt, create_cnt):
        zk.delete("/test_4lw_normal_node_" + str(i))

    # wait action result to avoid stale statistics.
    time.sleep(1)


def test_cmd_mntr(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        # reset stat first
        reset_node_stats(node1.name)

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10, get_cnt=10, set_cnt=5, ephemeral_cnt=2, watch_cnt=2, delete_cnt=2)

        data = node1.send_4lw_cmd(cmd='mntr')

        print("mntr output -------------------------------------")
        print(data)

        reader = csv.reader(data.split('\n'), delimiter='\t')
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0]] = row[1]

        assert len(result["zk_version"]) != 0

        assert int(result["zk_avg_latency"]) >= 0
        assert int(result["zk_max_latency"]) >= 0
        assert int(result["zk_min_latency"]) >= 0

        assert int(result["zk_min_latency"]) <= int(result["zk_avg_latency"])
        assert int(result["zk_max_latency"]) >= int(result["zk_avg_latency"])

        assert int(result["zk_num_alive_connections"]) == 1
        assert int(result["zk_outstanding_requests"]) == 0

        assert result["zk_server_state"] == "leader"
        assert result["zk_is_leader"] == "1"

        # contains:
        #   10 nodes created by test
        #   1 root node
        assert int(result["zk_znode_count"]) == 11
        assert int(result["zk_watch_count"]) == 2
        assert int(result["zk_ephemerals_count"]) == 2
        assert int(result["zk_approximate_data_size"]) > 0

        assert int(result["zk_open_file_descriptor_count"]) > 0
        assert int(result["zk_max_file_descriptor_count"]) > 0

        assert int(result["zk_followers"]) == 2
        assert int(result["zk_synced_followers"]) == 2

        # contains 31 user request response and some responses for server startup
        assert int(result["zk_packets_sent"]) >= 31
        assert int(result["zk_packets_received"]) >= 31
    finally:
        close_zk_client(zk)


def test_cmd_srst(started_cluster):
    client = None
    try:
        wait_nodes()
        clear_znodes()

        data = node1.send_4lw_cmd(cmd='srst')
        assert data.strip() == "Server stats reset."

        data = node1.send_4lw_cmd(cmd='mntr')
        assert len(data) != 0

        # print(data)
        reader = csv.reader(data.split('\n'), delimiter='\t')
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0]] = row[1]

        assert int(result["zk_packets_received"]) == 0
        assert int(result["zk_packets_sent"]) == 0

    finally:
        close_keeper_socket(client)


def test_cmd_conf(started_cluster):
    client = None
    try:
        wait_nodes()
        clear_znodes()

        data = node1.send_4lw_cmd(cmd='conf')

        reader = csv.reader(data.split('\n'), delimiter='=')
        result = {}

        for row in reader:
            if len(row) != 0:
                print(row)
                result[row[0]] = row[1]

        assert result["my_id"] == "1"

        assert result["port"] == "8101"
        assert result["host"] == "node1"

        assert result["internal_port"] == "8103"
        assert result["thread_count"] == "16"
        assert result["snapshot_create_interval"] == "10000"

        assert result["four_letter_word_white_list"] == "*"

        assert result["log_dir"] == "./data/log"
        assert result["snapshot_dir"] == "./data/snapshot"

        assert result["min_session_timeout_ms"] == "1000"
        assert result["max_session_timeout_ms"] == "30000"
        assert result["operation_timeout_ms"] == "1000"
        assert result["dead_session_check_period_ms"] == "500"
        assert result["heart_beat_interval_ms"] == "500"
        assert result["election_timeout_lower_bound_ms"] == "5000"
        assert result["election_timeout_upper_bound_ms"] == "10000"

        assert result["reserved_log_items"] == "1000000"
        assert result["snapshot_distance"] == "3000000"
        assert result["max_stored_snapshots"] == "5"

        assert result["shutdown_timeout"] == "5000"
        assert result["startup_timeout"] == "6000000"

        assert result["raft_logs_level"] == "debug"
        assert result["rotate_log_storage_interval"] == "100000"
        assert result["log_fsync_mode"] == "fsync_parallel"

        assert result["log_fsync_interval"] == "1000"
        assert result["nuraft_thread_size"] == "32"
        assert result["fresh_log_gap"] == "200"

    finally:
        close_keeper_socket(client)


def test_cmd_isro(started_cluster):
    wait_nodes()
    assert node1.send_4lw_cmd('isro') == 'rw'
    assert node2.send_4lw_cmd('isro') == 'ro'


def test_cmd_srvr(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        reset_node_stats(node1.name)

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = node1.send_4lw_cmd(cmd='srvr')

        print("srvr output -------------------------------------")
        print(data)

        reader = csv.reader(data.split('\n'), delimiter=':')
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0].strip()] = row[1].strip()

        assert 'RaftKeeper version' in result
        assert 'Latency min/avg/max' in result
        assert result['Received'] == '11'
        assert result['Sent'] == '11'
        assert int(result['Connections']) == 1
        assert int(result['Zxid']) > 14
        assert result['Mode'] == 'leader'
        assert result['Node count'] == '11'

    finally:
        close_zk_client(zk)


def test_cmd_stat(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats(node1.name)
        reset_conn_stats(node1.name)

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = node1.send_4lw_cmd(cmd='stat')

        print("stat output -------------------------------------")
        print(data)

        # keeper statistics
        stats = [n for n in data.split('\n') if '=' not in n]
        reader = csv.reader(stats, delimiter=':')
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0].strip()] = row[1].strip()

        assert 'RaftKeeper version' in result
        assert 'Latency min/avg/max' in result
        assert result['Received'] == '11'
        assert result['Sent'] == '11'
        assert int(result['Connections']) == 1
        assert int(result['Zxid']) > 14
        assert result['Mode'] == 'leader'
        assert result['Node count'] == '11'

        # filter connection statistics
        cons = [n for n in data.split('\n') if '=' in n]
        # filter connection created by 'cons'
        cons = [n for n in cons if 'recved=0' not in n and len(n) > 0]
        assert len(cons) == 1

        conn_stat = re.match(r'(.*?)[:].*[(](.*?)[)].*', cons[0].strip(), re.S).group(2)
        assert conn_stat is not None

        result = {}
        for col in conn_stat.split(','):
            col = col.strip().split('=')
            result[col[0]] = col[1]

        assert result['recved'] == '11'
        assert result['sent'] == '11'

    finally:
        close_zk_client(zk)


def test_cmd_cons(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_conn_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = node1.send_4lw_cmd(cmd='cons')

        print("cons output -------------------------------------")
        print(data)

        # filter connection created by 'cons'
        cons = [n for n in data.split('\n') if 'recved=0' not in n and len(n) > 0]
        assert len(cons) == 1

        conn_stat = re.match(r'(.*?)[:].*[(](.*?)[)].*', cons[0].strip(), re.S).group(2)
        assert conn_stat is not None

        result = {}
        for col in conn_stat.split(','):
            col = col.strip().split('=')
            result[col[0]] = col[1]

        assert result['recved'] == '11'
        assert result['sent'] == '11'
        assert 'sid' in result
        assert result['lop'] == 'Create'
        assert 'est' in result
        assert result['to'] == '30000'
        assert result['lcxid'] == '0xa'
        assert 'lzxid' in result
        assert 'lresp' in result
        assert int(result['llat']) >= 0
        assert int(result['minlat']) >= 0
        assert int(result['avglat']) >= 0
        assert int(result['maxlat']) >= 0

    finally:
        close_zk_client(zk)


def test_cmd_crst(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_conn_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = node1.send_4lw_cmd(cmd='crst')

        print("crst output -------------------------------------")
        print(data)

        data = node1.send_4lw_cmd(cmd='cons')
        print("cons output -------------------------------------")
        print(data)

        # 2 connections, 1 for 'cons' command, 1 for zk
        # but if there is reconnection len(cons) may large than 2
        cons = [n for n in data.split('\n') if len(n) > 0]
        assert len(cons) >= 2

        zk_con = None
        if 'sid' in cons[0]:
            zk_con = cons[0]
        else:
            zk_con = cons[1]

        conn_stat = re.match(r'(.*?)[:].*[(](.*?)[)].*', zk_con.strip(), re.S).group(2)
        assert conn_stat is not None

        result = {}
        for col in conn_stat.split(','):
            col = col.strip().split('=')
            result[col[0]] = col[1]

        assert result['recved'] == '0'
        assert result['sent'] == '0'
        assert 'sid' in result
        assert result['lop'] == 'NA'
        assert 'est' in result
        assert result['to'] == '30000'
        assert 'lcxid' not in result
        assert result['lzxid'] == '0xffffffffffffffff'
        assert result['lresp'] == '0'
        assert int(result['llat']) == 0
        assert int(result['minlat']) == 0
        assert int(result['avglat']) == 0
        assert int(result['maxlat']) == 0

    finally:
        close_zk_client(zk)


def test_cmd_dump(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, ephemeral_cnt=2)

        data = node1.send_4lw_cmd(cmd='dump')

        print("dump output -------------------------------------")
        print(data)

        list_data = data.split('\n')

        session_count = int(re.match(r'.*[(](.*?)[)].*', list_data[0], re.S).group(1))
        assert session_count == 1

        assert '\t' + '/test_4lw_ephemeral_node_0' in list_data
        assert '\t' + '/test_4lw_ephemeral_node_1' in list_data
    finally:
        close_zk_client(zk)


def test_cmd_wchs(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=2, watch_cnt=2)

        data = node1.send_4lw_cmd(cmd='wchs')

        print("wchs output -------------------------------------")
        print(data)

        list_data = [n for n in data.split('\n') if len(n.strip()) > 0]

        # 37 connections watching 632141 paths
        # Total watches:632141
        matcher = re.match(r'([0-9].*) connections watching ([0-9].*) paths', list_data[0], re.S)
        conn_count = int(matcher.group(1))
        watch_path_count = int(matcher.group(2))
        watch_count = int(re.match(r'Total watches:([0-9].*)', list_data[1], re.S).group(1))

        assert conn_count == 1
        assert watch_path_count == 2
        assert watch_count == 2
    finally:
        close_zk_client(zk)


def test_cmd_wchc(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=2, watch_cnt=2)

        data = node1.send_4lw_cmd(cmd='wchc')

        print("wchc output -------------------------------------")
        print(data)

        list_data = [n for n in data.split('\n') if len(n.strip()) > 0]

        assert len(list_data) == 3
        assert '\t' + '/test_4lw_normal_node_0' in list_data
        assert '\t' + '/test_4lw_normal_node_1' in list_data
    finally:
        close_zk_client(zk)


def test_cmd_wchp(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=2, watch_cnt=2)

        data = node1.send_4lw_cmd(cmd='wchp')

        print("wchp output -------------------------------------")
        print(data)

        list_data = [n for n in data.split('\n') if len(n.strip()) > 0]

        assert len(list_data) == 4
        assert '/test_4lw_normal_node_0' in list_data
        assert '/test_4lw_normal_node_1' in list_data
    finally:
        close_zk_client(zk)


def test_cmd_csnp(started_cluster):
    zk = None
    try:
        wait_nodes()
        zk = get_fake_zk(node1.name, timeout=30.0)
        data = node1.send_4lw_cmd(cmd="csnp")

        print("csnp output -------------------------------------")
        print(data)

        try:
            int(data)
            assert True
        except ValueError:
            assert False
    finally:
        close_zk_client(zk)


def test_cmd_lgif(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=100)

        data = node1.send_4lw_cmd(cmd="lgif")

        print("lgif output -------------------------------------")
        print(data)

        reader = csv.reader(data.split("\n"), delimiter="\t")
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0]] = row[1]

        assert int(result["first_log_idx"]) == 1
        assert int(result["first_log_term"]) == 1
        assert int(result["last_log_idx"]) >= 1
        assert int(result["last_log_term"]) == 1
        assert int(result["last_committed_log_idx"]) >= 1
        assert int(result["leader_committed_log_idx"]) >= 1
        assert int(result["target_committed_log_idx"]) >= 1
        assert int(result["last_snapshot_idx"]) >= 1
    finally:
        close_zk_client(zk)


def test_cmd_rqld(started_cluster):
    wait_nodes()
    # node2 can not be leader
    for node in [node1, node3]:
        data = node.send_4lw_cmd(cmd="rqld")
        assert data == "Sent leadership request to leader."

        print("rqld output -------------------------------------")
        print(data)

        if not node.is_leader():
            # pull wait to become leader
            retry = 0
            # TODO not a restrict way
            while not node.is_leader() and retry < 30:
                time.sleep(1)
                retry += 1
            if retry == 30:
                print(
                    node.name
                    + " does not become leader after 30s, maybe there is something wrong."
                )
        assert node.is_leader()
