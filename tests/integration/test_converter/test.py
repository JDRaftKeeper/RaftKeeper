#!/usr/bin/env python3
import os

import pytest
from kazoo.client import KazooClient

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_client

cluster = RaftKeeperCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/keeper_config.xml', 'configs/logs_conf.xml'],
                            stay_alive=True)


def start_zookeeper():
    node.exec_in_container(['bash', '-c', '/opt/zookeeper/bin/zkServer.sh start'])


def stop_zookeeper():
    node.exec_in_container(['bash', '-c', '/opt/zookeeper/bin/zkServer.sh stop'])


def clear_zookeeper():
    node.exec_in_container(['bash', '-c', 'rm -fr /zookeeper/*'])


def restart_and_clear_zookeeper():
    stop_zookeeper()
    clear_zookeeper()
    start_zookeeper()


def clear_raftkeeper_data():
    node.exec_in_container(
        ['bash', '-c', 'rm -fr /var/lib/raftkeeper/data/raft_log/* /var/lib/raftkeeper/data/raft_snapshot/*'])


def convert_zookeeper_data():
    cmd = '/usr/bin/raftkeeper converter --zookeeper-logs-dir /zookeeper/version-2/ --zookeeper-snapshots-dir  ' \
          '/zookeeper/version-2/ --output-dir /var/lib/raftkeeper/data/raft_snapshot'
    output = node.exec_in_container(['bash', '-c', cmd])
    print("convert_zookeeper_data output: ", output)


def stop_raftkeeper():
    node.stop_raftkeeper()


def start_raftkeeper():
    node.start_raftkeeper(start_wait=True)


def copy_zookeeper_data(make_zk_snapshots):
    stop_zookeeper()

    if make_zk_snapshots:  # force zookeeper to create snapshot
        start_zookeeper()
        stop_zookeeper()

    stop_raftkeeper()
    clear_raftkeeper_data()
    convert_zookeeper_data()
    start_zookeeper()
    start_raftkeeper()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_genuine_zk(timeout=60.0):
    _genuine_zk_instance = KazooClient(hosts=cluster.get_instance_ip('node') + ":2181", timeout=timeout)
    _genuine_zk_instance.start()
    return _genuine_zk_instance


def compare_stats(stat1, stat2, path):
    assert stat1.czxid == stat2.czxid, "path " + path + " cxzids not equal for stats: " + str(
        stat1.czxid) + " != " + str(stat2.zxid)
    assert stat1.mzxid == stat2.mzxid, "path " + path + " mxzids not equal for stats: " + str(
        stat1.mzxid) + " != " + str(stat2.mzxid)
    assert stat1.version == stat2.version, "path " + path + " versions not equal for stats: " + str(
        stat1.version) + " != " + str(stat2.version)
    assert stat1.cversion == stat2.cversion, "path " + path + " cversions not equal for stats: " + str(
        stat1.cversion) + " != " + str(stat2.cversion)
    # assert stat1.aversion == stat2.aversion, "path " + path + " aversions not equal for stats: " + str(
    # stat1.aversion) + " != " + str(stat2.aversion)  ACL
    assert stat1.ephemeralOwner == stat2.ephemeralOwner, "path " + path + " ephemeralOwners not equal for stats: " + str(
        stat1.ephemeralOwner) + " != " + str(stat2.ephemeralOwner)
    assert stat1.dataLength == stat2.dataLength, "path " + path + " ephemeralOwners not equal for stats: " + str(
        stat1.dataLength) + " != " + str(stat2.dataLength)
    assert stat1.numChildren == stat2.numChildren, "path " + path + " numChildren not equal for stats: " + str(
        stat1.numChildren) + " != " + str(stat2.numChildren)
    # assert stat1.pzxid == stat2.pzxid, "path " + path + " pzxid not equal for stats: " + str(stat1.pzxid) + " != "
    # + str(stat2.pzxid) from fuzzy snapshot


def compare_states(zk1, zk2, path="/"):
    data1, stat1 = zk1.get(path)
    data2, stat2 = zk2.get(path)
    print("Left Stat", stat1)
    print("Right Stat", stat2)
    assert data1 == data2, "Data not equal on path " + str(path)
    # both paths have strange stats
    if path not in ("/", "/zookeeper"):
        compare_stats(stat1, stat2, path)

    first_children = list(sorted(zk1.get_children(path)))
    second_children = list(sorted(zk2.get_children(path)))
    print("Got children left", first_children)
    print("Got children rigth", second_children)
    assert first_children == second_children, "Childrens are not equal on path " + path

    for children in first_children:
        print("Checking child", os.path.join(path, children))
        compare_states(zk1, zk2, os.path.join(path, children))


@pytest.mark.parametrize(
    'create_snapshots',
    [
        True, False
    ]
)
def test_smoke(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()

    genuine_connection = get_genuine_zk()
    genuine_connection.create("/test", b"data")

    assert genuine_connection.get("/test")[0] == b"data"

    close_zk_client(genuine_connection)
    copy_zookeeper_data(create_snapshots)

    genuine_connection = get_genuine_zk()
    fake_connection = node.get_fake_zk()

    compare_states(genuine_connection, fake_connection)

    close_zk_client(genuine_connection)
    close_zk_client(fake_connection)


def get_bytes(s):
    return s.encode()


@pytest.mark.parametrize(
    'create_snapshots',
    [
        True, False
    ]
)
def test_simple_crud_requests(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()

    genuine_connection = get_genuine_zk()
    for i in range(100):
        genuine_connection.create("/test_create" + str(i), get_bytes("data" + str(i)))

    # some set queries
    for i in range(10):
        for j in range(i + 1):
            genuine_connection.set("/test_create" + str(i), get_bytes("value" + str(j)))

    for i in range(10, 20):
        genuine_connection.delete("/test_create" + str(i))

    path = "/test_create_deep"
    for i in range(10):
        genuine_connection.create(path, get_bytes("data" + str(i)))
        path = os.path.join(path, str(i))

    genuine_connection.create("/test_sequential", b"")
    for i in range(10):
        genuine_connection.create("/test_sequential/" + "a" * i + "-", get_bytes("dataX" + str(i)), sequence=True)

    genuine_connection.create("/test_ephemeral", b"")
    # for i in range(10):  create_snapshots is false raft deserialize session timeout is 0, so do'not compare
    # ephemeral znode genuine_connection.create("/test_ephemeral/" + str(i), get_bytes("dataX" + str(i)),
    # ephemeral=True)

    close_zk_client(genuine_connection)
    copy_zookeeper_data(create_snapshots)

    genuine_connection = get_genuine_zk()
    fake_connection = node.get_fake_zk()

    compare_states(genuine_connection, fake_connection)

    # especially ensure that counters are the same
    for i in range(10):
        genuine_connection.create("/test_sequential/" + "a" * 10 + "-", get_bytes("dataX" + str(i)), sequence=True)
        fake_connection.create("/test_sequential/" + "a" * 10 + "-", get_bytes("dataX" + str(i)), sequence=True)

    first_children = list(sorted(genuine_connection.get_children("/test_sequential")))
    second_children = list(sorted(fake_connection.get_children("/test_sequential")))
    assert first_children == second_children, "Childrens are not equal on path " + path

    close_zk_client(genuine_connection)
    close_zk_client(fake_connection)


@pytest.mark.parametrize(
    'create_snapshots',
    [
        True, False
    ]
)
def test_multi_and_failed_requests(started_cluster, create_snapshots):
    restart_and_clear_zookeeper()

    genuine_connection = get_genuine_zk()
    genuine_connection.create('/test_multitransactions')
    for i in range(10):
        t = genuine_connection.transaction()
        t.create('/test_multitransactions/freddy' + str(i), get_bytes('data' + str(i)))
        # t.create('/test_multitransactions/fred' + str(i), get_bytes('value' + str(i)), ephemeral=True)
        # create_snapshots is false raft deserialize session timeout is 0, so do'not compare ephemeral znode
        t.create('/test_multitransactions/smith' + str(i), get_bytes('entity' + str(i)), sequence=True)
        t.set_data('/test_multitransactions', get_bytes("somedata" + str(i)))
        t.commit()

    with pytest.raises(Exception):
        genuine_connection.set('/test_multitransactions/freddy0', get_bytes('mustfail' + str(i)), version=1)

    t = genuine_connection.transaction()

    t.create('/test_bad_transaction', get_bytes('data' + str(1)))
    t.check('/test_multitransactions', version=32)
    t.create('/test_bad_transaction1', get_bytes('data' + str(2)))
    # should fail
    t.commit()

    assert genuine_connection.exists('/test_bad_transaction') is None
    assert genuine_connection.exists('/test_bad_transaction1') is None

    t = genuine_connection.transaction()
    t.create('/test_bad_transaction2', get_bytes('data' + str(1)))
    t.delete('/test_multitransactions/freddy0', version=5)

    # should fail
    t.commit()
    assert genuine_connection.exists('/test_bad_transaction2') is None
    assert genuine_connection.exists('/test_multitransactions/freddy0') is not None

    close_zk_client(genuine_connection)
    copy_zookeeper_data(create_snapshots)

    genuine_connection = get_genuine_zk()
    fake_connection = node.get_fake_zk()

    compare_states(genuine_connection, fake_connection)

    close_zk_client(genuine_connection)
    close_zk_client(fake_connection)

# @pytest.mark.parametrize(
#     ('create_snapshots'),
#     [
#         True, False
#     ]
# )
# ACL
# def test_acls(started_cluster, create_snapshots):
#     restart_and_clear_zookeeper()
#     genuine_connection = get_genuine_zk()
#     genuine_connection.add_auth('digest', 'user1:password1')
#     genuine_connection.add_auth('digest', 'user2:password2')
#     genuine_connection.add_auth('digest', 'user3:password3')
#
#     genuine_connection.create("/test_multi_all_acl", b"data", acl=[make_acl("auth", "", all=True)])
#
#     other_connection = get_genuine_zk()
#     other_connection.add_auth('digest', 'user1:password1')
#     other_connection.set("/test_multi_all_acl", b"X")
#     assert other_connection.get("/test_multi_all_acl")[0] == b"X"
#
#     yet_other_auth_connection = get_genuine_zk()
#     yet_other_auth_connection.add_auth('digest', 'user2:password2')
#
#     yet_other_auth_connection.set("/test_multi_all_acl", b"Y")
#
#     genuine_connection.add_auth('digest', 'user3:password3')
#
# # just to check that we are able to deserialize it genuine_connection.set_acls("/test_multi_all_acl",
# acls=[make_acl("auth", "", read=True, write=False, create=True, delete=True, admin=True)])
#
#     no_auth_connection = get_genuine_zk()
#
#     with pytest.raises(Exception):
#         no_auth_connection.set("/test_multi_all_acl", b"Z")
#
#     copy_zookeeper_data(create_snapshots)
#
#     genuine_connection = get_genuine_zk()
#     genuine_connection.add_auth('digest', 'user1:password1')
#     genuine_connection.add_auth('digest', 'user2:password2')
#     genuine_connection.add_auth('digest', 'user3:password3')
#
#     fake_connection = get_fake_zk()
#     fake_connection.add_auth('digest', 'user1:password1')
#     fake_connection.add_auth('digest', 'user2:password2')
#     fake_connection.add_auth('digest', 'user3:password3')
#
#     compare_states(genuine_connection, fake_connection)
#
# for connection in [genuine_connection, fake_connection]: acls, stat = connection.get_acls("/test_multi_all_acl")
# assert stat.aversion == 1 assert len(acls) == 3 for acl in acls: assert acl.acl_list == ['READ', 'CREATE',
# 'DELETE', 'ADMIN'] assert acl.id.scheme == 'digest' assert acl.perms == 29 assert acl.id.id in (
# 'user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=', 'user2:lo/iTtNMP+gEZlpUNaCqLYO3i5U=', 'user3:wr5Y0kEs9nFX3bKrTMKxrlcFeWo=')
