import pytest
from helpers.cluster import ClickHouseCluster
from helpers.cluster_service import RaftKeeperCluster
import time
import socket
import struct
from multiprocessing.dummy import Pool

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

# from kazoo.protocol.serialization import Connect, read_buffer, write_buffer

cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/logs_conf.xml'],
                             stay_alive=True)


bool_struct = struct.Struct("B")
int_struct = struct.Struct("!i")
int_int_struct = struct.Struct("!ii")
int_int_long_struct = struct.Struct("!iiq")
int_int_int_struct = struct.Struct("!iii")

int_long_int_long_struct = struct.Struct("!iqiq")
long_struct = struct.Struct("!q")
multiheader_struct = struct.Struct("!iBi")
reply_header_struct = struct.Struct("!iqi")
stat_struct = struct.Struct("!qqqqiiiqiiq")



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
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(node_name) + ":8101", timeout=timeout)
    _fake_zk_instance.retry = KazooRetry(ignore_expire=False, max_delay=1.0, max_tries=1)
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


def write_buffer(bytes):
    if bytes is None:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes


def read_buffer(bytes, offset):
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index : index + length], offset


def handshake(node_name=node1.name, session_timeout=11000, session_id=0):
    client = get_keeper_socket(node_name)
    protocol_version = 0
    last_zxid_seen = 0
    session_passwd = b"\x00" * 16
    read_only = 0

    # Handshake serialize and deserialize code is from 'kazoo.protocol.serialization'.

    # serialize handshake
    req = bytearray()
    req.extend(
        int_long_int_long_struct.pack(
            protocol_version, last_zxid_seen, session_timeout, session_id
        )
    )
    req.extend(write_buffer(session_passwd))
    req.extend([1 if read_only else 0])
    # add header
    req = int_struct.pack(45) + req
    print("handshake request - len:", req.hex(), len(req))

    # send request
    client.send(req)
    data = client.recv(1_000)

    # deserialize response
    print("handshake response - len:", data.hex(), len(data))
    # ignore header
    offset = 4
    proto_version, negotiated_timeout, session_id = int_int_long_struct.unpack_from(
        data, offset
    )
    offset += int_int_long_struct.size
    password, offset = read_buffer(data, offset)
    try:
        read_only = bool_struct.unpack_from(data, offset)[0] == 1
        offset += bool_struct.size
    except struct.error:
        read_only = False

    print("negotiated_timeout - session_id", negotiated_timeout, session_id)
    return client

def heartbeat(client):
    length = 8
    xid = 1
    op_num = 11

    # serialize heartbeat
    req = bytearray()
    req.extend(
        int_int_int_struct.pack(
            length, xid, op_num
        )
    )

    print("heartbeat request - len:", req.hex(), len(req))

    # send request
    client.send(req)
    data = client.recv(1_000)
    return data


def test_session_timeout(started_cluster):
    wait_nodes()

    client1 = handshake(node1.name)

    client2 = handshake(node2.name)

    client3 = handshake(node3.name)

    p = Pool(3)
    p.map(heartbeat, [client1, client2, client3])

    time.sleep(9)
    p.map(heartbeat, [client2, client3])

    time.sleep(4)
    assert len(heartbeat(client1)) == 0
    assert len(heartbeat(client2)) > 0
    assert len(heartbeat(client3)) > 0
