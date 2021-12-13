import pytest
from helpers.cluster import ClickHouseCluster
from helpers.cluster_service import ClickHouseServiceCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager


from kazoo.client import KazooClient, KazooState

cluster1 = ClickHouseServiceCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml', 'configs/log_conf.xml'], stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml', 'configs/log_conf.xml'], stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml', 'configs/log_conf.xml'], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()

        yield cluster1

    finally:
        cluster1.shutdown()

def smaller_exception(ex):
    return '\n'.join(str(ex).split('\n')[0:2])

def wait_node(cluster1, node):
    for _ in range(100):
        zk = None
        try:
            # node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(cluster1, node.name, timeout=30.0)
            zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            if zk:
                zk.stop()
                zk.close()
    else:
        raise Exception("Can't wait node", node.name, "to become ready")

def wait_nodes(cluster1, node1, node2, node3):
    for node in [node1, node2, node3]:
        wait_node(cluster1, node)


def get_fake_zk(cluster1, nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster1.get_instance_ip(nodename) + ":5102", timeout=timeout)
    def reset_listener(state):
        nonlocal _fake_zk_instance
        print("Fake zk callback called for state", state)
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance


def test_cluster_replicated_table(started_cluster):
    cluster3 = ClickHouseCluster(__file__)
    node4 = cluster3.add_instance('node4', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros4.xml'], with_zookeeper=False, stay_alive=True)
    node5 = cluster3.add_instance('node5', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros5.xml'], with_zookeeper=False, stay_alive=True)
    node6 = cluster3.add_instance('node6', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros6.xml'], with_zookeeper=False, stay_alive=True)

    node7 = cluster3.add_instance('node7', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros7.xml'], with_zookeeper=False, stay_alive=True)
    node8 = cluster3.add_instance('node8', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros8.xml'], with_zookeeper=False, stay_alive=True)
    node9 = cluster3.add_instance('node9', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros9.xml'], with_zookeeper=False, stay_alive=True)

    node10 = cluster3.add_instance('node10', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros10.xml'], with_zookeeper=False, stay_alive=True)
    node11 = cluster3.add_instance('node11', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros11.xml'], with_zookeeper=False, stay_alive=True)
    node12 = cluster3.add_instance('node12', main_configs=['configs/log_conf.xml', 'configs/use_test_keeper.xml', 'configs/cluster.xml', 'configs/macros12.xml'], with_zookeeper=False, stay_alive=True)

    try:
        wait_nodes(cluster1, node1, node2, node3)
        cluster3.start(destroy_dirs=False)

        node1_zk = get_fake_zk(cluster1, "node1")
        node1_zk.get("/")

        node4.query("DROP DATABASE IF EXISTS tmp_smoke_test ON CLUSTER test_ch_service_cluster")
        node4.query("CREATE DATABASE IF NOT EXISTS tmp_smoke_test ON CLUSTER test_ch_service_cluster")
        node4.query("CREATE TABLE IF NOT EXISTS tmp_smoke_test.alerts_local01 on CLUSTER test_ch_service_cluster ( tenant_id UInt32, alert_id String, timestamp1 DateTime Codec(Delta, LZ4), alert_data String, acked UInt8, ack_time DateTime,  ack_user LowCardinality(String)   )  ENGINE = ReplicatedMergeTree('/clickhouse/tables/tmp_smoke_test/alerts_local01/shard-{shard}', '{replica}')  PARTITION BY toYYYYMM(timestamp1)  ORDER BY (tenant_id, timestamp1, alert_id)   SETTINGS index_granularity = 8192")
        node4.query("CREATE TABLE IF NOT EXISTS tmp_smoke_test.alerts ON CLUSTER test_ch_service_cluster AS tmp_smoke_test.alerts_local01 ENGINE = Distributed('test_ch_service_cluster', 'tmp_smoke_test', 'alerts_local01', rand())")

        node4.query("INSERT INTO tmp_smoke_test.alerts (tenant_id, alert_id, timestamp1, alert_data)  SELECT   toUInt32(rand(1)%1000+1) AS tenant_id,   randomPrintableASCII(64) as alert_id,  toDateTime('2020-01-01 00:00:00') + rand(2)%(3600*24*30)*12 as timestamp1,   randomPrintableASCII(1024) as alert_data FROM numbers(10000)")
        node4.query("INSERT INTO tmp_smoke_test.alerts (tenant_id, alert_id, timestamp1, alert_data)  SELECT   toUInt32(rand(1)%1000+1) AS tenant_id,   randomPrintableASCII(64) as alert_id,  toDateTime('2020-01-01 00:00:00') + rand(2)%(3600*24*30)*12 as timestamp1,   randomPrintableASCII(1024) as alert_data FROM numbers(10000)")
        node4.query("INSERT INTO tmp_smoke_test.alerts (tenant_id, alert_id, timestamp1, alert_data)  SELECT   toUInt32(rand(1)%1000+1) AS tenant_id,   randomPrintableASCII(64) as alert_id,  toDateTime('2020-01-01 00:00:00') + rand(2)%(3600*24*30)*12 as timestamp1,   randomPrintableASCII(1024) as alert_data FROM numbers(10000)")
        node4.query("INSERT INTO tmp_smoke_test.alerts (tenant_id, alert_id, timestamp1, alert_data)  SELECT   toUInt32(rand(1)%1000+1) AS tenant_id,   randomPrintableASCII(64) as alert_id,  toDateTime('2020-01-01 00:00:00') + rand(2)%(3600*24*30)*12 as timestamp1,   randomPrintableASCII(1024) as alert_data FROM numbers(10000)")

        node4.query("SYSTEM FLUSH DISTRIBUTED tmp_smoke_test.alerts")
        time.sleep(2)

        node4.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node5.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node6.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node7.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node8.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node9.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node10.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node11.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)
        node12.query("SYSTEM SYNC REPLICA tmp_smoke_test.alerts_local01", timeout=60)

        num4 = int(node4.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num5 = int(node5.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num6 = int(node6.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num7 = int(node7.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num8 = int(node8.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num9 = int(node9.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num10 = int(node10.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num11 = int(node11.query("SELECT count() from tmp_smoke_test.alerts_local01"))
        num12 = int(node12.query("SELECT count() from tmp_smoke_test.alerts_local01"))

        print("num4", num4)
        print("num5", num5)
        print("num6", num6)
        print("num7", num7)
        print("num8", num8)
        print("num9", num9)
        print("num10", num10)
        print("num11", num11)
        print("num12", num12)

        num_sum = num4 + num7 + num10

        assert str(num_sum) + '\n' == node12.query("SELECT count() from tmp_smoke_test.alerts")
        assert str(num_sum) + '\n' == "40000\n"
        assert num5 == num4
        assert num6 == num4
        assert num8 == num7
        assert num9 == num7
        assert num11 == num10
        assert num12 == num10

        node4.query("DROP DATABASE IF EXISTS tmp_smoke_test ON CLUSTER test_ch_service_cluster SYNC")

    finally:
        cluster3.shutdown()