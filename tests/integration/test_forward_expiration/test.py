import time

import pytest

from datetime import datetime
from kazoo.exceptions import OperationTimeoutError
from helpers.cluster_service import RaftKeeperCluster
from helpers.network import PartitionManager
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml', 'configs/log_conf.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()
        yield cluster1
    finally:
        cluster1.shutdown()


def test_forward_expiration(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk(session_timeout=15)
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.sync("/")  # Make node1 and node3 establish a forward connection

        with PartitionManager() as pm:
            pm.partition_instances(node3, node2)
            pm.partition_instances(node3, node1)
            time.sleep(3)
            begin = datetime.now()
            with pytest.raises(OperationTimeoutError):
                node1_zk.sync("/")
            end = datetime.now()
            # (read_timeout = negotiated_session_timeout * 2.0 / 3.0), this is operation timeout
            # server operation_timeout_ms is 1s. Expect to return within 2 seconds. client operation_timeout_ms default 10s
            assert (end - begin).seconds < 2

        node1_zk = node1.get_fake_zk()
        node1_zk.create("/test_forward_expiration", b"somedata")

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])
