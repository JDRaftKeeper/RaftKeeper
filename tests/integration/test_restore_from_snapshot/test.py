#!/usr/bin/env python3
import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_keeper1.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_keeper2.xml'],
                              stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_keeper3.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()
        yield cluster1
    finally:
        cluster1.shutdown()


def test_recover_from_snapshot(started_cluster):
    node1_zk = node2_zk = node3_zk = None
    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.create("/test_recover_from_snapshot", "somedata".encode())

        node2_zk.sync("/test_recover_from_snapshot")
        node3_zk.sync("/test_recover_from_snapshot")

        assert node1_zk.get("/test_recover_from_snapshot")[0] == b"somedata"
        assert node2_zk.get("/test_recover_from_snapshot")[0] == b"somedata"
        assert node3_zk.get("/test_recover_from_snapshot")[0] == b"somedata"

        node3.stop_raftkeeper()
        # node3 maybe leader we should wait new leader
        node1.wait_for_join_cluster()

        # at least we will have 2 snapshots
        for i in range(435):
            node1_zk.create("/test_recover_from_snapshot" + str(i), ("somedata" + str(i)).encode())

        for i in range(435):
            if i % 10 == 0:
                node1_zk.delete("/test_recover_from_snapshot" + str(i))

    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])

    # stale node should recover from leader's snapshot
    # with some sanitizers can start longer than 5 seconds
    node3.start_raftkeeper(start_wait=True)
    print("Restarted")

    try:
        node1_zk = node1.get_fake_zk()
        node2_zk = node2.get_fake_zk()
        node3_zk = node3.get_fake_zk()

        node1_zk.sync("/test_recover_from_snapshot")
        node2_zk.sync("/test_recover_from_snapshot")
        node3_zk.sync("/test_recover_from_snapshot")

        assert node1_zk.get("/test_recover_from_snapshot")[0] == b"somedata"
        assert node2_zk.get("/test_recover_from_snapshot")[0] == b"somedata"
        assert node3_zk.get("/test_recover_from_snapshot")[0] == b"somedata"

        for i in range(435):
            if i % 10 != 0:
                assert node1_zk.get("/test_recover_from_snapshot" + str(i))[0] == ("somedata" + str(i)).encode()
                assert node2_zk.get("/test_recover_from_snapshot" + str(i))[0] == ("somedata" + str(i)).encode()
                assert node3_zk.get("/test_recover_from_snapshot" + str(i))[0] == ("somedata" + str(i)).encode()
            else:
                assert node1_zk.exists("/test_recover_from_snapshot" + str(i)) is None
                assert node2_zk.exists("/test_recover_from_snapshot" + str(i)) is None
                assert node3_zk.exists("/test_recover_from_snapshot" + str(i)) is None
    finally:
        close_zk_clients([node1_zk, node2_zk, node3_zk])
