import pytest
import subprocess
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

def test_performance(started_cluster):

    wait_nodes(cluster1, node1, node2, node3)
    #
    res = subprocess.run("pwd", env=None, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))
    # if res.returncode != 0:
    #     # check_call(...) from subprocess does not print stderr, so we do it manually
    #     raise Exception('Command {} return non-zero code {}: {}'.format(args, res.returncode, res.stderr.decode('utf-8')))
    #
    # res = subprocess.run("cd /ClickHouse/benchmark/raft-benchmark", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #
    # res = subprocess.run(['which', 'mvn'], env=None, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    # print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))
    #
    # args = ['/usr/local/bin/mvn', '-version']
    # cmd = ['git', 'status']
    # my_env = os.environ.copy()
    #
    # my_env["PATH"] = "/usr/local/bin:" + my_env["PATH"]
    # print(my_env)
    # p = subprocess.run("mvn -v", env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print('命令: ', p.args)
    # print('退出码: ', p.returncode)
    # print('标准输出: ', p.stdout)
    # print('标准错误: ', p.stderr)

    #- cd ./benchmark/raft-benchmark
    #- mvn clean compile package
    #- cd target
    #- unzip raft-benchmark-1.0-bin.zip
    res = subprocess.run("ls /ClickHouse", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    res = subprocess.run("cd /ClickHouse/benchmark/raft-benchmark", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    # my_env = os.environ.copy()
    # my_env["PATH"] = "/usr/local/bin/apache-maven-3.3.9/bin:" + my_env["PATH"]
    # res = subprocess.call("cd /ClickHouse/benchmark/raft-benchmark && mvn clean compile package", timeout=500, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    # print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    res = subprocess.run("cd /ClickHouse/benchmark/raft-benchmark/target && unzip -o raft-benchmark-1.0-bin.zip", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    res = subprocess.run("cd /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    res = subprocess.run("cd /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    res = subprocess.run("ls /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    host1 = cluster1.get_instance_ip('node1')
    res = subprocess.run("sed -i 's/NODE1/" + host1 + "/g' /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    host2 = cluster1.get_instance_ip('node2')
    res = subprocess.run("sed -i 's/NODE2/" + host2 + "/g' /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    host3 = cluster1.get_instance_ip('node3')
    res = subprocess.run("sed -i 's/NODE3/" + host3 + "/g' /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    # zk = get_fake_zk(cluster1, 'node1', timeout=30.0)
    # zk.create("/test12", b"hello")
    # print(zk.get("/test12"))

    res = subprocess.run("/bin/bash /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", timeout=500, env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Stderr:\n{}\n'.format(res.stderr.decode('utf-8')))
    print('Stdout:\n{}\n'.format(res.stdout.decode('utf-8')))

    res = subprocess.run("sed -i 's/" + host1 + "/NODE1/g' /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    res = subprocess.run("sed -i 's/" + host2 + "/NODE2/g' /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    res = subprocess.run("sed -i 's/" + host3 + "/NODE3/g' /ClickHouse/benchmark/raft-benchmark/target/raft-benchmark-1.0/bin/test.sh", env=None, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
