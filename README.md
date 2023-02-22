# RaftKeeper

RaftKeeper is a high-performance distributed consensus service. 
It is fully compatible with Zookeeper and can be accessed through the Zookeeper 
client. It implements most of the functions of Zookeeper (except: Container node, 
TTL node, quota, reconfig) and provides some additional functions, such as more 
monitoring indicators, manual Leader switching and so on. 

RaftKeeper provides a multi-thread processor for performance consideration. 
But also it provides below guarantee:
1. Requests response must be returned in order in one session.
2. All committed write requests must be handled in order, across all sessions.

RaftKeeper data resides in memory and provides snapshot + operation log data 
persistence capabilities. The execution framework adopts pipeline and batch 
execution methods to greatly improve system throughput, see benchmark for details.

RaftKeeper is derived from [ClickHouse](https://github.com/ClickHouse/ClickHouse) 
and take [NuRaft](https://github.com/eBay/NuRaft) as Raft implementation. 
We really appreciate the excellent work of the ClickHouse and NuRaft team.


# How to start?

## 1. Build RaftKeeper

### Build on macOS

Requirement: macOS 10+, Clang 13+, Cmake 3.3+

```
# install tools
brew install cmake llvm@13
 
# clone project
git clone git@xxx/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=/usr/local/opt/llvm@13/bin/clang CXX=/usr/local/opt/llvm@13/bin/clang++
cd RaftKeeper && sh bin/build.sh
```

### Build on Ubuntu

Requirement: Ubuntu20.04+, Clang 13+, Cmake 3.3+
```
# install tools
sudo apt-get install cmake llvm-13
 
# clone project
git clone git@xxx/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=/usr/bin/clang-13 CXX=/usr/bin/clang++-13
cd RaftKeeper/bin && bash start.sh
```

## 2. Deploy RaftKeeper

Deploy a three nodes cluster.
```
# download RaftKeeper
wget xxx 
tar -xzvf RaftKeeper.tar.gz
 
# configure it: replace my_id under <my_id> and id & host under <cluster>. Pls note that three nodes must has different id.
vim RaftKeeper/conf/config.xml
 
# start it
cd RaftKeeper/bin && bash start.sh
```


## 3. Access RaftKeeper

You can use ZooKeeper's shell client [zkCli.sh](https://zookeeper.apache.org/doc/r3.6.0/zookeeperCLI.html) 
to access to RaftKeeper, or you can use Java, python or C ZooKeeper clients to access. 
Also, RaftKeeper supports Zookeeper's [4lw command](https://zookeeper.apache.org/doc/r3.6.0/zookeeperAdmin.html#sc_zkCommands).

# How to migrate form Zookeeper?

RaftKeeper provides tool to translate Zookeeper data to RaftKeeper format. So you can 
simply move data into RaftKeeper.

1.Find Zookeeper leader node.
```
[zk1]$ echo srvr | nc zk1/zk2/zk3 zk_port | grep leader
```

2.Stop Zookeeper cluster, make sure that leader node is the last.
```
[zk1]$ cd /path/to/ZooKeeper && zkServer.sh stop
```

3.Restart Zookeeper leader node, this will generate a snapshot.
```
[zk1]$ cd /path/to/ZooKeeper && zkServer.sh start
[zk1]$ zkServer.sh stop
```
4.Copy Zookeeper leader node data to one of RaftKeeper node.
```
[zk1]$ scp log/version-2/* root@raft_node1:/path_to_transfer_tmp_data_dir/log
[zk1]$ scp data/version-2/* root@raft_node1:/path_to_transfer_tmp_data_dir/snapshot
```

5.Translate Zookeeper data to RaftKeeper format and copy to other nodes.
```
[raft_node1]$ /path/to/RaftKeeper/raftkeeper converter --zookeeper-logs-dir /path_to_transfer_tmp_data_dir/log --zookeeper-snapshots-dir /path_to_transfer_tmp_data_dir/snapshot --output-dir /path_to_transfer_tmp_data_dir/output
[raft_node1]$ scp -r /path_to_transfer_tmp_data_dir/output root@raft_node2:/path_to_transfer_tmp_data_dir
[raft_node1]$ scp -r /path_to_transfer_tmp_data_dir/output root@raft_node3:/path_to_transfer_tmp_data_dir
```

6.Copy data to RaftKeeper data directory.
```
[raft_node1]$ cd /path/to/RaftKeeper && sh bin/stop.sh
[raft_node1]$ rm -rf data/* && mv /path_to_transfer_tmp_data_dir/output data/snapshot
[raft_node1]$ sh bin/start.sh
```
