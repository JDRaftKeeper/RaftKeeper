# Migrate from Zookeeper

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
