# Four-letter word command

The four-letter word command, abbreviated as the 4lw command, is currently the sole method for monitoring and managing RaftKeeper.
It is based on the Zookeeper v3.5 4lw command, and we have made some extensions on this basis to better facilitate monitoring. 
Additionally, we provide some basic management commands.

The 4lw commands has a white list configuration four_letter_word_white_list which has default value 
`conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,lgif,rqld,uptm,csnp`.

You can send the commands to ClickHouse Keeper `nc`.

```
echo mntr | nc localhost 8101
```

Bellow is the detailed 4lw commands:

### For monitoring

#### ruok
Tests if server is running in a non-error state. The server will respond with `imok` if it is running. Otherwise, 
it will not respond at all. A response of `imok` does not necessarily indicate that the server has joined the quorum, 
just that the server process is active and bound to the specified client port. Use "stat" for details on state with 
respect to quorum and client connection information.
```
imok
```
#### uptm
Process uptime in seconds.
```
1654332
```

#### mntr
`mntr` is the provide rich metrics and is the main monitoring command. The following is the output

```
zk_version	RaftKeeper v2.0.4-e80e94979318adc94e81664053d782d0c9d7e60f, built on 2024-01-11 10:59:45 CST
zk_compatible_mode	zookeeper
zk_avg_latency	1
zk_max_latency	14598
zk_min_latency	0
zk_packets_received	59695194
zk_packets_sent	60406965
zk_num_alive_connections	0
zk_outstanding_requests	0
zk_server_state	leader
zk_znode_count	2899570
zk_watch_count	0
zk_ephemerals_count	41933
zk_approximate_data_size	1179161410
zk_snap_count	619
zk_snap_time_ms	9058483
zk_in_snapshot	0
zk_open_file_descriptor_count	126
zk_max_file_descriptor_count	60480000
zk_followers	2
zk_synced_followers	2
```

#### srvr
Lists full details for the server.

```
RaftKeeper version: RaftKeeper v2.0.4
Latency min/avg/max: 0/1/14598
Received: 59695194
Sent : 60406965
Connections: 0
Outstanding: 0
Zxid: 4960740637
Mode: leader
Node count: 2911227
```

#### stat
Lists brief details for the server and connected clients.

```
RaftKeeper version: RaftKeeper v2.0.4
Clients:
 10.203.46.164:64710(recved=0,sent=0)

Latency min/avg/max: 0/1/14598
Received: 59695194
Sent : 60406965
Connections: 0
Outstanding: 0
Zxid: 4960811549
Mode: leader
Node count: 2926715
```

#### srst
Reset server statistics. The command will affect the result of srvr, mntr and stat.

```
Server stats reset.
```

#### conf
Print details about serving configuration.
```
my_id=1
port=2181
host=lf06-ch-000335-raftkeeper-0-0
internal_port=8103
thread_count=16
snapshot_create_interval=3600
four_letter_word_white_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,lgif,rqld,uptm,csnp
log_dir=/data/jdolap/raft_service/raft_log
snapshot_dir=/data/jdolap/raft_service/raft_snapshot
max_session_timeout_ms=100000
min_session_timeout_ms=10000
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=10000
election_timeout_upper_bound_ms=20000
reserved_log_items=1000000
snapshot_distance=3000000
max_stored_snapshots=5
shutdown_timeout=5000
startup_timeout=6000000
raft_logs_level=information
rotate_log_storage_interval=100000
log_fsync_mode=fsync_parallel
log_fsync_interval=1000
nuraft_thread_size=16
fresh_log_gap=200
```

#### cons
List full connection/session details for all clients connected to this server. Includes information on numbers of packets 
received/sent, session id, operation latencies, last operation performed, etc...
```
 10.203.46.164:57828(recved=0,sent=0)
 11.118.155.104:52268(recved=94390633,sent=98101089,sid=0x33b,lop=List,est=1706212559968,to=30000,lcxid=0x59fae04,lzxid=0x1579e9c7e,lresp=1706610369786,llat=0,minlat=0,avglat=3,maxlat=18446744073709551615)
 11.115.128.210:51460(recved=89281242,sent=92735673,sid=0x33a,lop=List,est=1706212559943,to=30000,lcxid=0x551b775,lzxid=0x1579e9c32,lresp=1706610369723,llat=0,minlat=0,avglat=3,maxlat=18446744073709551615)
```

#### crst
Reset connection/session statistics for all connections.
```
Connection stats reset.
```

#### dirs
Shows the total size of snapshot and log files in bytes
```
snapshot_dir_size: 4189514301
log_dir_size: 1525068003
```

#### isro
Tests if server is running in read-only mode. The server will respond with ro if in read-only mode or rw if not in read-only mode.
```
rw
```

#### wchs
Lists brief information on watches for the server.
```
9 connections watching 32269 paths
Total watches:102157
```

#### wchc
Lists detailed information on watches for the server, by session. This outputs a list of sessions (connections) with associated watches (paths). Note, depending on the number of watches this operation may be expensive (impact server performance), use it carefully.

```
0x0000000000000001
    /clickhouse/task_queue/ddl
```

#### wchp
Lists detailed information on watches for the server, by path. This outputs a list of paths (znodes) with associated sessions. Note, depending on the number of watches this operation may be expensive (i.e., impact server performance), use it carefully.
```
/clickhouse/task_queue/ddl
    0x0000000000000001
```

#### dump
Lists the outstanding sessions and ephemeral nodes. This only works on the leader.
```
Sessions dump (2):
0x0000000000000001
0x0000000000000002
Sessions with Ephemerals (1):
0x0000000000000001
 /clickhouse/task_queue/ddl
```

#### lgif
Keeper log information. `first_log_idx` : my first log index in log store; 
`first_log_term` : my first log term; 
`last_log_idx` : my last log index in log store; 
`last_log_term` : my last log term; 
`last_committed_log_idx` : my last committed log index in state machine; 
`leader_committed_log_idx` : leader's committed log index from my perspective; 
`target_committed_log_idx` : target log index should be committed to; 
`last_snapshot_idx` : the largest committed log index in last snapshot.

```
first_log_idx	3747253442
first_log_term	5
last_log_idx	3751629198
last_log_term	5
last_committed_log_idx	3751629198
leader_committed_log_idx	3751629198
target_committed_log_idx	3751629198
last_snapshot_idx	3749065412
```


### For management

RaftKeeper also provides some useful command to manage system.

#### rqld
Request to become new leader. Return `Sent leadership request to leader.` if request sent 
or `Failed to send leadership request to leader.` if request not sent. Note that if node is 
already leader the outcome is same as the request is sent.

```
Sent leadership request to leader.
```

#### csnp
Schedule a snapshot creation task. Return the last committed log index of the scheduled snapshot if success 
or `Failed to schedule snapshot creation task.` if failed. Note that `lgif` command can help you determine 
whether the snapshot is done.

```
3751957612
```