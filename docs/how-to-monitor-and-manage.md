# Four-letter word command

The four-letter word command, abbreviated as the 4lw command, is currently the sole method for monitoring and managing RaftKeeper.
It is based on the Zookeeper v3.5 4lw command, and we have made some extensions on this basis to better facilitate monitoring. 
Additionally, we provide some basic management commands.

The 4lw commands has a white list configuration `four_letter_word_white_list` which has default value 
`conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,lgif,rqld,uptm,csnp`. If you want to 
enable more command, just add to it, or use `*`. 

You can send the commands to ClickHouse Keeper by `nc`.

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
`mntr` provide rich metrics and is the main monitoring command. The following is the output

```
zk_version	RaftKeeper v2.0.5-96935adae174aaa430db0af14f823b0bf892f38e, built on 2024-04-15 17:19:43 CST
zk_compatible_mode	zookeeper
zk_avg_latency	1
zk_max_latency	546
zk_min_latency	0
zk_packets_received	25596328
zk_packets_sent	25595940
zk_num_alive_connections	0
zk_outstanding_requests	0
zk_server_state	leader
zk_znode_count	2
zk_watch_count	0
zk_ephemerals_count	0
zk_approximate_data_size	3757
zk_snap_count	2
zk_snap_time_ms	1039
zk_snap_blocking_time_ms 20
zk_in_snapshot	0
zk_open_file_descriptor_count	126
zk_max_file_descriptor_count	60480000
zk_followers	2
zk_synced_followers	2
zk_p50_apply_read_request_time_ms	0.0
zk_p90_apply_read_request_time_ms	0.0
zk_p99_apply_read_request_time_ms	0.0
zk_p999_apply_read_request_time_ms	0.0
zk_cnt_apply_read_request_time_ms	151225987
zk_sum_apply_read_request_time_ms	19
zk_p50_apply_write_request_time_ms	0.0
zk_p90_apply_write_request_time_ms	0.0
zk_p99_apply_write_request_time_ms	0.0
zk_p999_apply_write_request_time_ms	0.0
zk_cnt_apply_write_request_time_ms	151225987
zk_sum_apply_write_request_time_ms	856
zk_avg_log_replication_batch_size	119.0
zk_min_log_replication_batch_size	1.0
zk_max_log_replication_batch_size	200
zk_cnt_log_replication_batch_size	383096
zk_sum_log_replication_batch_size	45654000
zk_p50_push_request_queue_time_ms	0.0
zk_p90_push_request_queue_time_ms	0.0
zk_p99_push_request_queue_time_ms	0.0
zk_p999_push_request_queue_time_ms	0.0
zk_cnt_push_request_queue_time_ms	25595940
zk_sum_push_request_queue_time_ms	76
zk_p50_readlatency	0.0
zk_p90_readlatency	1.0
zk_p99_readlatency	1.0
zk_p999_readlatency	1.0
zk_cnt_readlatency	10237988
zk_sum_readlatency	1992231
zk_p50_updatelatency	3.0
zk_p90_updatelatency	3.0
zk_p99_updatelatency	4.0
zk_p999_updatelatency	5.9
zk_cnt_updatelatency	15357952
zk_sum_updatelatency	39688173
```
The explanation of the metrics is as follows:
```
zk_version: RaftKeeper version including binary built time
zk_compatible_mode:	the binary compatible mode, the valid values is 'zookeeper' and 'clickhouse', ClickHouse is somewhat incompatible with Zookeeper, so we provide two binary packages.
zk_avg_latency: average latency in the whole process live time
zk_max_latency: max latency in the whole process live time
zk_min_latency: min latency in the whole process live time
zk_packets_received: packet received count in the whole process live time, you can simply think of it as the number of requests 
zk_packets_sent: packet sent count in the whole process live time, you can simply think of it as the number of requests
zk_num_alive_connections: active connections right now
zk_outstanding_requests: requests count in waiting queue, if it is large means that the process is under presure
zk_server_state: server role, leader for multi-node cluster and role is leader, follower for multi-node cluster and role is follower, observer for node who dees not participate in leader election and log replication, standalone for 1 node cluster.
zk_znode_count: znode count
zk_watch_count: watch
zk_ephemerals_count	41933
zk_approximate_data_size: approximate data size in byte
zk_snap_count: the number of snapshots created in the whole process live time
zk_snap_time_ms: The time spent creating snapshots in the whole process live time
zk_snap_blocking_time_ms: Blocking user request time when creating snapshots
zk_in_snapshot: whether process is creating snapshot right now
zk_open_file_descriptor_count: current opening fd count
zk_max_file_descriptor_count: max opening fd count
zk_followers: follower count, only present on the leader
zk_synced_followers: synced follower count, only present on the leader
zk_apply_read_request_time_ms: The time only for request processor to process read requests
zk_apply_write_request_time_ms: The time only for request processor to process write requests, replication is not included for write requests
zk_log_replication_batch_size: Records the batch size of each batch accumulation for replication
zk_push_request_queue_time_ms: The time for push request from handler to dispatcher's request queue
zk_readlatency: Latency for read request. The time start from when the server see the request until it leave final request processor
zk_updatelatency: Latency for write request. The time start from when the server see the request until it leave final request processor
```
Please note that the metrics `zk_followers` and `zk_synced_followers` are only present on the leader.

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
parallel=16
snapshot_create_interval=3600
four_letter_word_white_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,lgif,rqld,uptm,csnp
log_dir=/data/jdolap/raft_service/raft_log
snapshot_dir=/data/jdolap/raft_service/raft_snapshot
max_session_timeout_ms=3600000
min_session_timeout_ms=1000
operation_timeout_ms=3000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
client_req_timeout_ms=3000
election_timeout_lower_bound_ms=3000
election_timeout_upper_bound_ms=5000
reserved_log_items=1000000
snapshot_distance=3000000
max_stored_snapshots=5
shutdown_timeout=5000
startup_timeout=6000000
raft_logs_level=information
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
#### envi
Basic system information.
```
Environment:
raftkeeper.version=RaftKeeper v2.0.4
host.name=lf06-ch-000335-raftkeeper-0-1
os.name=Linux
os.arch=x86_64
os.version=4.18.0-193.el8.jd_017.x86_64
cpu.count=96
user.name=
user.home=/root/
user.dir=/software/servers/
user.tmp=/tmp/
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

RaftKeeper also provides some useful commands to manage system.

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

#### jmst

Dump Jemalloc statistics. Note that this only works when you Jemalloc is enabled.

Jemalloc is enabled when:
1. RaftKeeper is not built with sanitizers
2. and built with ENABLE_JEMALLOC=ON (default is ON)
3. and your OS is linux or freebsd

```
___ Begin jemalloc statistics ___
Version: "5.2.1-0-gea6b3e973b477b8061e0076bb257dbd7f3faa756"
Build-time option settings
  config.cache_oblivious: true
  config.debug: true
  config.fill: true
  config.lazy_lock: false
  config.malloc_conf: "percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:10000"
  config.opt_safety_checks: true
  config.prof: true
  config.prof_libgcc: false
  config.prof_libunwind: true
  config.stats: true
  config.utrace: false
  config.xmalloc: false
Run-time option settings
  opt.abort: true
  opt.abort_conf: true
  opt.confirm_conf: false
  opt.retain: true
  opt.dss: "secondary"
  opt.narenas: 96
  opt.percpu_arena: "percpu"
  opt.oversize_threshold: 0
  opt.metadata_thp: "disabled"
  opt.background_thread: false (background_thread: true)
  opt.dirty_decay_ms: 10000 (arenas.dirty_decay_ms: 10000)
  opt.muzzy_decay_ms: 10000 (arenas.muzzy_decay_ms: 10000)
  opt.lg_extent_max_active_fit: 6
  opt.junk: "true"
  opt.zero: false
  opt.tcache: true
  opt.lg_tcache_max: 15
  opt.thp: "default"
  opt.prof: false
  opt.prof_prefix: "jeprof"
  opt.prof_active: true (prof.active: false)
  opt.prof_thread_active_init: true (prof.thread_active_init: false)
  opt.lg_prof_sample: 19 (prof.lg_sample: 0)
  opt.prof_accum: false
  opt.lg_prof_interval: -1
  opt.prof_gdump: false
  opt.prof_final: false
  opt.prof_leak: false
  opt.stats_print: false
  opt.stats_print_opts: ""
Profiling settings
  prof.thread_active_init: false
  prof.active: false
  prof.gdump: false
  prof.interval: 0
  prof.lg_sample: 0
Arenas: 96
Quantum size: 16
Page size: 4096
Maximum thread-cached size class: 32768
Number of bin size classes: 36
Number of thread-cache bin size classes: 41
Number of large size classes: 196
Allocated: 15939680, active: 17014784, metadata: 11315328 (n_thp 0), resident: 35680256, mapped: 79454208, retained: 49520640
Background threads: 4, num_runs: 8, run_interval: 2502654625 ns
                           n_lock_ops (#/sec)       n_waiting (#/sec)      n_spin_acq (#/sec)  n_owner_switch (#/sec)   total_wait_ns   (#/sec)     max_wait_ns  max_n_thds
background_thread                  25       2               0       0               0       0              20       1               0         0               0           0
ctl                                 3       0               0       0               0       0               2       0               0         0               0           0
prof                                0       0               0       0               0       0               0       0               0         0               0           0
Merged arenas stats:
assigned threads: 21
uptime: 12720046886
dss allocation precedence: "N/A"
decaying:  time       npages       sweeps     madvises       purged
   dirty:   N/A           11            2            3         1921
   muzzy:   N/A         1921            0            0            0
                            allocated         nmalloc (#/sec)         ndalloc (#/sec)       nrequests   (#/sec)           nfill   (#/sec)          nflush   (#/sec)
small:                        4065376           20734    1727             328      27            7078       589             280        23               8         0
large:                       11874304              50       4              17       1              50         4              50         4               0         0
total:                       15939680           20784    1732             345      28            7128       594             330        27               8         0

active:                      17014784
mapped:                      79454208
retained:                    49520640
base:                        10697088
internal:                      618240
metadata_thp:                       0
tcache_bytes:                 2450904
resident:                    35680256
abandoned_vm:                       0
extent_avail:                       1
                           n_lock_ops (#/sec)       n_waiting (#/sec)      n_spin_acq (#/sec)  n_owner_switch (#/sec)   total_wait_ns   (#/sec)     max_wait_ns  max_n_thds

...

--- End jemalloc statistics ---
```

#### jmpg

Purge unused Jemalloc memory. This will release some memory back to the system.

```
ok
```

#### jmep

You can analyze the memory usage by Jemalloc with the following steps:
1. `jmep`: to start the memory analysis.
2. `jmfp`: to flush the profiling to a tmp file, you can execute it multiple times to get multiple profiling and then diff it.
3. `jmdp`: to stop the memory analysis.

Please note that before you start Jemalloc profiling, you should make sure that the RaftKeeper is started with env variable `MALLOC_CONF=background_thread:true,prof:true`,
else you will get the following error:
```
RaftKeeper was started without enabling profiling for jemalloc. To use jemalloc's profiler, following env variable should be set: MALLOC_CONF=background_thread:true,prof:true
```

Command `jmep` will enable Jemalloc profiling.
```
ok
```

#### jmfp

Flush the profiling to a tmp file and return the file name. The file name patten is `/tmp/jemalloc_raftkeeper.{pid}.{id}.heap`.

```
/tmp/jemalloc_raftkeeper.3737249.1.heap
```

#### jmdp

Please do not forget to disable Jemalloc profiling when you complete the memory analysis to avoid performance regression.

```
ok
```
