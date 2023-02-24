# RaftKeeper Benchmark

Zookeeper or RaftKeeper benchmark tool.

```
bin/benchmark.sh target nodes thread_size payload_size run_duration(second) only_create

Arguments:

target: test target - 'zookeeper', 'raftkeeper', 'etcd'
nodes: target nodes
thread_size: thread size, every thread will use a separated zookeeper client.
payload_size: data item size in byte
run_duration: test will run x seconds
only_create: whether only send create command, if not, send mixed request (create-10% set-40% get-40% delete-10%)

# For example : bin/benchmark.sh "localhost:2181" 10 100 20 true
```

# Session Consistency Test

Test read write consistency in one session.

```
bin/session_consistency.sh nodes thread_size

Arguments: 

nodes: target nodes
thread_size: thread size, all thread use a same zookeeper client.

# For example : bin/session_consistency.sh "localhost:2181" 10
```

