# RaftKeeper Benchmarks

You can use the Benchmark tool that comes with RaftKeeper to benchmark RaftKeeper performance.


## Environment

```
Server: Docker - 16 cores, 32GB memory, 50GB NVMe disk
System: CentOS 7.9
Version:  RaftKeeper 2.0.0, ZooKeeper 3.7.1
Config: RaftKeeper log level warning, ZooKeeper log level warn, others is default.
Test Data: every item is 100 bytes.
```

## 1. Write request benchmark (Create-100%) 
![benchmark-create-tps.png](images%2Fbenchmark-create-tps.png)

![benchmark-create-avgrt.png](images%2Fbenchmark-create-avgrt.png)

![benchmark-create-tp99.png](images%2Fbenchmark-create-tp99.png)

## 2. Mixed request benchmark (create-10% set-40% get-40% delete-10%）
![benchmark-mixed-tps.png](images%2Fbenchmark-mixed-tps.png)

![benchmark-mixed-avgrt.png](images%2Fbenchmark-mixed-avgrt.png)

![benchmark-mixed-tp99.png](images%2Fbenchmark-mixed-tp99.png)