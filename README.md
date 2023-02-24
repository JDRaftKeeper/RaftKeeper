# RaftKeeper

RaftKeeper is a high-performance distributed consensus service. 
It is fully compatible with Zookeeper and can be accessed through the Zookeeper 
client. It implements most of the functions of Zookeeper (except: Container node, 
TTL node, quota etc.) and provides some additional functions, such as more 
monitoring indicators, manual Leader switching and so on. 

RaftKeeper provides a multi-thread processor for performance consideration. 
But also it provides below guarantee:
1. Requests response must be returned in order in one session.
2. All committed write requests must be handled in order, across all sessions.

RaftKeeper data resides in memory and provides snapshot + operation log data 
persistence capabilities. The execution framework adopts pipeline and batch 
execution methods to greatly improve system throughput, see [Benchmark](benchmark%2FBenchmark.md) for details.

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

# How to migrate from Zookeeper?

RaftKeeper provides tool to translate Zookeeper data to RaftKeeper format. So you can 
simply move data into RaftKeeper, detail is in [migrate-from-zookeeper](docs%2Fmigrate-from-zookeeper.md).
