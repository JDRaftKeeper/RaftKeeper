**RaftKeeper** is a high-performance distributed consensus service. 

It is fully compatible with Zookeeper and can be accessed through the Zookeeper 
client. It implements most of the functions of Zookeeper (except: Container node, 
TTL node, quota etc.) and provides some additional functions, such as more 
monitoring indicators, manual Leader switching and so on. 

RaftKeeper provides same consistency guarantee:
1. Responses must be returned in order in one session.
2. All committed write requests must be handled in order, across all sessions.

RaftKeeper stores its data in memory, while also offering robust data persistence 
through a combination of snapshots and operation logs. To enhance system efficiency, 
the execution framework leverages a pipelined and batch processing approach, 
significantly boosting overall throughput. Crafted in C++, the system has undergone 
extensive low-level optimization to deliver superior performance, ensuring that it 
operates at peak efficiency.

The main features of RaftKeeper are its performance and query stability. 
1. The TPS is more than 2x higher than Zookeeper
2. TP99 is smoother than Zookeeper

See [benchmark](benchmark%2FREADME.md) for details.

RaftKeeper is derived from [ClickHouse](https://github.com/ClickHouse/ClickHouse) 
and take [NuRaft](https://github.com/eBay/NuRaft) as Raft implementation. 
We really appreciate the excellent work of the ClickHouse and NuRaft teams.


# How to start?

## 1. Build RaftKeeper

### Build on Ubuntu

Requirement: Ubuntu 20.04+, Clang 13+(17 is recommended), Cmake 3.20+
```
# install tools
sudo apt-get install cmake llvm-17
 
# clone project
git clone https://github.com/JDRaftKeeper/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=`which clang-17` CXX=`which clang++-17`
cd RaftKeeper && /bin/bash build.sh

# build for ClickHouse usage (for ClickHouse client is slightly incompatible with zookeeper)
cd RaftKeeper && /bin/bash build.sh 'clickhouse'

# now you can find built files in director 'build/'
```

Now RaftKeeper support build on Linux and Mac OX both x86 and arm64, details see [how-to-build](docs%2Fhow-to-build.md)

## 2. Deploy RaftKeeper

To deploy a RaftKeeper cluster you can see [how-to-deploy](docs%2Fhow-to-deploy.md).

## 3. Access RaftKeeper

You can use ZooKeeper's shell client [zkCli.sh](https://zookeeper.apache.org/doc/r3.6.0/zookeeperCLI.html) 
to access to RaftKeeper, or you can use Java, python or C ZooKeeper clients to access it.
The following is a `zkCli.sh` demo

```
./zkCli.sh -server localhost:8101
```
Also, RaftKeeper supports Zookeeper's [4lw command](https://zookeeper.apache.org/doc/r3.6.0/zookeeperAdmin.html#sc_zkCommands).
4lw command provide the ability to monitor and manage RaftKeeper, details see [how-to-monitor-and-manage](docs%2Fhow-to-monitor-and-manage.md)

# How to migrate from Zookeeper?

RaftKeeper provides a tool to translate Zookeeper data to RaftKeeper format. So you can 
simply move the translated data into RaftKeeper, detail is in [how-to-migrate-from-zookeeper](docs%2Fhow-to-migrate-from-zookeeper.md).
