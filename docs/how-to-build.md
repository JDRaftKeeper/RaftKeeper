# Build RaftKeeper

Now RaftKeeper support build on Linux and Mac OX.

### Build on Ubuntu

Requirement: Ubuntu 20.04+, Clang 13+, Cmake 3.12+
```
# install tools
sudo apt-get install cmake llvm-13
 
# clone project
git clone https://github.com/JDRaftKeeper/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=`which clang-13` CXX=`which clang++-13`
cd RaftKeeper && /bin/bash build.sh
```

### Build on Centos

Requirement: Centos 7.4+, Gcc 10.2.0+, Cmake 3.12+
```
# clone project
git clone https://github.com/JDRaftKeeper/RaftKeeper.git
git submodule sync && git submodule update --init --recursive

# build project
export CC=`which gcc-10` CXX=`which g++-10`
cd RaftKeeper && sh build.sh
```

### Build on macOS

Requirement: macOS 10.15+, Clang 13+, Cmake 3.12+

```
# install tools
brew install cmake llvm@13
 
# clone project
git clone https://github.com/JDRaftKeeper/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=/usr/local/opt/llvm@13/bin/clang CXX=/usr/local/opt/llvm@13/bin/clang++
cd RaftKeeper && sh build.sh
```

### Build for ClickHouse usage

ClickHouse client from v22.10 is a little incompatible with Zookeeper, for example multi-read response.
If you want to build a ClickHouse compatible binary.

```
sh build.sh 'clickhouse'
```

Uou can use `mntr` command to check the compatible mode of the binary.

```
echo mntr | nc localhost 8101
zk_version	RaftKeeper v2.0.4-8071b19a301138ea6525e9884d99e779d6d127c9, built on 2024-01-30 16:52:15 CST
zk_compatible_mode	zookeeper       /// The binary is zookeeper compatible.
zk_avg_latency	0
...
```