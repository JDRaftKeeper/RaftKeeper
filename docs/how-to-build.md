# Build RaftKeeper

RaftKeeper supports build on Linux and Mac OX.

### Build on Ubuntu

Requirement: Ubuntu 20.04+, Clang 17+, Cmake 3.12+
```
# install tools
sudo apt-get install cmake llvm-17
 
# clone project
git clone https://github.com/JDRaftKeeper/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=`which clang-17` CXX=`which clang++-17`
cd RaftKeeper && /bin/bash build.sh
```

### Build on macOS

Requirement: macOS 10.15+, Clang 17+, Cmake 3.20+

```
# install tools
brew install cmake llvm@17
 
# clone project
git clone https://github.com/JDRaftKeeper/RaftKeeper.git
git submodule sync && git submodule update --init --recursive
 
# build project
export CC=`which clang-17` CXX=`which clang++17`
cd RaftKeeper && sh build.sh
```

### Build for ClickHouse usage

ClickHouse client from v22.10 is a little incompatible with Zookeeper, for example multi-read response.
If you want to build a ClickHouse compatible binary, you can take the following command

```
sh build.sh 'clickhouse'
```

You can use `mntr` command to check the compatible mode of the binary.

```
echo mntr | nc localhost 8101

zk_version	RaftKeeper v2.0.4-8071b19a301138ea6525e9884d99e779d6d127c9, built on 2024-01-30 16:52:15 CST
zk_compatible_mode	zookeeper       /// The binary is zookeeper compatible.
...
```