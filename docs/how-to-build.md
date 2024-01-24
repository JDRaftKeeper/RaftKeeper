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

Requirement: macOS 10+, Clang 13+, Cmake 3.12+

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