#!/bin/bash
set -eo pipefail

# build raftkeeper
ROOT=$(dirname "$0")
ROOT=$(cd "$ROOT"; pwd)
mkdir -p $ROOT/build

cd $ROOT/build
cmake .. -DCMAKE_BUILD_TYPE=Release

PARALLEL="$(($(nproc) / 4 + 1))"
make -j $PARALLEL

# make tarball
cd "$ROOT"
mkdir -p build/RaftKeeper
mkdir -p build/RaftKeeper/bin
mkdir -p build/RaftKeeper/lib
mkdir -p build/RaftKeeper/conf
mkdir -p build/RaftKeeper/log
mkdir -p build/RaftKeeper/data

cp programs/server/bin/start.sh build/RaftKeeper/bin/.
cp programs/server/bin/stop.sh build/RaftKeeper/bin/.
cp build/programs/raftkeeper build/RaftKeeper/lib/.
cp programs/server/config.xml build/RaftKeeper/conf/.

cd $ROOT/build
tar -czvf RaftKeeper.tar.gz RaftKeeper
rm -rf RaftKeeper

echo "RaftKeeper is built into 'build/RaftKeeper.tar.gz'."
