set -eo pipefail
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`
mkdir -p $ROOT/build
cd $ROOT/build
cmake .. -DENABLE_JEMALLOC=0
PARALLEL="$(($(nproc) / 4 + 1))"
make -j $PARALLEL
cd $ROOT
mkdir -p RaftKeeper
mkdir -p RaftKeeper/bin
mkdir -p RaftKeeper/lib
mkdir -p RaftKeeper/conf
mkdir -p RaftKeeper/log
mkdir -p RaftKeeper/data
cp programs/server/bin/start.sh RaftKeeper/bin/.
cp programs/server/bin/stop.sh RaftKeeper/bin/.
cp build/programs/raftkeeper RaftKeeper/lib/.
cp programs/server/config.xml RaftKeeper/conf/.
tar -czvf RaftKeeper.tar.gz RaftKeeper
