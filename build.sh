#!/bin/bash
set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(cd "$ROOT"; pwd)
mkdir -p $ROOT/build

function version()
{
  grep VERSION_STRING $ROOT/version.txt | awk '{print $2}' | awk  -F')' '{print $1}'
}

# build binary
function build()
{
  cd $ROOT/build && find $ROOT/build -type f ! -name '*.tar.gz' -exec rm -f {} +

  COMPATIBLE_MODE_ZOOKEEPER="ON"
  if [ -n "$1" ] && [ "$1" == "clickhouse" ]; then
    COMPATIBLE_MODE_ZOOKEEPER="OFF"
  fi
  cmake .. -DCMAKE_BUILD_TYPE=Release -DCOMPATIBLE_MODE_ZOOKEEPER=$COMPATIBLE_MODE_ZOOKEEPER

  PARALLEL="$(($(nproc) / 4 + 1))"
  make -j $PARALLEL
}

# make tarball
function package()
{
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

  if [ -n "$1" ] && [ "$1" == "clickhouse" ]; then
    file_name=RaftKeeper-`version`-linux-x86_64-clickhouse.tar.gz
  else
    file_name=RaftKeeper-`version`-linux-x86_64.tar.gz
  fi
  tar -czvf $file_name.tar.gz RaftKeeper

  rm -rf RaftKeeper

  echo "RaftKeeper is built into build/${file_name}."
}

# Zookeeper compatible installation package
build "zookeeper"
package "zookeeper"

# ClickHouse compatible installation package
build "clickhouse"
package "clickhouse"

