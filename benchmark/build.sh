#!/bin/bash
set -eo pipefail

# build benchmark
ROOT=$(dirname "$0")
ROOT=$(cd "$ROOT"; pwd)

mvn clean compile package

cd $ROOT/target

unzip raft-benchmark-1.0-bin.zip
chmod +x raft-benchmark-1.0/bin/benchmark.sh

echo "benchmark is built into 'benchmark/target/raft-benchmark-1.0'."
