set -eo pipefail
START_DIR=`dirname "$0"`
START_DIR=`cd "$START_DIR"; pwd`
cd $START_DIR/
pid=`cat raftkeeper.pid`
kill -15 $pid
rm raftkeeper.pid
