set -eo pipefail
START_DIR=`dirname "$0"`
START_DIR=`cd "$START_DIR"; pwd`
cd $START_DIR/..
nohup ./lib/raftkeeper server --config=conf/config.xml >/dev/null 2>&1 &
pidfile="${START_DIR}/raftkeeper.pid"
echo $! >"${pidfile}"
