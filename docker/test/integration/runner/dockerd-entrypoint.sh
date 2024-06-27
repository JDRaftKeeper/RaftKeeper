#!/bin/bash
set -e

setsid dockerd --host=unix:///var/run/docker.sock --tls=false --host=tcp://0.0.0.0:2375 --default-address-pool base=172.17.0.0/12,size=24 &>/var/log/somefile &

set +e
reties=0
while true; do
    docker info &>/dev/null && break
    reties=$((reties+1))
    if [[ $reties -ge 200 ]]; then # 20 sec max
        echo "Can't start docker daemon, 20 sec timeout exceeded." >&2
        ps -ef|grep dockerd >&2
        cat /var/log/somefile >&2
        exit 1;
    fi
    sleep 0.1
done
docker load -i /usr/local/bin/zookeeper.tar
docker load -i /usr/local/bin/raftkeeper-integration-helper.tar
docker load -i /usr/local/bin/raftkeeper-integration-tests.tar
docker image ls

set -e

echo "Start tests"
export RAFTKEEPER_TESTS_SERVER_BIN_PATH=/raftkeeper
export RAFTKEEPER_TESTS_BASE_CONFIG_DIR=/raftkeeper-config

cd /RaftKeeper/tests/integration
exec "$@"
