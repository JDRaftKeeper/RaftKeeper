## Docker containers for integration tests
- `runner` container with that runs integration tests in docker
- `help` container helper docker container to run iptables without sudo
- `base` container to run RaftKeeper
- `runnner/compose` contains docker\_compose YaML files that are used in tests

## How to build a new image for integration tests
### Make sure you got those images
```
#You can pull those images, or build those yourself.
docker pull raftkeeper/raftkeeper-integration-tests:latest
docker pull raftkeeper/raftkeeper-network-partition:latest
docker pull zookeeper:3.7.1
```
### Build
```
cd RaftKeeper/docker/test/integration/runner

#We just save all images we need, and copy those to image raftkeeper-integration-tests-runner.
#So that we can use raftkeeper-integration-tests-runner even if you are in an internal network environment.
#You can see more detail in runner/Dockerfile and runner/dockerd-entrypoint.sh
docker save -o raftkeeper-integration-tests.tar zookeeper:3.7.1
docker save -o raftkeeper-network-partition.tar raftkeeper/raftkeeper-network-partition:latest
docker save -o raftkeeper-integration-tests.tar raftkeeper/raftkeeper-integration-tests:latest

#build
docker build -t raftkeeper/raftkeeper-integration-tests-runner .
```

How to run integration tests is described in tests/integration/README.md
