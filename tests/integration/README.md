## RaftKeeper integration tests

This directory contains integration tests who run in docker. The first level docker 
`raftkeeper_integration_tests` will set up a RaftKeeper or Zookeeper cluster who 
runs in the second level docker.

### Running with runner script

You can run tests via `./runner` script and pass pytest arguments as last arg:
```
$ ./runner --binary /path/to/raftkeeper_bianry  --base-configs-dir /path/to/raftkeeper_config_dir 'test_auth -ss'
```

Path to binary and configs maybe specified via env variables:
```
$ export RAFTKEEPER_TESTS_BASE_CONFIG_DIR=$HOME/ClickHouse/programs/server/
$ export RAFTKEEPER_TESTS_SERVER_BIN_PATH=/path/to/raftkeeper_bianry
$ 
$ ./runner
$ or ./runner '-v -ss'
$ or ./runner 'test_auth'
$ or ./runner test/auth/test.py::test_digest_auth_basic
```

