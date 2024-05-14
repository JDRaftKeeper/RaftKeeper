Service directory contains RaftKeeper core logical. It mainly contains 4 parts:

1. State machine, log store and snapshot store which are under NuRaft framework.
2. Pipeline and batching execution framework which is the key of high performance.
3. Keeper store which execute ZooKeeper logical. This part is originally from ClickHouse, we really appreciate the excellent work of the ClickHouse team.
