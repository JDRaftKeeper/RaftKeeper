# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/poco/Util
    contrib/libs/NuRaft
)


SRCS(
    FileLogStore.cpp
    KeeperServer.cpp
    KeeperStateMachine.cpp
    LogEntry.cpp
    LogSegment.cpp
    RaftCommon.cpp
    RaftGRPCService.cpp
    RaftKeeperServer.cpp
    RaftServer.cpp
    RaftStateManager.cpp
    SvsKeeperStorage.cpp
    SvsKeeperStorageDispatcher.cpp
    SessionExpiryQueue.cpp
    Snapshot.cpp
    ThreadSafeQueue.cpp
    tests/raft_grpc_client.cpp
    tests/raft_tcp_client.cpp

)

END()
