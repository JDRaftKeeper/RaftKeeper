#pragma once

#include <Common/ThreadPool.h>
#include <libnuraft/nuraft.hxx>

#include <ZooKeeper/ZooKeeperCommon.h>


namespace RK
{

using RunnerId = size_t;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

#ifdef COMPATIBLE_MODE_ZOOKEEPER
    const String ZOOKEEPER_SYSTEM_PATH = "/zookeeper";
    const String ZOOKEEPER_CONFIG_NODE = ZOOKEEPER_SYSTEM_PATH + "/config";
#else
    const String CLICKHOUSE_KEEPER_SYSTEM_PATH = "/keeper";
    const String CLICKHOUSE_KEEPER_API_VERSION_PATH = CLICKHOUSE_KEEPER_SYSTEM_PATH + "/api_version";

    enum class KeeperApiVersion : uint8_t
    {
        ZOOKEEPER_COMPATIBLE = 0,
        WITH_FILTERED_LIST,
        WITH_MULTI_READ
    };

    inline constexpr auto CURRENT_KEEPER_API_VERSION = KeeperApiVersion::WITH_MULTI_READ;
#endif

struct RequestId;

/// Attached session id and forwarding info to request
struct RequestForSession
{
    int64_t session_id;
    Coordination::ZooKeeperRequestPtr request;

    /// measured in millisecond
    int64_t create_time{};

    /// for forward request
    int32_t server_id{-1};
    int32_t client_id{-1};

    //    /// RaftKeeper can generate request, for example: sessionCleanerTask
    //    bool is_internal{false};

    explicit RequestForSession() = default;

    RequestForSession(Coordination::ZooKeeperRequestPtr request_, int64_t session_id_, int64_t create_time_)
        : session_id(session_id_), request(request_), create_time(create_time_)
    {
    }

    bool isForwardRequest() const { return server_id > -1 && client_id > -1; }
    RequestId getRequestId() const;

    String toString() const;
    String toSimpleString() const;
};

using RequestsForSessions = std::vector<RequestForSession>;

/// Attached session id to response
struct ResponseForSession
{
    int64_t session_id;
    Coordination::ZooKeeperResponsePtr response;
};

using ResponsesForSessions = std::vector<ResponseForSession>;

/// Global client request id.
struct RequestId
{
    int64_t session_id;
    Coordination::XID xid;

    String toString() const;
    bool operator==(const RequestId & other) const;

    struct RequestIdHash
    {
        std::size_t operator()(const RequestId & request_id) const;
    };
};

/// Simple error request info.
struct ErrorRequest
{
    bool accepted;
    nuraft::cmd_result_code error_code; /// TODO new error code instead of NuRaft error code
    int64_t session_id; /// For new session request, this is internal_id, for there is no session_id right now.
    Coordination::XID xid;
    Coordination::OpNum opnum;

    String toString() const;
    RequestId getRequestId() const;
};

using ErrorRequests = std::list<ErrorRequest>;

/// Is new session request or update session request
bool isSessionRequest(Coordination::OpNum opnum);
bool isSessionRequest(const Coordination::ZooKeeperRequestPtr & request);

bool isNewSessionRequest(Coordination::OpNum opnum);

}
