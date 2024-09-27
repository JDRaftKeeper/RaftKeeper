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

using nuraft::cmd_result_code;
inline String toString(const cmd_result_code & code)
{
    switch (code) {
        case cmd_result_code::OK:
            return "OK";
        case cmd_result_code::CANCELLED:
            return "CANCELLED";
        case cmd_result_code::TIMEOUT:
            return "TIMEOUT";
        case cmd_result_code::NOT_LEADER:
            return "NOT_LEADER";
        case cmd_result_code::BAD_REQUEST:
            return "BAD_REQUEST";
        case cmd_result_code::SERVER_ALREADY_EXISTS:
            return "SERVER_ALREADY_EXISTS";
        case cmd_result_code::CONFIG_CHANGING:
            return "CONFIG_CHANGING";
        case cmd_result_code::SERVER_IS_JOINING:
            return "SERVER_IS_JOINING";
        case cmd_result_code::SERVER_NOT_FOUND:
            return "SERVER_NOT_FOUND";
        case cmd_result_code::CANNOT_REMOVE_LEADER:
            return "CANNOT_REMOVE_LEADER";
        case cmd_result_code::SERVER_IS_LEAVING:
            return "SERVER_IS_LEAVING";
        case cmd_result_code::TERM_MISMATCH:
            return "TERM_MISMATCH";
        case cmd_result_code::RESULT_NOT_EXIST_YET:
            return "RESULT_NOT_EXIST_YET";
        case cmd_result_code::FAILED:
            return "FAILED";
    }
    return "Unknown value: {}" + std::to_string(code);
}

/// Simple error request info.
struct ErrorRequest
{
    bool accepted;
    cmd_result_code error_code; /// TODO new error code instead of NuRaft error code
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

using nuraft::log_val_type;
inline std::string toString(const log_val_type & log_type)
{
    switch (log_type)
    {
        case log_val_type::app_log:
            return "app_log";
        case log_val_type::conf:
            return "conf";
        case log_val_type::cluster_server:
            return "cluster_server";
        case log_val_type::log_pack:
            return "log_pack";
        case log_val_type::snp_sync_req:
            return "snp_sync_req";
        case log_val_type::custom:
            return "custom";
    }
    return "Unknown value: {}" + std::to_string(log_type);
}

}
