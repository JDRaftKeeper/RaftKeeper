#pragma once

#include <ZooKeeper/ZooKeeperCommon.h>
#include <libnuraft/nuraft.hxx>


namespace RK
{

struct RequestId;

/// Attached session id to request
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

/// Attached session id to response
struct ResponseForSession
{
    int64_t session_id;
    Coordination::ZooKeeperResponsePtr response;
};

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
    int64_t session_id;
    Coordination::XID xid;
    Coordination::OpNum opnum;

    String toString() const;
    RequestId getRequestId() const;
};

using ErrorRequests = std::list<ErrorRequest>;

}
