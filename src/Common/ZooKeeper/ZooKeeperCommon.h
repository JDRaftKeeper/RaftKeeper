/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <boost/noncopyable.hpp>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <map>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <cstdint>
#include <optional>
#include <functional>


namespace Coordination
{

struct ZooKeeperResponse : virtual Response
{
    XID xid = 0;
    int64_t zxid;

    /// used to calculate request latency
    UInt64 request_created_time_ms = 0;

    virtual ~ZooKeeperResponse() override = default;
    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void write(WriteBuffer & out) const;
    virtual OpNum getOpNum() const = 0;

    virtual bool operator== (const ZooKeeperResponse & response) const
    {
        if (const ZooKeeperResponse * zk_response = dynamic_cast<const ZooKeeperResponse *>(&response))
        {
            return error == zk_response->error && xid == zk_response->xid;
        }
        return false;
    }

    bool operator!= (const ZooKeeperResponse & response) const
    {
        return !(*this == response);
    }

    String toString() const override
    {
        return Response::toString() + ", xid " + std::to_string(xid) + ", zxid " + std::to_string(zxid);
    }
};

using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

struct ZooKeeperRequest : virtual Request
{
    XID xid = 0;
    bool has_watch = false;
    /// If the request was not send and the error happens, we definitely sure, that it has not been processed by the server.
    /// If the request was sent and we didn't get the response and the error happens, then we cannot be sure was it processed or not.
    bool probably_sent = false;

    bool restored_from_zookeeper_log = false;

    ZooKeeperRequest() = default;
    ZooKeeperRequest(const ZooKeeperRequest &) = default;
    virtual ~ZooKeeperRequest() override = default;

    virtual OpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out) const;

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void readImpl(ReadBuffer &) = 0;

    static std::shared_ptr<ZooKeeperRequest> read(ReadBuffer & in);

    virtual ZooKeeperResponsePtr makeResponse() const = 0;
    virtual bool isReadRequest() const = 0;
};

using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperHeartbeatRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
    void writeImpl(WriteBuffer &) const override {}
    void readImpl(ReadBuffer &) override {}
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid);
    }
};

struct ZooKeeperHeartbeatResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
};

/** Internal request.
 */
struct ZooKeeperSetWatchesRequest final : ZooKeeperRequest
{
    using Watches = std::vector<String>;

    int64_t relative_zxid;
    Watches data_watches;
    Watches exist_watches;
    Watches list_watches;

    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::SetWatches; }
    void writeImpl(WriteBuffer &) const override;
    void readImpl(ReadBuffer &) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid);
    }
};

struct ZooKeeperSetWatchesResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::SetWatches; }
};

struct ZooKeeperSyncRequest final : ZooKeeperRequest
{
    String path;
    String getPath() const override { return path; }
    OpNum getOpNum() const override { return OpNum::Sync; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperSyncResponse final : ZooKeeperResponse
{
    String path;
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Sync; }
};

struct ZooKeeperWatchResponse final : WatchResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    void write(WriteBuffer & out) const override;

    OpNum getOpNum() const override
    {
        throw Exception("OpNum for watch response doesn't exist", Error::ZRUNTIMEINCONSISTENCY);
    }
};

struct ZooKeeperAuthRequest final : ZooKeeperRequest
{
    int32_t type = 0;   /// ignored by the server
    String scheme;
    String data;

    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Auth; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", type " + std::to_string(type) + ", scheme " + scheme + ", data" + data;
    }
};

struct ZooKeeperAuthResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}

    OpNum getOpNum() const override { return OpNum::Auth; }
};

struct ZooKeeperCloseRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Close; }
    void writeImpl(WriteBuffer &) const override {}
    void readImpl(ReadBuffer &) override {}

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid);
    }
};

struct ZooKeeperCloseResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override
    {
        throw Exception("Received response for close request", Error::ZRUNTIMEINCONSISTENCY);
    }

    void writeImpl(WriteBuffer &) const override {}

    OpNum getOpNum() const override { return OpNum::Close; }
};

struct ZooKeeperCreateRequest final : public CreateRequest, ZooKeeperRequest
{
    /// used only during restore from zookeeper log
    int32_t parent_cversion = -1;

    ZooKeeperCreateRequest() = default;
    explicit ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Create; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    String data;
        //    bool is_ephemeral = false;
        //    bool is_sequential = false;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + ", data " + data + ", is_ephemeral "
            + std::to_string(is_ephemeral) + ", is_sequential " + std::to_string(is_sequential);
    }
};

struct ZooKeeperCreateResponse final : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Create; }

    bool operator== (const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperCreateResponse * create_response = dynamic_cast<const ZooKeeperCreateResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && create_response->path_created == path_created;
        }
        return false;
    }

    String toString() const override
    {
        return "CreateResponse " + ZooKeeperResponse::toString() + ", path_created " + path_created;
    }
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() = default;
    explicit ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Remove; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + ", version "
            + std::to_string(version);
    }
};

struct ZooKeeperRemoveResponse final : RemoveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::Remove; }
};

struct ZooKeeperExistsRequest final : ExistsRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Exists; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        //    String path;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Exists; }

    bool operator== (const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperExistsResponse * exists_response = dynamic_cast<const ZooKeeperExistsResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && exists_response->stat == stat;
        }
        return false;
    }

    String toString() const override
    {
        return "ExistsResponse ," + ZooKeeperResponse::toString() + ", stat " + stat.toString();
    }
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Get; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        //    String path;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Get; }

    bool operator== (const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperGetResponse * get_response = dynamic_cast<const ZooKeeperGetResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && get_response->stat == stat && get_response->data == data;
        }
        return false;
    }

    String toString() const override
    {

        return "GetResponse " + ZooKeeperResponse::toString() + ", stat " + stat.toString() + ", data " + data;
    }
};

struct ZooKeeperSetRequest final : SetRequest, ZooKeeperRequest
{
    ZooKeeperSetRequest() = default;
    explicit ZooKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Set; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    String data;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + "，data " + data + ", version "
            + std::to_string(version);
    }
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Set; }

    bool operator== (const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperSetResponse * set_response = dynamic_cast<const ZooKeeperSetResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && set_response->stat == stat;
        }
        return false;
    }

    String toString() const override
    {
        return "SetResponse " + ZooKeeperResponse::toString() + ", stat " + stat.toString();
    }
};

struct ZooKeeperListRequest : ListRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::List; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperSimpleListRequest final : ZooKeeperListRequest
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperListResponse : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::List; }

    bool operator== (const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperListResponse * list_response = dynamic_cast<const ZooKeeperListResponse *>(&response))
        {
            std::vector<String> copy_other_nodes(list_response->names);
            std::vector<String> copy_nodes(names);
            std::sort(copy_other_nodes.begin(), copy_other_nodes.end());
            std::sort(copy_nodes.begin(), copy_nodes.end());
            return ZooKeeperResponse::operator==(response) && list_response->stat == stat && copy_other_nodes == copy_nodes;
        }
        return false;
    }

    String toString() const override
    {
        String base = "ListResponse " + ZooKeeperResponse::toString() + ", stat " + stat.toString() + ", names ";
        auto func = [&](const String & value){ base += ", " + value; };
        std::for_each(names.begin(), names.end(), func);
        return base;
    }
};

struct ZooKeeperSimpleListResponse final : ZooKeeperListResponse
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
};

struct ZooKeeperCheckRequest final : CheckRequest, ZooKeeperRequest
{
    ZooKeeperCheckRequest() = default;
    explicit ZooKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Check; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    String data;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + ", version "
            + std::to_string(version);
    }
};

struct ZooKeeperCheckResponse final : CheckResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::Check; }
};

/// This response may be received only as an element of responses in MultiResponse.
struct ZooKeeperErrorResponse final : ErrorResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Error; }
};

struct ZooKeeperSetACLRequest final : SetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::SetACL; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

struct ZooKeeperSetACLResponse final : SetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::SetACL; }
};

struct ZooKeeperGetACLRequest final : GetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::GetACL; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
};

struct ZooKeeperGetACLResponse final : GetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::GetACL; }
};

struct ZooKeeperMultiRequest final : MultiRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Multi; }
    ZooKeeperMultiRequest() = default;

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls);

    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override;
    String toString() const override
    {
        String base = Coordination::toString(getOpNum());
        auto func = [&](const RequestPtr & value){ base += ", " + value->toString(); };
        std::for_each(requests.begin(), requests.end(), func);
        return base;
    }
};

struct ZooKeeperMultiResponse final : MultiResponse, ZooKeeperResponse
{
    OpNum getOpNum() const override { return OpNum::Multi; }

    explicit ZooKeeperMultiResponse(const Requests & requests)
    {
        responses.reserve(requests.size());

        for (const auto & request : requests)
            responses.emplace_back(dynamic_cast<const ZooKeeperRequest &>(*request).makeResponse());
    }

    explicit ZooKeeperMultiResponse(const Responses & responses_)
    {
        responses = responses_;
    }

    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    bool operator== (const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperMultiResponse * multi_response = dynamic_cast<const ZooKeeperMultiResponse *>(&response))
        {
            if (ZooKeeperResponse::operator==(response))
            {
                for (size_t i = 0; i < responses.size(); ++i)
                {
                    /// responses list must be ZooKeeperResponse ?
                    if (const ZooKeeperResponse * rhs_response = dynamic_cast<const ZooKeeperResponse *>(multi_response->responses[i].get()))
                    {
                        if (const ZooKeeperResponse * lhs_response = dynamic_cast<const ZooKeeperResponse *>(responses[i].get()))
                            if (*rhs_response != *lhs_response)
                                return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    String toString() const override
    {
        String base = "MultiResponse " + ZooKeeperResponse::toString();
        auto func = [&](const ResponsePtr & value){ base += ", " + value->toString(); };
        std::for_each(responses.begin(), responses.end(), func);
        return base;
    }
};

/// Fake internal coordination (keeper) response. Never received from client
/// and never send to client.
struct ZooKeeperSessionIDRequest final : ZooKeeperRequest
{
    int64_t internal_id;
    int64_t session_timeout_ms;
    /// Who requested this session
    int32_t server_id;

    Coordination::OpNum getOpNum() const override { return OpNum::SessionID; }
    String getPath() const override { return {}; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    Coordination::ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

/// Fake internal coordination (keeper) response. Never received from client
/// and never send to client.
struct ZooKeeperSessionIDResponse final : ZooKeeperResponse
{
    int64_t internal_id;
    int64_t session_id;
    /// Who requested this session
    int32_t server_id;

    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    Coordination::OpNum getOpNum() const override { return OpNum::SessionID; }
};

struct ZooKeeperSetSeqNumRequest final : SetSeqNumRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::SetSeqNum; }
    ZooKeeperSetSeqNumRequest() = default;

    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        return "";
    }
};

struct ZooKeeperSetSeqNumResponse final : SetSeqNumResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::SetSeqNum; }

    bool operator== (const ZooKeeperResponse &) const override
    {
        throw Exception("Unsupport operator.", Error::ZBADARGUMENTS);
    }

    String toString() const override
    {
        return "SetSeqNumResponse ";
    }
};


class ZooKeeperRequestFactory final : private boost::noncopyable
{

public:
    using Creator = std::function<ZooKeeperRequestPtr()>;
    using OpNumToRequest = std::unordered_map<OpNum, Creator>;

    static ZooKeeperRequestFactory & instance();

    ZooKeeperRequestPtr get(OpNum op_num) const;

    void registerRequest(OpNum op_num, Creator creator);

private:
    OpNumToRequest op_num_to_request;
    ZooKeeperRequestFactory();
};

}
