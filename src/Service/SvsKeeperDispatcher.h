#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif


#include <functional>
#include <Service/SvsKeeperServer.h>
#include <Service/SvsKeeperSettings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>


namespace DB
{
#ifndef __THREAD_POOL_VEC__
//#    define __THREAD_POOL_VEC__
#endif

using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response)>;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;
class SvsKeeperDispatcher
{
private:
    std::mutex push_request_mutex;

    CoordinationSettingsPtr coordination_settings;
    using RequestsQueue = ConcurrentBoundedQueue<SvsKeeperStorage::RequestForSession>;
    RequestsQueue requests_queue{1};
    SvsKeeperResponsesQueue responses_queue;
    std::atomic<bool> shutdown_called{false};
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;

    std::mutex session_to_response_callback_mutex;
    SessionToResponseCallback session_to_response_callback;

#ifdef __THREAD_POOL_VEC__
    std::vector<ThreadFromGlobalPool> request_threads;
    std::vector<ThreadFromGlobalPool> response_threads;
#else
    ThreadPoolPtr request_thread;
    ThreadPoolPtr responses_thread;
#endif

    ThreadFromGlobalPool session_cleaner_thread;

    std::unique_ptr<SvsKeeperServer> server;

    Poco::Logger * log;

private:
    void requestThread();
    void responseThread();
    void sessionCleanerTask();
    void setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response);

public:
    SvsKeeperDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config);

    void shutdown();

    ~SvsKeeperDispatcher() = default;

    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    bool isLeader() const { return server->isLeader(); }

    bool hasLeader() const { return server->isLeaderAlive(); }

    int64_t getSessionID(long session_timeout_ms) { return server->getSessionID(session_timeout_ms); }

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);

    /// no need to lock
    UInt64 getNodeNum()
    {
        return server->getNodeNum();
    }

    UInt64 getNodeSizeMB()
    {
        return server->getNodeSizeMB();
    }

    /// no need to lock
    UInt64 getSessionNum()
    {
        return server->getSessionNum();
    }
};

}