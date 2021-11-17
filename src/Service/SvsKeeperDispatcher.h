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
#include <Service/AvgMinMaxCounter.h>


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

    SvsKeeperSettingsPtr coordination_settings;
    using RequestsQueue = SvsKeeperThreadSafeQueue<SvsKeeperStorage::RequestForSession>;
    RequestsQueue requests_queue;
//    ConcurrentBoundedQueue<SvsKeeperStorage::ResponseForSession> responses_queue{1};
    SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession> responses_queue;
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
    AvgMinMaxCounter request_counter{"request"};

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

    SvsKeeperStorage::ResponsesForSessions singleProcessReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session);

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
    UInt64 getWatchNodeNum()
    {
        return server->getWatchNodeNum();
    }
    UInt64 getEphemeralNodeNum()
    {
        return server->getEphemeralNodeNum();
    }

    UInt64 getNodeSizeMB()
    {
        return server->getNodeSizeMB();
    }

    UInt64 getZxid()
    {
        return server->getZxid();
    }

    /// no need to lock
    UInt64 getSessionNum()
    {
        return server->getSessionNum();
    }

    UInt64 getOutstandingRequests()
    {
        return requests_queue.size();
    }

    void resetRequestCounter()
    {
        request_counter.reset();
    }

    void addValueToRequestCounter(UInt64 value)
    {
        request_counter.addDataPoint(value);
    }

    AvgMinMaxCounter getRequestCounter()
    {
        return request_counter;
    }

    SessionAndWatcherPtr getWatchInfo() const{
        return server->getWatchInfo();
    }

    EphemeralsPtr getEphemeralInfo() { return server->getEphemeralInfo(); }

};

}
