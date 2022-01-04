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
#include <Service/NuRaftStateMachine.h>
#include <Service/Keeper4LWInfo.h>
#include <Service/KeeperConnectionStats.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/SvsKeeperSettings.h>


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

    using RequestsQueue = ConcurrentBoundedQueue<SvsKeeperStorage::RequestForSession>;
    RequestsQueue requests_queue{20000};
    SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession> responses_queue;
//    SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession> responses_queue;
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

    mutable std::mutex keeper_stats_mutex;
    KeeperConnectionStats keeper_stats;

    KeeperConfigurationAndSettingsPtr configuration_and_settings;

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

    int64_t getSessionID(int64_t session_timeout_ms) { return server->getSessionID(session_timeout_ms); }
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms)
    {
        return server->updateSessionTimeout(session_id, session_timeout_ms);
    }

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);

    /// Invoked when a request completes.
    void updateKeeperStatLatency(uint64_t process_time_ms);

    /// Are we leader
    bool isLeader() const
    {
        return server->isLeader();
    }

    bool hasLeader() const
    {
        return server->isLeaderAlive();
    }

    bool isObserver() const
    {
        return server->isObserver();
    }

    uint64_t getLogDirSize() const;

    uint64_t getSnapDirSize() const;

    /// Request statistics such as qps, latency etc.
    KeeperConnectionStats getKeeperConnectionStats() const
    {
        std::lock_guard lock(keeper_stats_mutex);
        return keeper_stats;
    }

    Keeper4LWInfo getKeeper4LWInfo();

    const NuRaftStateMachine & getStateMachine() const
    {
        return *server->getKeeperStateMachine();
    }

    const KeeperConfigurationAndSettingsPtr & getKeeperConfigurationAndSettings() const
    {
        return configuration_and_settings;
    }

    void incrementPacketsSent()
    {
        std::lock_guard lock(keeper_stats_mutex);
        keeper_stats.incrementPacketsSent();
    }

    void incrementPacketsReceived()
    {
        std::lock_guard lock(keeper_stats_mutex);
        keeper_stats.incrementPacketsReceived();
    }

    void resetConnectionStats()
    {
        std::lock_guard lock(keeper_stats_mutex);
        keeper_stats.reset();
    }
};

}
