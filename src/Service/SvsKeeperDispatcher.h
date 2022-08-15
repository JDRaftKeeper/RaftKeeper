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
#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperSyncProcessor.h>
#include <Service/SvsKeeperCommitProcessor.h>
#include <Service/SvsKeeperFollowerProcessor.h>
#include <Poco/FIFOBuffer.h>

#define USE_NIO_FOR_KEEPER

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
    ptr<RequestsQueue> requests_queue;
    SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession> responses_queue;
    std::atomic<bool> shutdown_called{false};
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;

    std::mutex session_to_response_callback_mutex;
    SessionToResponseCallback session_to_response_callback;

    using UpdateConfigurationQueue = ConcurrentBoundedQueue<ConfigUpdateAction>;
    /// More than 1k updates is definitely misconfiguration.
    UpdateConfigurationQueue update_configuration_queue{1000};

#ifdef __THREAD_POOL_VEC__
    std::vector<ThreadFromGlobalPool> request_threads;
    std::vector<ThreadFromGlobalPool> response_threads;
#else
    ThreadPoolPtr request_thread;
    ThreadPoolPtr responses_thread;
#endif

    ThreadFromGlobalPool session_cleaner_thread;

    /// Apply or wait for configuration changes
    ThreadFromGlobalPool update_configuration_thread;

    std::shared_ptr<SvsKeeperServer> server;

    mutable std::mutex keeper_stats_mutex;
    KeeperConnectionStats keeper_stats;

    KeeperConfigurationAndSettingsPtr configuration_and_settings;

    Poco::Logger * log;

    std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor;

    SvsKeeperSyncProcessor svskeeper_sync_processor;

    SvsKeeperFollowerProcessor follower_request_processor;

private:
    void requestThread();
    void requestThreadFakeZk(size_t thread_index);
    void responseThread();
    void sessionCleanerTask();
    void setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response);

public:
    SvsKeeperDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config);

    void shutdown();

    ~SvsKeeperDispatcher() = default;

    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    bool putForwardingRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    int64_t getSessionID(int64_t session_timeout_ms) { return server->getSessionID(session_timeout_ms); }
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms)
    {
        return server->updateSessionTimeout(session_id, session_timeout_ms);
    }

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback, bool is_reconnected = false);
    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);

    /// Thread apply or wait configuration changes from leader
    void updateConfigurationThread();
    /// Registered in ConfigReloader callback. Add new configuration changes to
    /// update_configuration_queue. Keeper Dispatcher apply them asynchronously.
    void updateConfiguration(const Poco::Util::AbstractConfiguration & config);

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
    void requestThreadAtomicConsistency(size_t thread_index);
    void requestThreadFakeZooKeeper(size_t thread_index);
};

}
