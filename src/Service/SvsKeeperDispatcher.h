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
    struct RequestsQueue
    {
        using Queue = ConcurrentBoundedQueue<SvsKeeperStorage::RequestForSession>;
        std::vector<ptr<Queue>> queues;

        explicit RequestsQueue(size_t child_queue_size, size_t capacity = 20000)
        {
            assert(child_queue_size > 0);
            assert(capacity > 0);

            queues.resize(child_queue_size);
            for (size_t i=0; i< child_queue_size; i++)
            {
                queues[i] = std::make_shared<Queue>(std::max(1ul, capacity / child_queue_size));
            }
        }

        bool push(const SvsKeeperStorage::RequestForSession & request)
        {
            return queues[request.session_id % queues.size()]->push(std::forward<const SvsKeeperStorage::RequestForSession>(request));
        }

        bool tryPush(const SvsKeeperStorage::RequestForSession & request, UInt64 wait_ms = 0)
        {
            return queues[request.session_id % queues.size()]->tryPush(
                std::forward<const SvsKeeperStorage::RequestForSession>(request), wait_ms);
        }

        bool pop(size_t queue_id, SvsKeeperStorage::RequestForSession & request)
        {
            assert(queue_id != 0 && queue_id <= queues.size());
            return queues[queue_id]->pop(request);
        }

        bool tryPop(size_t queue_id, SvsKeeperStorage::RequestForSession & request, UInt64 wait_ms = 0)
        {
            assert(queue_id != 0 && queue_id <= queues.size());
            return queues[queue_id]->tryPop(request, wait_ms);
        }

        bool tryPopAny(SvsKeeperStorage::RequestForSession & request, UInt64 wait_ms = 0)
        {
            for (const auto & queue : queues)
            {
                if (queue->tryPop(request, wait_ms))
                    return true;
            }
            return false;
        }

        size_t size() const
        {
            size_t size{};
            for (const auto & queue : queues)
                size += queue->size();
            return size;
        }
    };

    std::mutex push_request_mutex;
    ptr<RequestsQueue> requests_queue;
    SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession> responses_queue;
//    SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession> responses_queue;
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

    std::unique_ptr<SvsKeeperServer> server;

    mutable std::mutex keeper_stats_mutex;
    KeeperConnectionStats keeper_stats;

    KeeperConfigurationAndSettingsPtr configuration_and_settings;

    Poco::Logger * log;


private:
    void requestThread(const int thread_index);
    void responseThread();
    void sessionCleanerTask();
    void setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response);

public:
    SvsKeeperDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config);

    void shutdown();

    ~SvsKeeperDispatcher() = default;

    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    int64_t getSessionID(int64_t session_timeout_ms) { return server->getSessionID(session_timeout_ms); }
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms)
    {
        return server->updateSessionTimeout(session_id, session_timeout_ms);
    }

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
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
};

}
