#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include "Service/SvsKeeperDispatcher.h"
#include "SvsKeeperMetrics.h"

/// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M) \
    M(avg_latency, "avg latency in microsecond") \
    M(max_latency, "max latency in microsecond") \
    M(min_latency, "min latency in microsecond") \
    M(num_alive_connections, "Active session count") \
    M(outstanding_requests, "requests blocked in queue")   \
    M(is_leader, "Whether current node is Raft leader") \
    M(znode_count, "nodes count which include all node types") \
    M(watch_count, "watch count") \
    M(ephemerals_count, "ephemeral nodes count") \
    M(approximate_data_size, "state machine size in MB, not accurate") \
    M(zxid, "state machine zxid") \

    /**
     * M(packets_received, "request count") \
     * M(packets_sent, "response count include watch response") \
     * M(followers, "state machine size in MB, not accurate") \
     * M(pending_syncs, "pending requests to synchronize to followers, only in leader") \
     * M(open_file_descriptor_count, "state machine size in MB, not accurate") \
     * M(max_file_descriptor_count, "state machine size in MB, not accurate")
     */



namespace ServiceMetrics
{
#define M(NAME, DOCUMENTATION) extern const Metric NAME = __COUNTER__;
APPLY_FOR_METRICS(M)
#undef M
constexpr Metric END = __COUNTER__;

std::atomic<Value> values[END]{}; /// Global variable, initialized by zeros.

const char * getName(Metric event)
{
    static const char * strings[] = {
#define M(NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_METRICS(M)
#undef M
    };

    return strings[event];
}

const char * getDocumentation(Metric event)
{
    static const char * strings[] = {
#define M(NAME, DOCUMENTATION) DOCUMENTATION,
        APPLY_FOR_METRICS(M)
#undef M
    };

    return strings[event];
}

Metric end()
{
    return END;
}

///--------- MetricsUpdater

static auto get_next_update_time(std::chrono::seconds update_period)
{
    using namespace std::chrono;

    const auto now = time_point_cast<seconds>(system_clock::now());

    // Use seconds since the start of the hour, because we don't know when
    // the epoch started, maybe on some weird fractional time.
    const auto start_of_hour = time_point_cast<seconds>(time_point_cast<hours>(now));
    const auto seconds_passed = now - start_of_hour;

    // Rotate time forward by half a period -- e.g. if a period is a minute,
    // we'll collect metrics on start of minute + 30 seconds. This is to
    // achieve temporal separation with MetricTransmitter. Don't forget to
    // rotate it back.
    const auto rotation = update_period / 2;

    const auto periods_passed = (seconds_passed + rotation) / update_period;
    const auto seconds_next = (periods_passed + 1) * update_period - rotation;
    const auto time_next = start_of_hour + seconds_next;

    return time_next;
}

void MetricsUpdater::run()
{
    setThreadName("MetricsUpdater");
    LOG_INFO(log, "MetricsUpdater start");

    while (true)
    {
        {
            std::unique_lock lock{mutex};
            if (wait_cond.wait_until(lock, get_next_update_time(update_period), [this] { return quit; }))
            {
                break;
            }
        }

        try
        {
            updateMetrics();
            /// reset request counter
            global_context.getSvsKeeperStorageDispatcher()->resetRequestCounter();
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    LOG_INFO(log, "MetricsUpdater stopped");
}

void MetricsUpdater::updateMetrics()
{
    ServiceMetrics::set(ServiceMetrics::is_leader, global_context.getSvsKeeperStorageDispatcher()->isLeader());
    ServiceMetrics::set(ServiceMetrics::num_alive_connections, global_context.getSvsKeeperStorageDispatcher()->getSessionNum());

    ServiceMetrics::set(ServiceMetrics::znode_count, global_context.getSvsKeeperStorageDispatcher()->getNodeNum());
    ServiceMetrics::set(ServiceMetrics::watch_count, global_context.getSvsKeeperStorageDispatcher()->getWatchNodeNum());
    ServiceMetrics::set(ServiceMetrics::ephemerals_count, global_context.getSvsKeeperStorageDispatcher()->getEphemeralNodeNum());
    ServiceMetrics::set(ServiceMetrics::approximate_data_size, global_context.getSvsKeeperStorageDispatcher()->getNodeSizeMB());

    DB::AvgMinMaxCounter request_counter = global_context.getSvsKeeperStorageDispatcher()->getRequestCounter();
    ServiceMetrics::set(ServiceMetrics::avg_latency, request_counter.getAvg());
    ServiceMetrics::set(ServiceMetrics::min_latency, request_counter.getMin());
    ServiceMetrics::set(ServiceMetrics::max_latency, request_counter.getMax());

    ServiceMetrics::set(ServiceMetrics::outstanding_requests, global_context.getSvsKeeperStorageDispatcher()->getOutstandingRequests());
    ServiceMetrics::set(ServiceMetrics::zxid, global_context.getSvsKeeperStorageDispatcher()->getZxid());
}

MetricsUpdater::~MetricsUpdater()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        wait_cond.notify_one();
        if (thread)
            thread->join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}

#undef APPLY_FOR_METRICS
