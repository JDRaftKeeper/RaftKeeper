#include <Service/ConnectionStats.h>

namespace RK
{

uint64_t ConnectionStats::getMinLatency() const
{
    UInt64 current_min = min_latency.load();
    return current_min == std::numeric_limits<uint64_t>::max() ? static_cast<uint64_t>(0): current_min;
}

uint64_t ConnectionStats::getMaxLatency() const
{
    return max_latency;
}

uint64_t ConnectionStats::getAvgLatency() const
{
    uint64_t current_count = count.load();
    if (current_count != 0)
        return total_latency / current_count;
    return 0;
}

uint64_t ConnectionStats::getLastLatency() const
{
    return last_latency;
}

uint64_t ConnectionStats::getPacketsReceived() const
{
    return packets_received;
}

uint64_t ConnectionStats::getPacketsSent() const
{
    return packets_sent;
}

void ConnectionStats::incrementPacketsReceived()
{
    packets_received++;
}

void ConnectionStats::incrementPacketsSent()
{
    packets_sent++;
}

void ConnectionStats::updateLatency(uint64_t latency_ms)
{
    last_latency = latency_ms;
    total_latency += latency_ms;
    count++;

    if (latency_ms < min_latency)
    {
        min_latency = latency_ms;
    }

    if (latency_ms > max_latency)
    {
        max_latency = latency_ms;
    }
}

void ConnectionStats::reset()
{
    resetLatency();
    resetRequestCounters();
}

void ConnectionStats::resetLatency()
{
    total_latency = 0;
    count = 0;
    max_latency = 0;
    last_latency = 0;
    min_latency = std::numeric_limits<uint64_t>::max();
}

void ConnectionStats::resetRequestCounters()
{
    packets_received = 0;
    packets_sent = 0;
}

}
