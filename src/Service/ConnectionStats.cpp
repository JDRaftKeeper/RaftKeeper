#include <Service/ConnectionStats.h>

namespace RK
{

uint64_t ConnectionStats::getMinLatency() const
{
    return min_latency;
}

uint64_t ConnectionStats::getMaxLatency() const
{
    return max_latency;
}

uint64_t ConnectionStats::getAvgLatency() const
{
    if (count != 0)
        return total_latency / count;
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
    total_latency += (latency_ms);
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
    min_latency = 0;
    last_latency = 0;
}

void ConnectionStats::resetRequestCounters()
{
    packets_received = 0;
    packets_sent = 0;
}

}
