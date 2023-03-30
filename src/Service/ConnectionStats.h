#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <common/types.h>

namespace RK
{

/// Request statistics for connection or dispatcher
class ConnectionStats
{
public:
    ConnectionStats() = default;

    uint64_t getMinLatency() const;
    uint64_t getMaxLatency() const;

    uint64_t getAvgLatency() const;
    uint64_t getLastLatency() const;

    uint64_t getPacketsReceived() const;
    uint64_t getPacketsSent() const;

    void incrementPacketsReceived();
    void incrementPacketsSent();

    void updateLatency(uint64_t latency_ms);
    void reset();

private:
    void resetLatency();
    void resetRequestCounters();

    /// all responses with watch response included
    uint64_t packets_sent = 0;
    /// All user requests
    uint64_t packets_received = 0;

    /// For consistent with zookeeper measured by millisecond,
    /// otherwise maybe microsecond is better
    uint64_t total_latency = 0;

    uint64_t max_latency = 0;
    uint64_t min_latency = std::numeric_limits<uint64_t>::max();

    /// last operation latency
    uint64_t last_latency = 0;
    /// request count
    uint64_t count = 0;
};

}
