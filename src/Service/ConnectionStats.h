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
    std::atomic<uint64_t> packets_sent{0};
    /// All user requests
    std::atomic<uint64_t> packets_received{0};

    /// For consistent with zookeeper measured by millisecond,
    /// otherwise maybe microsecond is better
    std::atomic<UInt64> total_latency{0};

    std::atomic<UInt64> max_latency{0};
    std::atomic<UInt64> min_latency{std::numeric_limits<uint64_t>::max()};

    /// last operation latency
    std::atomic<UInt64> last_latency{0};
    /// request count
    std::atomic<UInt64> count{0};
};

}
