#pragma once

#include <string>

#include <Interpreters/Context.h>

#include <IO/WriteBuffer.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Service/SvsKeeperMetrics.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Write metrics in Prometheus format
class SvsKeeperMetricsWriter
{
public:
    SvsKeeperMetricsWriter(
        const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    void write(WriteBuffer & wb) const;

private:
    const bool send_events;
    const bool send_metrics;
    const bool send_status_info;

    static inline constexpr auto service_events_prefix = "SvsKeeperProfileEvents_";
    static inline constexpr auto service_metrics_prefix = "SvsKeeperMetrics_";
//    static inline constexpr auto service_status_prefix = "SvsKeeperStatusInfo_";
//    static inline constexpr auto asynchronous_metrics_prefix = "ClickHouseAsyncMetrics_";
//    static inline constexpr auto current_status_prefix = "ClickHouseStatusInfo_";
};

}
