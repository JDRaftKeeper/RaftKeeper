#pragma once

#include <string>

#include <Interpreters/Context.h>

#include <IO/WriteBuffer.h>
#include <Service/SvsKeeperMetrics.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
class SvsKeeperMetricsWriter;
using SvsKeeperMetricsWriterPtr = std::shared_ptr<SvsKeeperMetricsWriter>;

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

    /// type counter
    static inline constexpr auto service_events_prefix = "SvsKeeperProfileEvents_";
    /// type gauge
    static inline constexpr auto service_metrics_prefix = "SvsKeeperMetrics_";
//    static inline constexpr auto service_status_prefix = "SvsKeeperStatusInfo_";
};

}
