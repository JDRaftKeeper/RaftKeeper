#include <algorithm>

#include <IO/WriteHelpers.h>

#include "Service/SvsKeeperDispatcher.h"
#include "SvsKeeperMetrics.h"
#include "SvsKeeperMetricsWriter.h"
#include "SvsKeeperProfileEvents.h"


namespace ServiceMetrics
{
extern const Metric IsLeader;
extern const Metric Sessions;
extern const Metric Nodes;
extern const Metric StateMachineSizeInMB;
}

namespace
{
template <typename T>
void writeOutLine(DB::WriteBuffer & wb, T && val)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar('\n', wb);
}

template <typename T, typename... TArgs>
void writeOutLine(DB::WriteBuffer & wb, T && val, TArgs &&... args)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar(' ', wb);
    writeOutLine(wb, std::forward<TArgs>(args)...);
}

void replaceInvalidChars(std::string & metric_name)
{
    std::replace(metric_name.begin(), metric_name.end(), '.', '_');
}

}

namespace DB
{
SvsKeeperMetricsWriter::SvsKeeperMetricsWriter(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_name)
    : send_events(config.getBool(config_name + ".events", true))
    , send_metrics(config.getBool(config_name + ".metrics", true))
//    , send_asynchronous_metrics(config.getBool(config_name + ".asynchronous_metrics", true))
    , send_status_info(config.getBool(config_name + ".status_info", true))
{
}

void SvsKeeperMetricsWriter::write(WriteBuffer & wb) const
{
    if (send_events)
    {
        for (size_t i = 0, end = ServiceProfileEvents::end(); i < end; ++i)
        {
            const auto counter = ServiceProfileEvents::global_counters[i].load(std::memory_order_relaxed);

            std::string metric_name{ServiceProfileEvents::getName(static_cast<ServiceProfileEvents::Event>(i))};
            std::string metric_doc{ServiceProfileEvents::getDocumentation(static_cast<ServiceProfileEvents::Event>(i))};

            replaceInvalidChars(metric_name);
            std::string key{service_events_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "counter");
            writeOutLine(wb, key, counter);
        }
    }

    if (send_metrics)
    {
        for (size_t i = 0, end = ServiceMetrics::end(); i < end; ++i)
        {
            // collect metrics
            const auto value = ServiceMetrics::values[i].load(std::memory_order_relaxed);

            std::string metric_name{ServiceMetrics::getName(static_cast<ServiceMetrics::Metric>(i))};
            std::string metric_doc{ServiceMetrics::getDocumentation(static_cast<ServiceMetrics::Metric>(i))};

            replaceInvalidChars(metric_name);
            std::string key{service_metrics_prefix + metric_name};

            writeOutLine(wb, "# HELP", key, metric_doc);
            writeOutLine(wb, "# TYPE", key, "gauge");
            writeOutLine(wb, key, value);
        }
    }

    if (send_status_info)
    {
    }
}

}
