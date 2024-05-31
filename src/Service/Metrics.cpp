#include <Service/Metrics.h>
#include <algorithm>

namespace RK
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ReservoirSampler::update(RK::UInt64 value)
{
    UInt64 c = count.fetch_add(1);

    if (c < DEFAULT_SIZE)
    {
        values[c].store(value);
        return;
    }

    static thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<> dis(0, c);

    UInt64 i = dis(gen);
    if (i < DEFAULT_SIZE)
        values[i].store(value);
}

std::vector<UInt64> ReservoirSampler::getSnapshot() const
{
    size_t s = std::min(DEFAULT_SIZE, count.load());

    std::vector<UInt64> copy(s);

    for (size_t i = 0; i < s; i++)
        copy[i] = values[i].load();

    return copy;
}

double AdvanceSummary::getValue(const std::vector<UInt64>& numbers, double quantile)
{
    if (quantile < 0.0 || quantile > 1.0)
    {
        LOG_ERROR(&Poco::Logger::get("AdvanceSummary"), "Quantile {} is not in [0..1]", quantile);
        return 0.0;
    }

    if (numbers.empty())
        return 0.0;

    auto index = quantile * (numbers.size() + 1);
    size_t pos = static_cast<size_t>(index);

    if (pos < 1)
        return numbers[0];


    if (pos >= numbers.size())
        return numbers.back();

    auto lower = numbers[pos - 1];
    auto upper = numbers[pos];
    return lower + (index - std::floor(index)) * (upper - lower);
}

Strings AdvanceSummary::values() const
{
    auto numbers = reservoir_sampler.getSnapshot();
    std::sort(numbers.begin(), numbers.end());

    Strings results;
    results.emplace_back(fmt::format("zk_p50_{}\t{:.1f}", name, getValue(numbers, 0.5)));
    results.emplace_back(fmt::format("zk_p90_{}\t{:.1f}", name, getValue(numbers, 0.9)));
    results.emplace_back(fmt::format("zk_p99_{}\t{:.1f}", name, getValue(numbers, 0.99)));
    results.emplace_back(fmt::format("zk_p999_{}\t{:.1f}", name, getValue(numbers, 0.999)));
    results.emplace_back(fmt::format("zk_cnt_{}\t{}", name, count.load()));
    results.emplace_back(fmt::format("zk_sum_{}\t{}", name, sum.load()));
    return results;
}

Strings SimpleSummary::values() const
{
    Strings results;
    results.emplace_back(fmt::format("zk_{}\t{}", name, sum.load()));
    return results;
}

void BasicSummary::add(RK::UInt64 value)
{
    UInt64 current;

    while (value > (current = max.load()) && !max.compare_exchange_strong(current, value))
    {
    }

    while (value < (current = min.load()) && !min.compare_exchange_strong(current, value))
    {
    }

    count ++;
    sum += value;
}

Strings BasicSummary::values() const
{
    Strings results;
    results.emplace_back(fmt::format("zk_avg_{}\t{:.1f}", name, getAvg()));
    results.emplace_back(fmt::format("zk_min_{}\t{}", name, getMin()));
    results.emplace_back(fmt::format("zk_max_{}\t{}", name, max.load()));
    results.emplace_back(fmt::format("zk_cnt_{}\t{}", name, count.load()));
    results.emplace_back(fmt::format("zk_sum_{}\t{}", name, sum.load()));
    return results;
}

using SummaryPtr = std::shared_ptr<Summary>;

Metrics::Metrics()
{
    push_request_queue_time_ms = getSummary("push_request_queue_time_ms", SummaryLevel::ADVANCED);
    log_replication_batch_size = getSummary("log_replication_batch_size", SummaryLevel::BASIC);
    response_socket_send_size = getSummary("response_socket_send_size", SummaryLevel::BASIC);
    forward_response_socket_send_size = getSummary("forward_response_socket_send_size", SummaryLevel::BASIC);
    apply_write_request_time_ms = getSummary("apply_write_request_time_ms", SummaryLevel::ADVANCED);
    apply_read_request_time_ms = getSummary("apply_read_request_time_ms", SummaryLevel::ADVANCED);
    read_latency = getSummary("readlatency", SummaryLevel::ADVANCED);
    update_latency = getSummary("updatelatency", SummaryLevel::ADVANCED);

    snap_time_ms = getSummary("snap_time_ms", SummaryLevel::SIMPLE);
    snap_blocking_time_ms = getSummary("snap_blocking_time_ms", SummaryLevel::SIMPLE);
    snap_count = getSummary("snap_count", SummaryLevel::SIMPLE);
}

SummaryPtr Metrics::getSummary(const RK::String & name, RK::SummaryLevel level)
{
    SummaryPtr summary;

    if (summaries.contains(name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Already registered summary {} ", name);

    if (level == SummaryLevel::SIMPLE)
        summary = std::make_shared<SimpleSummary>(name);
    else if (level == SummaryLevel::BASIC)
        summary = std::make_shared<BasicSummary>(name);
    else
        summary =  std::make_shared<AdvanceSummary>(name);

    summaries.emplace(name, summary);

    return summary;
}

std::map<String, Strings> Metrics::dumpMetricsValues() const
{
    std::map<String, Strings> metrics_values;
    for (const auto & [name, summary] : summaries)
    {
        metrics_values.emplace(name, summary->values());
    }

    return metrics_values;
}

}
