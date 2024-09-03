#pragma once

#include <array>
#include <atomic>
#include <map>
#include <random>

#include <Poco/Logger.h>

#include <common/types.h>


namespace RK
{

inline UInt64 getCurrentTimeMilliseconds()
{
    return duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
};


/**
 * Uses the reservoir sampling algorithm to sample statistical values
 * The ReservoirSampler is thread-safe
 */
class ReservoirSampler
{
public:
    static constexpr UInt64 DEFAULT_SIZE = 4096;

    size_t size() const
    {
        UInt64 c = count.load();
        return c > DEFAULT_SIZE ? c : DEFAULT_SIZE;
    }

    void update(UInt64 value);

    void reset()
    {
        count.store(0);
    }

    std::vector<UInt64> getSnapshot() const;

private:
    std::atomic<UInt64> count{0};
    std::array<std::atomic<UInt64>, DEFAULT_SIZE> values;
};


class Summary
{
public:
    virtual void add(UInt64) = 0;
    virtual void reset() = 0;
    virtual ~Summary() = default;
    virtual Strings values() const = 0;
};

enum SummaryLevel
{
    /**
     * The returned Summary is expected to track only sum of values
     */
     SIMPLE,

    /**
     * The returned Summary is expected to track only simple aggregated
     * values, like min/max/avg
     */
    BASIC,

    /**
     * It is expected that the returned Summary performs expensive
     * aggregations, like percentiles.
     */
    ADVANCED
};


class SimpleSummary : public Summary
{
public:
    explicit SimpleSummary(const String & name_): name(name_)
    {
    }

    Strings values() const override;

    void add(RK::UInt64 value) override { sum += value; }
    UInt64 getSum() const { return sum.load(); }

    void reset() override { sum.store(0); }

private:
    String name;
    std::atomic<UInt64> sum{0};
};

class BasicSummary : public Summary
{
public:
    explicit BasicSummary(const String & name_): name(name_)
    {
    }

    void reset() override
    {
        count.store(0);
        sum.store(0);
        min.store(std::numeric_limits<UInt64>::max());
        max.store(0);
    }

    void add(UInt64 value) override;

    double getAvg() const
    {
        UInt64 current_count = count.load();
        UInt64 current_sum = sum.load();

        return current_count > 0 ? current_sum / current_count : 0;
    }

    UInt64 getMin() const
    {
        UInt64 current_min = min.load();
        return current_min == std::numeric_limits<UInt64>::max() ? 0 : current_min;
    }

    Strings values() const override;

private:
    String name;
    std::atomic<UInt64> count{0};
    std::atomic<UInt64> sum{0};
    std::atomic<UInt64> min{std::numeric_limits<UInt64>::max()};
    std::atomic<UInt64> max{0};
};

class AdvanceSummary : public Summary
{
public:
    explicit AdvanceSummary(const String & name_): name(name_)
    {
    }

    void reset() override
    {
        count.store(0);
        sum.store(0);
        reservoir_sampler.reset();
    }

    void add(UInt64 value) override
    {
        count ++;
        sum += value;
        reservoir_sampler.update(value);
    }

    static double getValue(const std::vector<UInt64>& numbers, double quantile);

    Strings values() const override;

private:
    String name;
    ReservoirSampler reservoir_sampler;
    std::atomic<UInt64> count{0};
    std::atomic<UInt64> sum{0};
};

/** Implements Summary Metrics for RK.
  * There is possible race-condition, but we don't need the stats to be extremely accurate.
  */
class Metrics
{
public:
    using SummaryPtr = std::shared_ptr<Summary>;

    static Metrics & getMetrics()
    {
        static Metrics metrics;
        return metrics;
    }

    std::map<String, Strings> dumpMetricsValues() const;

    void reset()
    {
        for (const auto & [_, summary] : summaries)
            summary->reset();
    }

    SummaryPtr push_request_queue_time_ms;
    SummaryPtr log_replication_batch_size;
    SummaryPtr response_socket_send_size;
    SummaryPtr forward_response_socket_send_size;
    SummaryPtr apply_write_request_time_ms;
    SummaryPtr apply_read_request_time_ms;
    SummaryPtr read_latency;
    SummaryPtr update_latency;
    SummaryPtr snap_time_ms;
    SummaryPtr snap_blocking_time_ms;
    SummaryPtr snap_count;

private:
    Metrics();
    SummaryPtr getSummary(const String & name, SummaryLevel level);

    std::map<String, SummaryPtr> summaries;
};

}
