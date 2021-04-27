#ifndef CLICKHOUSE_SVSKEEPERMETRICS_H
#define CLICKHOUSE_SVSKEEPERMETRICS_H


#include <atomic>
#include <cstdint>
#include <utility>
#include <stddef.h>
#include <common/types.h>

#include <Interpreters/Context.h>


namespace ServiceMetrics
{
/// Metric identifier (index in array).
using Metric = size_t;
using Value = DB::Int64;

/// Get name of metric by identifier. Returns statically allocated string.
const char * getName(Metric event);
/// Get text description of metric by identifier. Returns statically allocated string.
const char * getDocumentation(Metric event);

/// Metric identifier -> current value of metric.
extern std::atomic<Value> values[];

/// Get index just after last metric identifier.
Metric end();

/// Set value of specified metric.
inline void set(Metric metric, Value value)
{
    values[metric].store(value, std::memory_order_relaxed);
}

/// Add value for specified metric. You must subtract value later; or see class Increment below.
inline void add(Metric metric, Value value = 1)
{
    values[metric].fetch_add(value, std::memory_order_relaxed);
}

inline void sub(Metric metric, Value value = 1)
{
    add(metric, -value);
}

/// For lifetime of object, add amount for specified metric. Then subtract.
class Increment
{
private:
    std::atomic<Value> * what;
    Value amount;

    Increment(std::atomic<Value> * what_, Value amount_) : what(what_), amount(amount_) { *what += amount; }

public:
    Increment(Metric metric, Value amount_ = 1) : Increment(&values[metric], amount_) { }

    ~Increment()
    {
        if (what)
            what->fetch_sub(amount, std::memory_order_relaxed);
    }

    Increment(Increment && old) { *this = std::move(old); }

    Increment & operator=(Increment && old)
    {
        what = old.what;
        amount = old.amount;
        old.what = nullptr;
        return *this;
    }

    void changeTo(Value new_amount)
    {
        what->fetch_add(new_amount - amount, std::memory_order_relaxed);
        amount = new_amount;
    }

    void sub(Value value = 1)
    {
        what->fetch_sub(value, std::memory_order_relaxed);
        amount -= value;
    }

    /// Subtract value before destructor.
    void destroy()
    {
        what->fetch_sub(amount, std::memory_order_relaxed);
        what = nullptr;
    }
};

class MetricsUpdater
{
public:
    MetricsUpdater(DB::Context & global_context_, int update_period_seconds)
        : global_context(global_context_), update_period(update_period_seconds), log(&(Poco::Logger::get("MetricsUpdater")))
    {
    }

    void start()
    {
        thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
    }

    void run();
    void updateMetrics();

    ~MetricsUpdater();

private:
    DB::Context & global_context;
    const std::chrono::seconds update_period;

    std::mutex mutex;
    std::condition_variable wait_cond;

    bool quit{false};
    std::unique_ptr<ThreadFromGlobalPool> thread;

    Poco::Logger * log;
};

}


#endif //CLICKHOUSE_SVSKEEPERMETRICS_H
