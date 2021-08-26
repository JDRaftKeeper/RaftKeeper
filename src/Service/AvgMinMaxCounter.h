#pragma once

#include <atomic>
#include <common/types.h>

namespace DB
{
struct AvgMinMaxCounter
{
    AvgMinMaxCounter(AvgMinMaxCounter & counter);
    String name;
    std::atomic<UInt64> total = 0;
    std::atomic<UInt64> min = 0;
    std::atomic<UInt64> max = 0;
    std::atomic<UInt64> count = 0;

    explicit AvgMinMaxCounter(String && name_) : name(name_) { }

    void addDataPoint(UInt64 value);

    UInt64 getAvg() {
        UInt64 count_val = count.load();
        return count_val == 0 ? 0 : total.load() / count_val;
    }

    UInt64 getMin() { return min.load(); }

    UInt64 getMax() { return max.load(); }

    void reset();

private:
    void setMin(UInt64 value);

    void setMax(UInt64 value);

};
}
