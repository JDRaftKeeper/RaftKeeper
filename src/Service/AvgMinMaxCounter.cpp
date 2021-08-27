#include "AvgMinMaxCounter.h"

namespace DB
{
void AvgMinMaxCounter::AvgMinMaxCounter::addDataPoint(UInt64 value)
{
    total.fetch_add(value);
    count.fetch_add(1);
    setMin(value);
    setMax(value);
}

void AvgMinMaxCounter::reset()
{
    total.exchange(0);
    min.exchange(0);
    max.exchange(0);
    count.exchange(0);
}

void AvgMinMaxCounter::setMin(UInt64 value)
{
    UInt64 old_val;
    while ((old_val = min.load()) > value && !min.compare_exchange_strong(old_val, value))
    {
    }
}

void AvgMinMaxCounter::setMax(UInt64 value)
{
    UInt64 old_val;
    while ((old_val = max.load()) < value && !max.compare_exchange_strong(old_val, value))
    {
    }
}

AvgMinMaxCounter::AvgMinMaxCounter(AvgMinMaxCounter & counter)
    : total(counter.total.load()), min(counter.min.load()), max(counter.max.load()), count(counter.count.load())
{
}

}
