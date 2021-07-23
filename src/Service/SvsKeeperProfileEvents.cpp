#include "SvsKeeperProfileEvents.h"

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(SvsKeeperInit, "") \
    M(SvsKeeperTransactions, "") \
    M(SvsKeeperTransactionTimeInMicroseconds, "") \
    M(SvsKeeperHeartBeat, "") \
    M(SvsKeeperSync, "") \
    M(SvsKeeperList, "") \
    M(SvsKeeperCreate, "") \
    M(SvsKeeperRemove, "") \
    M(SvsKeeperExists, "") \
    M(SvsKeeperGet, "") \
    M(SvsKeeperSet, "") \
    M(SvsKeeperMulti, "") \
    M(SvsKeeperCheck, "") \
    M(SvsKeeperClose, "") \
    M(SvsKeeperWatchResponse, "") \
    M(SvsKeeperListWatchResponse, "") \
    M(SvsKeeperUserExceptions, "") \
    M(SvsKeeperHardwareExceptions, "") \
    M(SvsKeeperOtherExceptions, "") \
    M(SvsKeeperWaitMicroseconds, "") \
    M(SvsKeeperBytesSent, "") \
    M(SvsKeeperBytesReceived, "") \


namespace ServiceProfileEvents
{

#define M(NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = __COUNTER__;

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
/// Initialize global counters statically
Counters global_counters(global_counters_array);

const Event Counters::num_counters = END;


Counters::Counters(VariableContext level_, Counters * parent_)
    : counters_holder(new Counter[num_counters] {}),
      parent(parent_),
      level(level_)
{
    counters = counters_holder.get();
}

void Counters::resetCounters()
{
    if (counters)
    {
        for (Event i = 0; i < num_counters; ++i)
            counters[i].store(0, std::memory_order_relaxed);
    }
}

void Counters::reset()
{
    parent = nullptr;
    resetCounters();
}

Counters Counters::getPartiallyAtomicSnapshot() const
{
    Counters res(VariableContext::Snapshot, nullptr);
    for (Event i = 0; i < num_counters; ++i)
        res.counters[i].store(counters[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
    return res;
}

const char * getName(Event event)
{
    static const char * strings[] =
        {
#define M(NAME, DOCUMENTATION) #NAME,
            APPLY_FOR_EVENTS(M)
#undef M
        };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
        {
#define M(NAME, DOCUMENTATION) DOCUMENTATION,
            APPLY_FOR_EVENTS(M)
#undef M
        };

    return strings[event];
}


Event end() { return END; }


void increment(Event event, Count amount)
{
    global_counters.increment(event, amount);
}

}

#undef APPLY_FOR_EVENTS
