#include "SvsKeeperProfileEvents.h"

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(req_all, "all client request count, include heart beat, internal generated request not included") \
    M(req_read, "client read request count, if request get with watch, treat as write request") \
    M(req_write, "client write request count, if request get with watch, treat as write request") \
    M(req_time, "all client request time") \
    M(sm_req_heart_beat, "state machine request heart beat count") \
    M(sm_req_sync, "state machine sync request count") \
    M(sm_req_list, "list request count") \
    M(sm_req_create, "state machine create request count") \
    M(sm_req_remove, "state machine remove request count") \
    M(sm_req_exist, "exists request count") \
    M(sm_req_get, "get request count") \
    M(sm_req_set, "state machine set request count") \
    M(sm_req_multi, "state machine multi request count") \
    M(sm_req_check, "check request count") \
    M(sm_req_close, "state machine close request count") \
    M(watch_response, "state machine watch response count") \
    M(list_watch_response, "state machine list watch response count") \
    M(create_snapshot_count, "snapshot creating count since startup") \
    M(load_snapshot_count, "snapshot loading count since startup") \
    M(apply_received_snapshot_count, "apply received snapshot count since startup") \


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
