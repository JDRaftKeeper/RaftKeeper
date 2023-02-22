#include <Service/NuRaftInMemoryLogStore.h>

namespace RK
{
namespace
{
    using namespace nuraft;
    ptr<log_entry> makeClone(const ptr<log_entry> & entry)
    {
        ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
        return clone;
    }
}

NuRaftInMemoryLogStore::NuRaftInMemoryLogStore() : start_idx(1)
{
    nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(sizeof(UInt64));
    logs[0] = nuraft::cs_new<nuraft::log_entry>(0, buf);
}

UInt64 NuRaftInMemoryLogStore::start_index() const
{
    return start_idx;
}

UInt64 NuRaftInMemoryLogStore::next_slot() const
{
    std::lock_guard<std::mutex> l(logs_lock);
    // Exclude the dummy entry.
    return start_idx + logs.size() - 1;
}

nuraft::ptr<nuraft::log_entry> NuRaftInMemoryLogStore::last_entry() const
{
    UInt64 next_idx = next_slot();
    std::lock_guard<std::mutex> lock(logs_lock);
    auto entry = logs.find(next_idx - 1);
    if (entry == logs.end())
        entry = logs.find(0);

    return makeClone(entry->second);
}

UInt64 NuRaftInMemoryLogStore::append(nuraft::ptr<nuraft::log_entry> & entry)
{
    ptr<log_entry> clone = makeClone(entry);

    std::lock_guard<std::mutex> l(logs_lock);
    UInt64 idx = start_idx + logs.size() - 1;
    logs[idx] = clone;
    return idx;
}

void NuRaftInMemoryLogStore::write_at(UInt64 index, nuraft::ptr<nuraft::log_entry> & entry)
{
    nuraft::ptr<log_entry> clone = makeClone(entry);

    // Discard all logs equal to or greater than `index.
    std::lock_guard<std::mutex> l(logs_lock);
    auto itr = logs.lower_bound(index);
    while (itr != logs.end())
        itr = logs.erase(itr);
    logs[index] = clone;
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> NuRaftInMemoryLogStore::log_entries(UInt64 start, UInt64 end)
{
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();

    ret->resize(end - start);
    UInt64 cc = 0;
    for (UInt64 i = start; i < end; ++i)
    {
        nuraft::ptr<nuraft::log_entry> src = nullptr;
        {
            std::lock_guard<std::mutex> l(logs_lock);
            auto entry = logs.find(i);
            if (entry == logs.end())
            {
                entry = logs.find(0);
                assert(0);
            }
            src = entry->second;
        }
        (*ret)[cc++] = makeClone(src);
    }
    return ret;
}

nuraft::ptr<nuraft::log_entry> NuRaftInMemoryLogStore::entry_at(UInt64 index)
{
    nuraft::ptr<nuraft::log_entry> src = nullptr;
    {
        std::lock_guard<std::mutex> l(logs_lock);
        auto entry = logs.find(index);
        if (entry == logs.end())
            entry = logs.find(0);
        src = entry->second;
    }
    return makeClone(src);
}

UInt64 NuRaftInMemoryLogStore::term_at(UInt64 index)
{
    UInt64 term = 0;
    {
        std::lock_guard<std::mutex> l(logs_lock);
        auto entry = logs.find(index);
        if (entry == logs.end())
            entry = logs.find(0);
        term = entry->second->get_term();
    }
    return term;
}

nuraft::ptr<nuraft::buffer> NuRaftInMemoryLogStore::pack(UInt64 index, Int32 cnt)
{
    std::vector<nuraft::ptr<nuraft::buffer>> returned_logs;

    UInt64 UInt64otal = 0;
    for (UInt64 ii = index; ii < index + cnt; ++ii)
    {
        ptr<log_entry> le = nullptr;
        {
            std::lock_guard<std::mutex> l(logs_lock);
            le = logs[ii];
        }
        assert(le.get());
        nuraft::ptr<nuraft::buffer> buf = le->serialize();
        UInt64otal += buf->size();
        returned_logs.push_back(buf);
    }

    nuraft::ptr<buffer> buf_out = nuraft::buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + UInt64otal);
    buf_out->pos(0);
    buf_out->put(static_cast<Int32>(cnt));

    for (auto & entry : returned_logs)
    {
        nuraft::ptr<nuraft::buffer> & bb = entry;
        buf_out->put(static_cast<Int32>(bb->size()));
        buf_out->put(*bb);
    }
    return buf_out;
}

void NuRaftInMemoryLogStore::apply_pack(UInt64 index, nuraft::buffer & pack)
{
    pack.pos(0);
    Int32 num_logs = pack.get_int();

    for (Int32 i = 0; i < num_logs; ++i)
    {
        UInt64 cur_idx = index + i;
        Int32 buf_size = pack.get_int();

        nuraft::ptr<nuraft::buffer> buf_local = nuraft::buffer::alloc(buf_size);
        pack.get(buf_local);

        nuraft::ptr<nuraft::log_entry> le = nuraft::log_entry::deserialize(*buf_local);
        {
            std::lock_guard<std::mutex> l(logs_lock);
            logs[cur_idx] = le;
        }
    }

    {
        std::lock_guard<std::mutex> l(logs_lock);
        auto entry = logs.upper_bound(0);
        if (entry != logs.end())
            start_idx = entry->first;
        else
            start_idx = 1;
    }
}

bool NuRaftInMemoryLogStore::compact(UInt64 last_log_index)
{
    std::lock_guard<std::mutex> l(logs_lock);
    for (UInt64 ii = start_idx; ii <= last_log_index; ++ii)
    {
        auto entry = logs.find(ii);
        if (entry != logs.end())
            logs.erase(entry);
    }

    start_idx = last_log_index + 1;
    return true;
}

}
