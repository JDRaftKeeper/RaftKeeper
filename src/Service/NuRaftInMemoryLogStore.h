#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Core/Types.h>
#include <libnuraft/log_store.hxx> // Y_IGNORE

namespace DB
{

class NuRaftInMemoryLogStore : public nuraft::log_store
{
public:
    NuRaftInMemoryLogStore();

    UInt64 start_index() const override;

    UInt64 next_slot() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    UInt64 append(nuraft::ptr<nuraft::log_entry> & entry) override;

    void write_at(UInt64 index, nuraft::ptr<nuraft::log_entry> & entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(UInt64 start, UInt64 end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(UInt64 index) override;

    UInt64 term_at(UInt64 index) override;

    nuraft::ptr<nuraft::buffer> pack(UInt64 index, Int32 cnt) override;

    void apply_pack(UInt64 index, nuraft::buffer & pack) override;

    bool compact(UInt64 last_log_index) override;

    bool flush() override { return true; }

private:
    std::map<size_t, nuraft::ptr<nuraft::log_entry>> logs;
    mutable std::mutex logs_lock;
    std::atomic<size_t> start_idx;
};

}
