#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Service/NuRaftLogSegment.h>
#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>

namespace DB
{
using nuraft::int32;
using nuraft::int64;
using nuraft::ulong;

#ifndef _TEST_MEMORY_
//#    define _TEST_MEMORY_
#endif

class LogEntryQueue
{
public:
    LogEntryQueue() : batch_index(0), max_index(0), log(&(Poco::Logger::get("FileLogStore"))) { }
    ptr<log_entry> getEntry(const UInt64 & index);
    void putEntry(UInt64 & index, ptr<log_entry> & entry);
    void putEntryOrClear(UInt64 & index, ptr<log_entry> & entry);
    void clear();

private:
    static constexpr UInt8 BIT_SIZE = 16;
    static constexpr UInt32 MAX_VECTOR_SIZE = 65536; //2^16
    UInt64 batch_index;
    UInt64 max_index;
    ptr<log_entry> entry_vec[MAX_VECTOR_SIZE];
    std::shared_mutex queue_mutex;
    Poco::Logger * log;
};

class NuRaftFileLogStore : public nuraft::log_store
{
    __nocopy__(NuRaftFileLogStore)

public :
    NuRaftFileLogStore(const std::string & log_dir, bool force_new = false, bool force_sync_ = true, bool async_fsync_ = true, UInt64 fsync_interval_ = 1);
    NuRaftFileLogStore(const std::string & log_dir, bool force_new, UInt32 max_log_size_, UInt32 max_segment_count_, bool force_sync_ = true, bool async_fsync_ = true, UInt64 fsync_interval_ = 1);
    ~NuRaftFileLogStore() override;

    ulong next_slot() const override;

    ulong start_index() const override;

    ptr<log_entry> last_entry() const override;

    ulong append(ptr<log_entry> & entry) override;

    void write_at(ulong index, ptr<log_entry> & entry) override;

    void end_of_append_batch(ulong start, ulong cnt) override;

    //get log entries
    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override;
    ptr<std::vector<ptr<log_entry>>> log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) override;

    ptr<std::vector<VersionLogEntry>> log_entries_version_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0);

    ptr<log_entry> entry_at(ulong index) override;

    ulong term_at(ulong index) override;

    ptr<buffer> pack(ulong index, int32 cnt) override;

    void apply_pack(ulong index, buffer & pack) override;

    bool compact(ulong last_log_index) override;

    bool flush() override;

    ulong last_durable_index() override;

    void shutdown();

    void setRaftServer(nuraft::ptr<nuraft::raft_server> raft_instance_)
    {
        raft_instance = raft_instance_;
    }

    const ptr<LogSegmentStore> segmentStore() const { return segment_store; }

private:
    static ptr<log_entry> make_clone(const ptr<log_entry> & entry);
    void fsyncThread();
    Poco::Logger * log;
    ptr<LogSegmentStore> segment_store;
    LogEntryQueue log_queue;

    //mutable std::recursive_mutex log_lock;
    //log start index in current segment
    //std::atomic<UInt64> start_idx;
    //log's count in current segment

    //std::shared_mutex index_mutex;

#ifdef _TEST_MEMORY_
    std::atomic<UInt64> last_log_index_;
    std::unordered_map<ulong, ptr<log_entry>> mem_logs_;
#endif

    //last log entry
    ptr<log_entry> last_log_entry;
//    std::atomic<UInt32> to_flush_count {};
    bool force_sync;
    bool async_fsync;
    UInt64 fsync_interval{1};
    UInt64 to_flush_count{0};
    ThreadFromGlobalPool fsync_thread;
    std::atomic<bool> shutdown_called{false};
    ulong disk_last_durable_index;
    std::shared_ptr<Poco::Event> async_fsync_event;
    nuraft::ptr<nuraft::raft_server> raft_instance;
};

}
