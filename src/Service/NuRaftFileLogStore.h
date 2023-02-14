/**
 * Copyright 2021-2023 JD.com, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Service/NuRaftLogSegment.h>
#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Service/Settings.h>

namespace RK
{
using nuraft::int32;
using nuraft::int64;
using nuraft::ulong;

class LogEntryQueue
{
public:
    LogEntryQueue() : batch_index(0), max_index(0), log(&(Poco::Logger::get("LogEntryQueue"))) { }
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
    NuRaftFileLogStore(
        const std::string & log_dir,
        bool force_new = false,
        UInt32 max_log_size_ = LogSegmentStore::MAX_LOG_SIZE,
        UInt32 max_segment_count_ = LogSegmentStore::MAX_SEGMENT_COUNT,
        FsyncMode log_fsync_mode_ = FsyncMode::FSYNC_PARALLEL,
        UInt64 log_fsync_interval_ = 1000);

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
    void fsyncThread(bool & thread_started);

    Poco::Logger * log;
    ptr<LogSegmentStore> segment_store;
    LogEntryQueue log_queue;

    ptr<log_entry> last_log_entry;
    FsyncMode log_fsync_mode;
    UInt64 log_fsync_interval;

    UInt64 to_flush_count{0};
    ThreadFromGlobalPool fsync_thread;
    std::atomic<bool> shutdown_called{false};

    ulong disk_last_durable_index;
    std::shared_ptr<Poco::Event> parallel_fsync_event;
    nuraft::ptr<nuraft::raft_server> raft_instance;
};

}
