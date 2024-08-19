#include <memory>
#include <unistd.h>
#include <Service/LogEntry.h>
#include <Service/NuRaftFileLogStore.h>
#include <Common/setThreadName.h>

namespace RK
{
using namespace nuraft;

ptr<log_entry> LogEntryQueue::getEntry(const UInt64 & index)
{
    std::shared_lock read_lock(queue_mutex);

    /// match index
    if (index > max_index || max_index - index >= MAX_VECTOR_SIZE)
        return nullptr;

    /// match cycle
    if (index >> BIT_SIZE == batch_index || index >> BIT_SIZE == batch_index - 1)
        return entry_vec[index & (MAX_VECTOR_SIZE - 1)];

    return nullptr;
}

void LogEntryQueue::putEntry(UInt64 & index, const ptr<log_entry> & entry)
{
    LOG_TRACE(log, "put entry {}, index {}, batch {}", index, index & (MAX_VECTOR_SIZE - 1), batch_index);
    std::lock_guard write_lock(queue_mutex);
    entry_vec[index & (MAX_VECTOR_SIZE - 1)] = entry;
    batch_index = std::max(batch_index, index >> BIT_SIZE);
    max_index = std::max(max_index, index);
}

void LogEntryQueue::clear()
{
    LOG_INFO(log, "clear log queue.");
    std::lock_guard write_lock(queue_mutex);
    batch_index = 0;
    max_index = 0;
    for (auto & i : entry_vec)
        i = nullptr;
}

NuRaftFileLogStore::NuRaftFileLogStore(
    const String & log_dir,
    bool force_new,
    FsyncMode log_fsync_mode_,
    UInt64 log_fsync_interval_,
    UInt32 max_log_size_)
    : log_fsync_mode(log_fsync_mode_), log_fsync_interval(log_fsync_interval_)
{
    log = &(Poco::Logger::get("FileLogStore"));

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
    {
        parallel_fsync_event = std::make_shared<Poco::Event>();

        fsync_thread = ThreadFromGlobalPool([this] { fsyncThread(); });
    }

    segment_store = LogSegmentStore::getInstance(log_dir, force_new);
    segment_store->init(max_log_size_);

    if (segment_store->lastLogIndex() < 1)
        /// no log entry exists, return a dummy constant entry with value set to null and term set to  zero
        last_log_entry = cs_new<log_entry>(0, nuraft::buffer::alloc(0));
    else
        last_log_entry = segment_store->getEntry(segment_store->lastLogIndex());

    disk_last_durable_index = segment_store->lastLogIndex();
}

void NuRaftFileLogStore::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
    {
        parallel_fsync_event->set();
        if (fsync_thread.joinable())
            fsync_thread.join();
    }
}

NuRaftFileLogStore::~NuRaftFileLogStore()
{
    shutdown();
}

void NuRaftFileLogStore::fsyncThread()
{
    setThreadName("LogFsync");

    while (!shutdown_called)
    {
        parallel_fsync_event->wait();

        UInt64 last_flush_index = segment_store->flush();
        if (last_flush_index)
        {
            disk_last_durable_index = last_flush_index;
            if (raft_instance) /// For test
                raft_instance->notify_log_append_completion(true);
        }
    }

    LOG_INFO(log, "shutdown background raft log fsync thread.");
}

ulong NuRaftFileLogStore::next_slot() const
{
    return segment_store->lastLogIndex() + 1;
}

ulong NuRaftFileLogStore::start_index() const
{
    return segment_store->firstLogIndex();
}

ptr<log_entry> NuRaftFileLogStore::last_entry() const
{
    if (last_log_entry)
        return cloneLogEntry(last_log_entry);
    return cs_new<log_entry>(0, nuraft::buffer::alloc(0));
}

ulong NuRaftFileLogStore::append(ptr<log_entry> & entry)
{
    ptr<log_entry> cloned = cloneLogEntry(entry);
    UInt64 log_index = segment_store->appendEntry(entry);
    log_queue.putEntry(log_index, cloned);

    last_log_entry = cloned;

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL && entry->get_val_type() != log_val_type::app_log)
        parallel_fsync_event->set();

    return log_index;
}

void NuRaftFileLogStore::write_at(ulong index, ptr<log_entry> & entry)
{
    if (segment_store->writeAt(index, entry) == index)
        log_queue.clear();

    last_log_entry = entry;

    /// notify parallel fsync thread
    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL && entry->get_val_type() != log_val_type::app_log)
        parallel_fsync_event->set();

    LOG_DEBUG(log, "write entry at {}", index);
}

void NuRaftFileLogStore::end_of_append_batch(ulong start, ulong cnt)
{
    LOG_TRACE(log, "fsync log store, start log idx {}, log count {}", start, cnt);

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
    {
        parallel_fsync_event->set();
    }
    else if (log_fsync_mode == FsyncMode::FSYNC_BATCH)
    {
        to_flush_count += cnt;
        if (to_flush_count >= log_fsync_interval)
        {
            to_flush_count = 0;
            flush();
        }
    }
    else if (log_fsync_mode == FsyncMode::FSYNC)
    {
        flush();
    }
}

ptr<std::vector<ptr<log_entry>>> NuRaftFileLogStore::log_entries(ulong start, ulong end)
{
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();
    for (auto i = start; i < end; i++)
    {
        ret->push_back(entry_at(i));
    }
    LOG_DEBUG(log, "log entries, start {} end {}", start, end);
    return ret;
}

ptr<std::vector<ptr<log_entry>>> NuRaftFileLogStore::log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes)
{
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();

    int64 get_size = 0;

    for (auto i = start; i < end; i++)
    {
        auto entry = entry_at(i);
        if (!entry)
            return nullptr;

        int64_t entry_size = entry->get_buf().size() + sizeof(ulong) + sizeof(char);

        if (batch_size_hint_in_bytes > 0 && get_size + entry_size > batch_size_hint_in_bytes)
            break;

        ret->push_back(entry);
        get_size += entry_size;
    }

    return ret;
}

ptr<std::vector<LogEntryWithVersion>> NuRaftFileLogStore::log_entries_version_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes)
{
    ptr<std::vector<LogEntryWithVersion>> ret = cs_new<std::vector<LogEntryWithVersion>>();

    int64 get_size = 0;

    for (auto i = start; i < end; i++)
    {
        auto entry = entry_at(i);
        if (!entry)
            return nullptr;

        int64 entry_size = entry->get_buf().size() + sizeof(ulong) + sizeof(char);

        if (batch_size_hint_in_bytes > 0 && get_size + entry_size > batch_size_hint_in_bytes)
            break;

        ret->push_back({segment_store->getVersion(i), entry});
        get_size += entry_size;
    }

    return ret;
}

ptr<log_entry> NuRaftFileLogStore::entry_at(ulong index)
{
    auto res = log_queue.getEntry(index);
    if (res)
    {
        LOG_TRACE(log, "Get log {} from queue", index);
    }
    else
    {
        LOG_TRACE(log, "Get log {} from disk", index);
        res = segment_store->getEntry(index);
    }
    return res ? cloneLogEntry(res) : nullptr;
}

ulong NuRaftFileLogStore::term_at(ulong index)
{
    if (entry_at(index))
        return entry_at(index)->get_term();
    return 0;
}

ptr<buffer> NuRaftFileLogStore::pack(ulong index, int32 cnt)
{
    ptr<std::vector<ptr<log_entry>>> entries = log_entries(index, index + cnt);

    std::vector<ptr<buffer>> logs;
    size_t size_total = 0;
    for (const auto & le : *entries)
    {
        ptr<buffer> buf = le->serialize();
        size_total += buf->size();
        logs.push_back(buf);
    }

    ptr<buffer> buf_out = buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
    buf_out->pos(0);
    buf_out->put(cnt);

    for (auto & entry : logs)
    {
        ptr<buffer> & bb = entry;
        buf_out->put(static_cast<int32>(bb->size()));
        buf_out->put(*bb);
    }

    LOG_DEBUG(log, "pack log start {}, count {}", index, cnt);

    return buf_out;
}

void NuRaftFileLogStore::apply_pack(ulong index, buffer & pack)
{
    pack.pos(0);
    int32 num_logs = pack.get_int();

    for (int32 i = 0; i < num_logs; ++i)
    {
        ulong cur_idx = index + i;
        int32 buf_size = pack.get_int();

        ptr<buffer> buf_local = buffer::alloc(buf_size);
        pack.get(buf_local);

        if (cur_idx - segment_store->lastLogIndex() != 1)
            LOG_WARNING(log, "cur_idx {}, segment_store last_log_index {}, difference is not 1", cur_idx, segment_store->lastLogIndex());
        else
            LOG_DEBUG(log, "cur_idx {}, segment_store last_log_index {}", cur_idx, segment_store->lastLogIndex());

        ptr<log_entry> le = log_entry::deserialize(*buf_local);
        segment_store->writeAt(cur_idx, le);
    }

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
        parallel_fsync_event->set();

    LOG_DEBUG(log, "apply pack {}", index);
}

bool NuRaftFileLogStore::compact(ulong last_log_index)
{
    auto removed_count = segment_store->removeSegment(last_log_index + 1);
    log_queue.clear();
    LOG_DEBUG(log, "Compact log to {} and removed {} log segments", last_log_index, removed_count);
    return true;
}

bool NuRaftFileLogStore::flush()
{
    segment_store->flush();
    return true;
}

ulong NuRaftFileLogStore::last_durable_index()
{
    uint64_t last_log = next_slot() - 1;
    if (log_fsync_mode != FsyncMode::FSYNC_PARALLEL)
        return last_log;

    return disk_last_durable_index;
}

}
