#include <Common/setThreadName.h>

#include <Service/NuRaftFileLogStore.h>

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
    std::lock_guard write_lock(queue_mutex);
    LOG_TRACE(log, "put entry {}, index {}, batch {}", index, index & (MAX_VECTOR_SIZE - 1), batch_index);
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
    UInt64 max_log_segment_file_size_,
    LogEntryCodec write_codec_,
    bool defer_init)
    : log_fsync_mode(log_fsync_mode_), log_fsync_interval(log_fsync_interval_), log(&Poco::Logger::get("FileLogStore"))
{
    segment_store = LogSegmentStore::getInstance(log_dir, force_new, max_log_segment_file_size_, write_codec_);
    if (!defer_init)
    {
        segment_store->scan();
        recovery_prepared = true;
        init();
    }
}

void NuRaftFileLogStore::prepareRecovery(const LogRecoveryContext & recovery)
{
    if (initialized)
        throw std::logic_error("Cannot prepare recovery on an initialized log store");
    recovery_prepared = false;
    segment_store->scan(recovery);
    recovery_prepared = true;
}

void NuRaftFileLogStore::init()
{
    if (initialized)
        throw std::logic_error("Log store is already initialized");
    if (!recovery_prepared)
        throw std::logic_error("Log recovery must be validated before initialization");
    segment_store->finishRecovery();

    if (segment_store->lastLogIndex() < 1)
        /// no log entry exists, return a dummy constant entry with value set to null and term set to  zero
        last_log_entry = cs_new<log_entry>(0, nuraft::buffer::alloc(0));
    else
        last_log_entry = segment_store->getEntry(segment_store->lastLogIndex());

    disk_last_durable_index = segment_store->lastLogIndex();

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
        parallel_fsync_event = std::make_shared<Poco::Event>();

    try
    {
        compaction_thread = ThreadFromGlobalPool([this] { compactionThread(); });
        if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
            fsync_thread = ThreadFromGlobalPool([this] { fsyncThread(); });
        initialized = true;
    }
    catch (...)
    {
        shutdown();
        throw;
    }
}

void NuRaftFileLogStore::shutdown()
{
    {
        std::lock_guard lock(compaction_mutex);
        if (shutdown_called.exchange(true))
            return;
    }
    compaction_cv.notify_all();
    if (compaction_thread.joinable())
        compaction_thread.join();

    if (parallel_fsync_event)
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

        if (UInt64 last_flush_index = segment_store->flush())
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
    if (start_index() < next_slot() && last_log_entry)
        return cloneLogEntry(last_log_entry);
    return cs_new<log_entry>(0, nuraft::buffer::alloc(0));
}

ulong NuRaftFileLogStore::append(ptr<log_entry> & entry)
{
    const ptr<log_entry> cloned = cloneLogEntry(entry);
    UInt64 log_index = segment_store->appendEntry(entry);
    log_queue.putEntry(log_index, cloned);

    last_log_entry = cloned;

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL && entry->get_val_type() != log_val_type::app_log)
        parallel_fsync_event->set();

    return log_index;
}

void NuRaftFileLogStore::write_at(ulong index, ptr<log_entry> & entry)
{
    segment_store->writeAt(index, entry);

    log_queue.clear();
    last_log_entry = entry;

    /// log store file fsync
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
    /// Compaction only removes a prefix, so retained cache entries remain valid.
    if (index < start_index() || index >= next_slot())
        return nullptr;

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
    if (auto entry = entry_at(index))
        return entry->get_term();
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
    /// Applying a pack may replace retained entries; prefix compaction no longer clears this cache.
    log_queue.clear();
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
        last_log_entry = le;
    }

    if (log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
        parallel_fsync_event->set();

    LOG_DEBUG(log, "apply pack {}", index);
}

bool NuRaftFileLogStore::compact(ulong last_log_index)
{
    /// Like ClickHouse Keeper: synchronous logical completion, not an unlink barrier.
    compact_async(last_log_index, {});
    return true;
}

void NuRaftFileLogStore::compact_async(ulong last_log_index, const async_result<bool>::handler_type & when_done)
{
    {
        std::lock_guard lock(compaction_mutex);
        if (!initialized || shutdown_called)
            throw std::logic_error("Log compaction requested before initialization or after shutdown");

        /// Allocate/copy the callback before changing segment ownership.
        compaction_jobs.push_back({{}, when_done});
        try
        {
            compaction_jobs.back().segments = segment_store->detachSegments(last_log_index + 1);
        }
        catch (...)
        {
            compaction_jobs.pop_back();
            throw;
        }
        if (!compaction_jobs.back().segments.empty())
        {
            compaction_cv.notify_all();
            return;
        }
        compaction_jobs.pop_back();
    }
    if (when_done)
    {
        bool result = true;
        ptr<std::exception> error;
        when_done(result, error);
    }
}

void NuRaftFileLogStore::setRetentionBoundary(UInt64 oldest_snapshot_index)
{
    std::lock_guard lock(compaction_mutex);
    if (!initialized || shutdown_called)
        throw std::logic_error("Cannot publish snapshot retention to a stopped log store");
    compaction_jobs.push_back({});
    try
    {
        compaction_jobs.back().segments = segment_store->setRetentionBoundary(oldest_snapshot_index);
    }
    catch (...)
    {
        compaction_jobs.pop_back();
        throw;
    }
    if (compaction_jobs.back().segments.empty())
        compaction_jobs.pop_back();
    else
        compaction_cv.notify_all();
}

void NuRaftFileLogStore::waitForCleanup()
{
    std::unique_lock lock(compaction_mutex);
    compaction_cv.wait(lock, [this] { return compaction_jobs.empty() && !compaction_active; });
}

void NuRaftFileLogStore::compactionThread()
{
    setThreadName("LogCompaction");
    std::vector<RetrySegment> retries;
    while (true)
    {
        CompactionJob job;
        std::optional<RetrySegment> retry;
        {
            std::unique_lock lock(compaction_mutex);
            while (compaction_jobs.empty())
            {
                if (shutdown_called)
                    return; /// Failed files remain on disk and will be rediscovered at startup.
                auto next = std::min_element(
                    retries.begin(), retries.end(), [](const auto & lhs, const auto & rhs) { return lhs.next_attempt < rhs.next_attempt; });
                if (next == retries.end())
                    compaction_cv.wait(lock);
                else if (next->next_attempt <= std::chrono::steady_clock::now())
                {
                    retry = std::move(*next);
                    retries.erase(next);
                    break;
                }
                else
                    compaction_cv.wait_until(lock, next->next_attempt);
            }
            if (!retry)
            {
                job = std::move(compaction_jobs.front());
                compaction_jobs.pop_front();
            }
            compaction_active = true;
        }

        bool result = true;
        ptr<std::exception> error;
        auto remove = [&](const ptr<NuRaftLogSegment> & segment)
        {
            try
            {
                segment->remove();
                LOG_DEBUG(log, "Reclaimed log segment {}", segment->getFileName());
                return true;
            }
            catch (...)
            {
                result = false;
                if (!error)
                    error = std::make_shared<std::runtime_error>(getCurrentExceptionMessage(false));
                tryLogCurrentException(log, "Log segment reclamation failed; retaining it for background retry");
                return false;
            }
        };

        if (retry)
        {
            if (!remove(retry->segment))
            {
                retry->delay = std::min(retry->delay * 2, std::chrono::seconds(60));
                retry->next_attempt = std::chrono::steady_clock::now() + retry->delay;
                retries.push_back(std::move(*retry));
            }
        }
        else
        {
            for (const auto & segment : job.segments)
            {
                if (!remove(segment))
                    retries.push_back({segment, std::chrono::steady_clock::now() + std::chrono::seconds(1), std::chrono::seconds(1)});
            }
            if (job.when_done)
            {
                try
                {
                    job.when_done(result, error);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Log compaction callback failed");
                }
            }
        }
        if (!result)
            LOG_WARNING(log, "{} log segments pending reclamation retry", retries.size());
        {
            std::lock_guard lock(compaction_mutex);
            compaction_active = false;
        }
        compaction_cv.notify_all();
    }
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
