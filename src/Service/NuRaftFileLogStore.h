#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Service/NuRaftLogSegment.h>
#include <Service/Settings.h>
#include <libnuraft/nuraft.hxx>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>

namespace RK
{
using nuraft::int32;
using nuraft::int64;
using nuraft::ulong;

/// Act as a ring cache who will cache latest append logs.
/// TODO make it as a common tool.
class LogEntryQueue
{
public:
    LogEntryQueue() : batch_index(0), max_index(0), log(&(Poco::Logger::get("LogEntryQueue"))) { }

    /// get log from cache, return null if not exists.
    ptr<log_entry> getEntry(const UInt64 & index);

    /// put log into the queue
    void putEntry(UInt64 & index, ptr<log_entry> & entry);

    [[maybe_unused]] void putEntryOrClear(UInt64 & index, ptr<log_entry> & entry);

    /// clean all log
    void clear();

private:
    static constexpr UInt8 BIT_SIZE = 16;
    static constexpr UInt32 MAX_VECTOR_SIZE = 65536; /// 2^16

    /// every max_index log, increase batch_index
    UInt64 batch_index;

    /// queue capacity
    UInt64 max_index;

    /// logs
    ptr<log_entry> entry_vec[MAX_VECTOR_SIZE];

    std::shared_mutex queue_mutex;
    Poco::Logger * log;
};

class NuRaftFileLogStore : public nuraft::log_store
{
    __nocopy__(NuRaftFileLogStore) public : explicit NuRaftFileLogStore(
         const String & log_dir,
         bool force_new = false,
         FsyncMode log_fsync_mode_ = FsyncMode::FSYNC_PARALLEL,
         UInt64 log_fsync_interval_ = 1000,
         UInt32 max_log_size_ = LogSegmentStore::MAX_SEGMENT_FILE_SIZE,
         UInt32 max_segment_count_ = LogSegmentStore::MAX_SEGMENT_COUNT);

    ~NuRaftFileLogStore() override;

    /// The first available slot of the store, starts with 1
    ulong next_slot() const override;

    /// The start index of the log store, at the very beginning, it must be 1.
    /// However, after some compact actions, this could be anything equal to or
    /// greater than or equal to one
    ulong start_index() const override;

    /// Latest log entry in the log store
    ptr<log_entry> last_entry() const override;

    /// Append a log to log store
    ulong append(ptr<log_entry> & entry) override;

    /// Write at index and truncate log whose index large than index
    void write_at(ulong index, ptr<log_entry> & entry) override;

    /**
     * Invoked after a batch of logs is written as a part of
     * a single append_entries request. Usually invoke fsync.
     *
     * @param start The start log index number (inclusive)
     * @param cnt The number of log entries written.
     */
    void end_of_append_batch(ulong start, ulong cnt) override;

    /**
     * Get log entries with index [start, end).
     *
     * Return nullptr to indicate error if any log entry within the requested range
     * could not be retrieved (e.g. due to external log truncation).
     *
     * @param start The start log index number (inclusive).
     * @param end The end log index number (exclusive).
     * @return The log entries between [start, end).
     */
    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override;

    /**
     * (Optional)
     * Get log entries with index [start, end).
     *
     * The total size of the returned entries is limited by batch_size_hint.
     *
     * Return nullptr to indicate error if any log entry within the requested range
     * could not be retrieved (e.g. due to external log truncation).
     *
     * @param start The start log index number (inclusive).
     * @param end The end log index number (exclusive).
     * @param batch_size_hint_in_bytes Total size (in bytes) of the returned entries,
     *        see the detailed comment at
     *        `state_machine::get_next_batch_size_hint_in_bytes()`.
     * @return The log entries between [start, end) and limited by the total size
     *         given by the batch_size_hint_in_bytes.
     */
    ptr<std::vector<ptr<log_entry>>> log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) override;

    /// Same with log_entries_ext, but return log entry with version info.
    ptr<std::vector<VersionLogEntry>> log_entries_version_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0);

    /**
     * Get the log entry at the specified log index number.
     *
     * @param index Should be equal to or greater than 1.
     * @return The log entry or null if index >= this->next_slot().
     */
    ptr<log_entry> entry_at(ulong index) override;

    /**
     * Get the term for the log entry at the specified index.
     * Suggest to stop the system if the index >= this->next_slot()
     *
     * @param index Should be equal to or greater than 1.
     * @return The term for the specified log entry, or
     *         0 if index < this->start_index().
     */
    ulong term_at(ulong index) override;

    /**
     * Pack the given number of log items starting from the given index.
     *
     * @param index The start log index number (inclusive).
     * @param cnt The number of logs to pack.
     * @return Packed (encoded) logs.
     */
    ptr<buffer> pack(ulong index, int32 cnt) override;

    /**
     * Apply the log pack to current log store, starting from index.
     *
     * @param index The start log index number (inclusive).
     * @param pack logs.
     */
    void apply_pack(ulong index, buffer & pack) override;

    /**
     * Compact the log store by purging all log entries,
     * including the given log index number.
     *
     * If current maximum log index is smaller than given `last_log_index`,
     * set start log index to `last_log_index + 1`.
     *
     * @param last_log_index Log index number that will be purged up to (inclusive).
     * @return `true` on success.
     */
    bool compact(ulong last_log_index) override;

    /**
     * Synchronously flush all log entries in this log store to the backing storage
     * so that all log entries are guaranteed to be durable upon process crash.
     *
     * @return `true` on success.
     */
    bool flush() override;

    /**
     * This API is used only when `raft_params::parallel_log_appending_` flag is set.
     * Please refer to the comment of the flag.
     *
     * @return The last durable log index.
     */
    ulong last_durable_index() override;

    void shutdown();

    void setRaftServer(nuraft::ptr<nuraft::raft_server> raft_instance_) { raft_instance = raft_instance_; }

    ptr<LogSegmentStore> segmentStore() const { return segment_store; }

private:
    /// Thread used to flush log, only used in FSYNC_PARALLEL mode
    void fsyncThread();

    Poco::Logger * log;

    /// Used to operate log in the store
    ptr<LogSegmentStore> segment_store;

    /// Memory log cache
    LogEntryQueue log_queue;

    /// last log entry
    ptr<log_entry> last_log_entry;

    /// log flushed mode
    FsyncMode log_fsync_mode;
    UInt64 log_fsync_interval;

    /// How many log to flush, only used in FSYNC_BATCH mode
    UInt64 to_flush_count{0};

    /// Thread used to flush log, only used in FSYNC_PARALLEL mode
    ThreadFromGlobalPool fsync_thread;

    /// last flushed log index, only used in FSYNC_PARALLEL mode
    std::atomic<ulong> disk_last_durable_index;

    /// Used to notify fsync thread to flush log,
    /// only used in FSYNC_PARALLEL mode
    std::shared_ptr<Poco::Event> parallel_fsync_event;

    /// only used when log_fsync_mode is FSYNC_PARALLEL
    nuraft::ptr<nuraft::raft_server> raft_instance;

    std::atomic<bool> shutdown_called{false};
};

}
