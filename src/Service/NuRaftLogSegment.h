#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <shared_mutex>
#include <vector>

#include <Poco/DateTime.h>
#include <Poco/DateTimeFormatter.h>

#include <common/logger_useful.h>
#include <libnuraft/basic_types.hxx>
#include <libnuraft/nuraft.hxx>

#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>


namespace RK
{
using nuraft::int64;

enum class LogVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ctime, mtime, magic and version
};

/// Attach version to log entry
struct LogEntryWithVersion
{
    LogVersion version;
    ptr<log_entry> entry;
};

static constexpr auto CURRENT_LOG_VERSION = LogVersion::V1;

class NuRaftLogSegment
{
public:
    /// For new open segment
    NuRaftLogSegment(const String & log_dir_, UInt64 first_index_);

    /// For existing closed segment
    NuRaftLogSegment(const String & log_dir_, UInt64 first_index_, UInt64 last_index_, const String & file_name_, const String & create_time_);
    /// For existing open segment
    NuRaftLogSegment(const String & log_dir_, UInt64 first_index_, const String & file_name_, const String & create_time_);

    void load();
    inline UInt64 flush() const;

    /// Close an open segment
    /// is_full: whether the segment is full, if true, close full open log segment and rename to finish file name
    void close(bool is_full);
    void remove();

    /**
     * log segment file header
     *      magic : \0RaftLog 8 bytes
     *      version: version  1 bytes
     */
    void writeHeader();
    void readHeader();

    /// get data format version
    LogVersion getVersion() const { return version; }

    /// serialize entry, and append to open segment, return appended log index
    UInt64 appendEntry(const ptr<log_entry> & entry, std::atomic<UInt64> & last_log_index);

    /// get entry by index, return null if not exist.
    ptr<log_entry> getEntry(UInt64 index);

    /// Truncate segment from tail to last_index_kept.
    /// Return true if some logs are removed.
    /// This method will re-open the segment file if it is a closed one.
    bool truncate(UInt64 last_index_kept);

    bool isOpen() const { return is_open; }

    UInt64 getFileSize() const { return file_size.load(std::memory_order_consume); }

    /// First log index in the segment
    UInt64 firstIndex() const { return first_index; }
    /// Last log index in the segment
    UInt64 lastIndex() const { return last_index.load(std::memory_order_consume); }

    /// Segment file name, see LOG_FINISH_FILE_NAME and LOG_OPEN_FILE_NAME
    String getFileName();

#ifdef __APPLE__
    /// log_start_index, log_end_index, create_time
    static constexpr char LOG_FINISH_FILE_NAME[] = "log_%llu_%llu_%s";
    /// log_start_index, create_time
    static constexpr char LOG_OPEN_FILE_NAME[] = "log_%llu_open_%s";
#else
    /// log_start_index, log_end_index, create_time
    static constexpr char LOG_FINISH_FILE_NAME[] = "log_%lu_%lu_%s";
    /// log_start_index, create_time
    static constexpr char LOG_OPEN_FILE_NAME[] = "log_%lu_open_%s";
#endif

private:
    /// invoked when create new segment
    String getOpenFileName();
    String getOpenPath();

    /// when open segment reach log limit, we should open a new open segment
    /// and move current open segment as close segment.
    String getClosedFileName();
    String getClosedPath();

    /// current segment file path
    String getPath();

    /// open file by fd
    void openFileIfNeeded();

    /// close file, throw exception if failed
    void closeFileIfNeeded();

    /// get offset in file for log of index.
    /// return -1 if index out of range.
    int64_t getEntryOffset(UInt64 index) const;

    /// load log entry
    ptr<log_entry> loadEntry(int64_t offset) const;
    LogEntryHeader loadEntryHeader(int64_t offset) const;

    static constexpr size_t MAGIC_AND_VERSION_SIZE = 9;

    /// segment file directory
    String log_dir;

    /// first log index in the segment
    const UInt64 first_index;

    /// last log index in the segment
    std::atomic<UInt64> last_index;

    /// Segment is open or closed, if and only if the segment is open, it can be written.
    /// There is no more than one open segment in the log store.
    std::atomic_bool is_open = false;

    /// Segment file fd, -1 means the file is not open.
    /// All segments files in log store should be open.
    int seg_fd = -1;

    /// segment file name
    String file_name;

    /// segment file create time
    String create_time;

    /// segment file size
    std::atomic<UInt64> file_size = 0;

    Poco::Logger * log;

    /// global mutex
    mutable std::shared_mutex log_mutex;

    /// The file offset for the log of the start_index + index of vector.
    /// Will load all log entry offset in file into memory when starting.
    std::vector<int64_t> offsets;

    /// file format version, default V1
    LogVersion version;
};

/**
 * LogSegmentStore manages log segments and it use segmented append-only file, all data
 * in disk, all index in memory. Append one log entry, only cause one disk write, every
 * disk write will call fsync().
 *
 * SegmentLog file layout:
 *      log_1_1000_create_time: closed segment
 *      log_open_1001_create_time: open segment
 */
class LogSegmentStore
{
public:
    using Segments = std::vector<ptr<NuRaftLogSegment>>;

    static constexpr UInt32 MAX_SEGMENT_FILE_SIZE = 1000 * 1024 * 1024; //1G, 0.3K/Log, 3M logs
    static constexpr size_t LOAD_THREAD_NUM = 8;

    explicit LogSegmentStore(const String & log_dir_)
        : log_dir(log_dir_), first_log_index(1), last_log_index(0), log(&(Poco::Logger::get("LogSegmentStore")))
    {
        LOG_INFO(log, "Create LogSegmentStore {}.", log_dir_);
    }

    virtual ~LogSegmentStore() = default;
    static ptr<LogSegmentStore> getInstance(const String & log_dir, bool force_new = false);

    /// Init log store, will create dir if not exist
    void init(UInt32 max_segment_file_size_ = MAX_SEGMENT_FILE_SIZE);

    void close();
    /// Return last flushed log index
    UInt64 flush();

    /// first log index in whole log store
    UInt64 firstLogIndex() { return first_log_index.load(std::memory_order_acquire); }

    /// last log index in whole log store
    UInt64 lastLogIndex() { return last_log_index.load(std::memory_order_acquire); }

    /// Append entry to log store
    UInt64 appendEntry(const ptr<log_entry> & entry);

    /// First truncate log whose index is large than or equals with index of entry, then append it.
    UInt64 writeAt(UInt64 index, const ptr<log_entry> & entry);
    ptr<log_entry> getEntry(UInt64 index);

    /// Just for test, collection entries in [start_index, end_index]
    void getEntries(UInt64 start_index, UInt64 end_index, ptr<std::vector<ptr<log_entry>>> & entries);

    /// Remove segments from storage's head, logs in [1, first_index_kept) will be discarded, usually invoked when compaction.
    /// return number of segments removed
    int removeSegment(UInt64 first_index_kept);

    /// Delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    /// Return true if some logs are removed
    bool truncateLog(UInt64 last_index_kept);

    /// get closed segments, only for tests
    Segments getClosedSegments()
    {
        std::shared_lock read_lock(seg_mutex);
        return closed_segments;
    }

    /// get file format version
    LogVersion getVersion(UInt64 index);

private:
    /// open a new segment, invoked when init
    void openNewSegmentIfNeeded();
    /// list segments, invoked when init
    void loadSegmentMetaData();
    /// load listed segments, invoked when init
    void loadSegments();

    /// find segment by log index, return null if not found
    ptr<NuRaftLogSegment> getSegment(UInt64 log_index);

    /// file log store directory
    String log_dir;

    /// log range [first_log_index, last_log_index]
    std::atomic<UInt64> first_log_index;
    std::atomic<UInt64> last_log_index;

    /// max segment file size
    UInt32 max_segment_file_size;

    Poco::Logger * log;

    /// closed segments
    Segments closed_segments;

    /// open segments
    ptr<NuRaftLogSegment> open_segment;

    /// global mutex
    mutable std::shared_mutex seg_mutex;
};

}
