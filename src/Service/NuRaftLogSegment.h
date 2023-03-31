#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <vector>
#include <Service/KeeperCommon.h>
#include <Service/LogEntry.h>
#include <Service/proto/Log.pb.h>
#include <libnuraft/basic_types.hxx>
#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>


namespace RK
{
using nuraft::int64;

enum class LogVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ctime mtime
};

/// Attach version to log entry
struct VersionLogEntry
{
    LogVersion version;
    ptr<log_entry> entry;
};

static constexpr auto CURRENT_LOG_VERSION = LogVersion::V1;

class NuRaftLogSegment
{
public:
    NuRaftLogSegment(
        const std::string & log_dir_, UInt64 first_index_, const std::string & file_name_ = "", const std::string & create_time_ = "")
        : log_dir(log_dir_)
        , first_index(first_index_)
        , last_index(first_index_ - 1)
        , seg_fd(-1)
        , file_name(file_name_)
        , create_time(create_time_)
        , file_size(0)
        , is_open(true)
        , log(&(Poco::Logger::get("LogSegment")))
        , version(CURRENT_LOG_VERSION)
    {
    }

    NuRaftLogSegment(const std::string & log_dir_, UInt64 first_index_, UInt64 last_index_, const std::string file_name_ = "")
        : log_dir(log_dir_)
        , first_index(first_index_)
        , last_index(last_index_)
        , seg_fd(-1)
        , file_name(file_name_)
        , file_size(0)
        , is_open(false)
        , log(&(Poco::Logger::get("LogSegment")))
    {
    }

    ~NuRaftLogSegment() = default;

    /// create open segment
    /// return 0 if success
    int create();

    /// load an segment
    /// return 0 if success
    int load();

    /// Close open segment, return 0 if success.
    /// is_full: whether we segment is full, if true,
    /// close full open log segment and rename to
    /// finish file name, or else close ofstream
    int close(bool is_full);

    /// remove the segment
    /// return 0 if success
    int remove();

    /**
     * write segment file header
     *      magic : \0RaftLog 8 bytes
     *      version: version  1 bytes
     */
    void writeFileHeader();

    /// load data format version, return 0 if success.
    size_t loadVersion();

    /// get data format version
    LogVersion getVersion() const { return version; }

    /// flush log, return last flushed log index if success or 0 if failed
    inline UInt64 flush() const;

    /// serialize entry, and append to open segment, return new start index
    UInt64 appendEntry(ptr<log_entry> entry, std::atomic<UInt64> & last_log_index);

    [[maybe_unused]] int writeAt(UInt64 index, const ptr<log_entry> entry);

    /// get entry by index, return null if not exist.
    ptr<log_entry> getEntry(UInt64 index);

    /// get entry's term by index
    UInt64 getTerm(UInt64 index) const;

    /// Truncate segment from tail to last_index_kept
    int truncate(UInt64 last_index_kept);

    bool isOpen() const { return is_open; }

    UInt64 getFileSize() const { return file_size.load(std::memory_order_consume); }

    /// First log index in the segment
    UInt64 firstIndex() const { return first_index; }
    /// Last log index in the segment
    UInt64 lastIndex() const { return last_index.load(std::memory_order_consume); }

    /// Segment file name, see LOG_FINISH_FILE_NAME and LOG_OPEN_FILE_NAME
    std::string getFileName();

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
    /// log entry metadata in segment, when initializing
    /// load all log entry metadata in memory, see offset_term.
    struct LogMeta
    {
        /// offset in segment file
        off_t offset;
        /// calculate by current offset and next log offset
        size_t length;
        /// log term
        UInt64 term;
    };

    /// invoked when create new segment
    std::string getOpenFileName();
    std::string getOpenPath();

    /// when open segment reach log limit,
    /// we should open a new open segment
    /// and move current open segment as
    /// close segment.
    std::string getFinishFileName();
    std::string getFinishPath();

    /// current segment file path
    std::string getPath();

    /// open file by fd, return 0 if success.
    int openFile();

    /// close file, return 0 if success.
    int closeFile();

    /// get log entry meta, return 0 if success.
    int getMeta(UInt64 index, LogMeta * meta) const;

    /// load log entry header
    int loadLogEntryHeader(int fd, off_t offset, LogEntryHeader * header) const;

    /// load log entry
    int loadLogEntry(int fd, off_t offset, LogEntryHeader * head, ptr<log_entry> & entry) const;

    /// segment file directory
    std::string log_dir;

    /// first log index in the segment
    const UInt64 first_index;

    /// last log index in the segment
    std::atomic<UInt64> last_index;

    /// segment file fd
    int seg_fd;

    /// segment file name
    std::string file_name;

    /// segment file create time
    /// TODO use
    std::string create_time;

    /// segment file size
    std::atomic<UInt64> file_size;

    /// open or close
    bool is_open;

    Poco::Logger * log;

    /// global mutex
    mutable std::shared_mutex log_mutex;

    /// log entry metadata, when initializing, load all log entry metadata in memory
    std::vector<std::pair<UInt64 /*offset*/, UInt64 /*term*/>> offset_term;

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
    using SegmentVector = std::vector<ptr<NuRaftLogSegment>>;

    static constexpr UInt32 MAX_SEGMENT_FILE_SIZE = 1000 * 1024 * 1024; //1G, 0.3K/Log, 3M logs
    static constexpr UInt32 MAX_SEGMENT_COUNT = 50; //50G
    static constexpr int LOAD_THREAD_NUM = 8;

    explicit LogSegmentStore(const std::string & log_dir_)
        : log_dir(log_dir_), first_log_index(1), last_log_index(0), log(&(Poco::Logger::get("LogSegmentStore")))
    {
        LOG_INFO(log, "Create LogSegmentStore {}.", log_dir_);
    }

    virtual ~LogSegmentStore() = default;
    static ptr<LogSegmentStore> getInstance(const std::string & log_dir, bool force_new = false);

    /// Init log store, will create dir if not exist, return 0 if success
    int init(UInt32 max_segment_file_size_ = MAX_SEGMENT_FILE_SIZE, UInt32 max_segment_count_ = MAX_SEGMENT_COUNT);

    int close();

    /// flush log, return last flushed log index if success
    UInt64 flush();

    /// first log index in whole log store
    UInt64 firstLogIndex() { return first_log_index.load(std::memory_order_acquire); }

    /// last log index in whole log store
    UInt64 lastLogIndex() { return last_log_index.load(std::memory_order_acquire); }

    void setLastLogIndex(UInt64 index) { last_log_index.store(index, std::memory_order_release); }

    /// append entry to log store
    UInt64 appendEntry(ptr<log_entry> entry);

    /// First truncate log whose index large or equal entry.index,
    /// then append it.
    UInt64 writeAt(UInt64 index, ptr<log_entry> entry);
    ptr<log_entry> getEntry(UInt64 index);

    /// collection entries in [start_index, end_index]
    void getEntries(UInt64 start_index, UInt64 end_index, ptr<std::vector<ptr<log_entry>>> & entries);

    [[maybe_unused]] void
    getEntriesExt(UInt64 start_idx, UInt64 end_idx, int64 batch_size_hint_in_bytes, ptr<std::vector<ptr<log_entry>>> & entries);
    [[maybe_unused]] UInt64 getTerm(UInt64 index);

    /// Remove segments from storage's head, logs in [1, first_index_kept) will be discarded,
    /// usually invoked when compaction.
    int removeSegment();
    int removeSegment(UInt64 first_index_kept);

    /// Delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    int truncateLog(UInt64 last_index_kept);

    int reset(UInt64 next_log_index);

    /// get closed segments
    SegmentVector & getClosedSegments() { return segments; }

    /// get file format version
    LogVersion getVersion(UInt64 index);

private:
    /// open a new segment, invoked when init
    int openSegment();
    /// list segments, invoked when init
    int listSegments();
    /// load listed segments, invoked when init
    int loadSegments();

    /// find segment by log index
    int getSegment(UInt64 log_index, ptr<NuRaftLogSegment> & ptr);

    /// global instance
    static ptr<LogSegmentStore> segment_store;

    /// file log store directory
    std::string log_dir;

    /// log range [first_log_index, last_log_index]
    std::atomic<UInt64> first_log_index;
    std::atomic<UInt64> last_log_index;

    /// max segment file size
    UInt32 max_segment_file_size;

    /// max segment count
    UInt32 max_segment_count;

    Poco::Logger * log;

    /// closed segments
    SegmentVector segments;

    /// open segments
    ptr<NuRaftLogSegment> open_segment;

    /// global mutex
    mutable std::shared_mutex seg_mutex;
};

}
