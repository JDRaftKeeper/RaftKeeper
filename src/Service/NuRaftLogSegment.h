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
        , file_name(file_name_)
        , create_time(create_time_)
        , seg_fd(-1)
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
        , file_name(file_name_)
        , seg_fd(-1)
        , file_size(0)
        , is_open(false)
        , log(&(Poco::Logger::get("LogSegment")))
    {
    }

    ~NuRaftLogSegment() { }

    // create open segment
    int create();
    // load segment
    int load();
    // close open segment
    int close(bool is_full);
    // remove the segment
    int remove();

    void writeFileHeader();

    off_t loadVersion();

    LogVersion getVersion() const { return version; }

    inline UInt64 flush() const;

    // serialize entry, and append to open segment,return new start index
    UInt64 appendEntry(ptr<log_entry> entry, std::atomic<UInt64> & last_log_index);

    int writeAt(UInt64 index, const ptr<log_entry> entry);

    // get entry by index
    ptr<log_entry> getEntry(UInt64 index);

    // get entry's term by index
    UInt64 getTerm(UInt64 index) const;

    // truncate segment to last_index_kept
    int truncate(UInt64 last_index_kept);

    bool isOpen() const { return is_open; }

    UInt64 getFileSize() const { return file_size.load(std::memory_order_consume); }

    UInt64 firstIndex() const { return first_index; }

    UInt64 lastIndex() const { return last_index.load(std::memory_order_consume); }

    std::string getFileName();

public:
#ifdef __APPLE__
    //log_startindex_endindex_createtime
    static constexpr char LOG_FINISH_FILE_NAME[] = "log_%llu_%llu_%s";
    //log_startindex_open_createtime
    static constexpr char LOG_OPEN_FILE_NAME[] = "log_%llu_open_%s";
#else
    static constexpr char LOG_FINISH_FILE_NAME[] = "log_%lu_%lu_%s";
    static constexpr char LOG_OPEN_FILE_NAME[] = "log_%lu_open_%s";
#endif

private:
    struct LogMeta
    {
        off_t offset;
        size_t length;
        UInt64 term;
    };

    std::string getOpenFileName();
    std::string getOpenPath();
    std::string getFinishFileName();
    std::string getFinishPath();
    std::string getPath();

    int openFile();
    int closeFile();

    //Get log index
    int getMeta(UInt64 index, LogMeta * meta) const;
    int loadHeader(int fd, off_t offset, LogEntryHeader * head) const;
    int loadEntry(int fd, off_t offset, LogEntryHeader * head, ptr<log_entry> & entry) const;

    int truncateMetaAndGetLast(UInt64 last);

private:
    std::string log_dir;
    const UInt64 first_index;
    std::atomic<UInt64> last_index;
    std::string file_name;
    std::string create_time;
    int seg_fd;
    std::atomic<UInt64> file_size;
    bool is_open;
    Poco::Logger * log;
    mutable std::shared_mutex log_mutex;
    //file offset
    std::vector<std::pair<UInt64 /*offset*/, UInt64 /*term*/>> offset_term;
    LogVersion version;
};

// LogSegmentStore use segmented append-only file, all data in disk, all index in memory.
// append one log entry, only cause one disk write, every disk write will call fsync().
//
// SegmentLog layout:
//      log_1_1000_createtime: closed segment
//      log_open_1001_createtime: open segment
class LogSegmentStore
{
public:
    //first index
    typedef std::vector<ptr<NuRaftLogSegment>> SegmentVector;

    LogSegmentStore(const std::string & log_dir_)
        : log_dir(log_dir_), first_log_index(1), last_log_index(0), log(&(Poco::Logger::get("LogSegmentStore")))
    {
        LOG_INFO(log, "Create LogSegmentStore {}.", log_dir_);
    }

    virtual ~LogSegmentStore() { }
    static ptr<LogSegmentStore> getInstance(const std::string & log_dir, bool force_new = false);

    // init log store, check consistency and integrity
    int init(UInt32 max_log_file_size = MAX_LOG_SIZE, UInt32 max_log_segment_count = MAX_SEGMENT_COUNT);
    int close();
    UInt64 flush();

    // first log index in log
    UInt64 firstLogIndex() { return first_log_index.load(std::memory_order_acquire); }

    // last log index in log
    UInt64 lastLogIndex() { return last_log_index.load(std::memory_order_acquire); }
    void setLastLogIndex(UInt64 index) { last_log_index.store(index, std::memory_order_release); }

    // append entry to log
    UInt64 appendEntry(ptr<log_entry> entry);

    UInt64 writeAt(UInt64 index, const ptr<log_entry> entry);

    // get logentry by index
    ptr<log_entry> getEntry(UInt64 index);

    void getEntries(UInt64 start_index, UInt64 end_index, ptr<std::vector<ptr<log_entry>>> & entries);

    void getEntriesExt(UInt64 start_idx, UInt64 end_idx, int64 batch_size_hint_in_bytes, ptr<std::vector<ptr<log_entry>>> & entries);

    // get logentry's term by index
    UInt64 getTerm(UInt64 index);

    // append entries to log and update IOMetric, return success append number
    // int appendEntries(const std::vector<ptr<log_entry>> & entries, IOMetric * metric);

    // truncate old log segment when segment count reach max_segment_count
    int removeSegment();

    // delete segment from storage's head, [1, first_index_kept) will be discarded
    // The log segment to which first_index_kept belongs will not be deleted.
    int removeSegment(UInt64 first_index_kept);

    // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    int truncateLog(UInt64 last_index_kept);

    int reset(UInt64 next_log_index);

    //closed segments, unclude open segment
    SegmentVector & getSegments() { return segments; }

    LogVersion getVersion(UInt64 index);

    //void sync();
public:
    static constexpr UInt32 MAX_LOG_SIZE = 1000 * 1024 * 1024; //1G, 0.3K/Log, 3M logs
    static constexpr UInt32 MAX_SEGMENT_COUNT = 50; //50G
    static constexpr int LOAD_THREAD_NUM = 8;

private:
    int openSegment();
    //for LogSegmentStore init
    void listFiles(std::vector<std::string> & seg_files);
    int listSegments();
    int loadSegments();

    //get LogSegment by log index
    int getSegment(UInt64 log_index, ptr<NuRaftLogSegment> & ptr);

    //for truncate
    /*
    void popSegments(UInt64 first_index_kept, std::vector<ptr<LogSegment>> & popped);
    void popSegmentsFromBack(const UInt64 first_index_kept, std::vector<ptr<LogSegment>> & popped, ptr<LogSegment> & last_segment);
    */

private:
    static ptr<LogSegmentStore> segment_store;
    std::string log_dir;
    std::atomic<UInt64> first_log_index;
    std::atomic<UInt64> last_log_index;
    UInt32 max_log_size;
    UInt32 max_segment_count;
    Poco::Logger * log;
    SegmentVector segments;
    mutable std::shared_mutex seg_mutex;
    ptr<NuRaftLogSegment> open_segment;
    //bool enable_sync;
};

}
