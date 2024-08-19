#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <Poco/File.h>

#include <Common/ThreadPool.h>

#include <Service/Crc32.h>
#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>
#include <Service/NuRaftLogSegment.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_DOESNT_EXIST;
    extern const int CORRUPTED_LOG;
}

using namespace nuraft;

[[maybe_unused]] int ftruncateUninterrupted(int fd, off_t length)
{
    int rc;
    do
    {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

bool compareSegment(ptr<NuRaftLogSegment> & lhs, ptr<NuRaftLogSegment> & rhs)
{
    return lhs->firstIndex() < rhs->firstIndex();
}

NuRaftLogSegment::NuRaftLogSegment(const String & log_dir_, UInt64 first_index_)
    : log_dir(log_dir_)
    , first_index(first_index_)
    , last_index(first_index_ - 1)
    , is_open(true)
    , log(&(Poco::Logger::get("NuRaftLogSegment")))
    , version(CURRENT_LOG_VERSION)
{
    Poco::DateTime now;
    create_time = Poco::DateTimeFormatter::format(now, "%Y%m%d%H%M%S");

    std::lock_guard write_lock(log_mutex);

    file_name = getOpenFileName();
    String full_path = getOpenPath();

    LOG_INFO(log, "Creating new log segment {}", file_name);

    if (Poco::File(full_path).exists())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Try to create a log segment but file {} already exists.", full_path);

    seg_fd = ::open(full_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (seg_fd == -1)
        throwFromErrno(ErrorCodes::CANNOT_OPEN_FILE, "Fail to create new log segment {}", full_path);
}

NuRaftLogSegment::NuRaftLogSegment(const String & log_dir_, UInt64 first_index_, UInt64 last_index_, const String & file_name_, const String & create_time_)
    : log_dir(log_dir_)
    , first_index(first_index_)
    , last_index(last_index_)
    , file_name(file_name_)
    , create_time(create_time_)
    , log(&(Poco::Logger::get("NuRaftLogSegment")))
{
}

NuRaftLogSegment::NuRaftLogSegment(const String & log_dir_, UInt64 first_index_, const String & file_name_, const String & create_time_)
    : log_dir(log_dir_)
    , first_index(first_index_)
    , last_index(first_index_ - 1)
    , is_open(true)
    , file_name(file_name_)
    , create_time(create_time_)
    , log(&(Poco::Logger::get("NuRaftLogSegment")))
{
}

String NuRaftLogSegment::getOpenFileName()
{
    return fmt::format("log_{}_open_{}", first_index, create_time);
}

String NuRaftLogSegment::getOpenPath()
{
    String path(log_dir);
    path += "/" + getOpenFileName();
    return path;
}

String NuRaftLogSegment::getClosedFileName()
{
    return fmt::format("log_{}_{}_{}", first_index, last_index.load(std::memory_order_relaxed), create_time);
}

String NuRaftLogSegment::getClosedPath()
{
    String path(log_dir);
    path += "/" + getClosedFileName();
    return path;
}

String NuRaftLogSegment::getFileName()
{
    return file_name;
}

String NuRaftLogSegment::getPath()
{
    return log_dir + "/" + getFileName();
}

void NuRaftLogSegment::openFileIfNeeded()
{
    if (seg_fd != -1)
        return;

    LOG_INFO(log, "Opening log segment file {}", file_name);

    String full_path = getPath();
    if (!Poco::File(full_path).exists())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Log segment file {} does not exist.", file_name);

    seg_fd = ::open(full_path.c_str(), O_RDWR);
    if (seg_fd == -1)
        throwFromErrno(ErrorCodes::CANNOT_OPEN_FILE, "Fail to open log segment file {}", file_name);
}

void NuRaftLogSegment::closeFileIfNeeded()
{
    LOG_INFO(log, "Closing log segment file {}", file_name);
    if (seg_fd != -1)
    {
        if (::close(seg_fd) != 0)
            throwFromErrno(ErrorCodes::CANNOT_CLOSE_FILE, "Error when closing a log segment file");
        seg_fd = -1;
    }
}

void NuRaftLogSegment::writeHeader()
{
    if (!is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Log segment {} not open yet", file_name);

    if (seg_fd < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} not open yet", file_name);

    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {0, 'R', 'a', 'f', 't', 'L', 'o', 'g'};
    };

    std::lock_guard write_lock(log_mutex);
    auto version_uint8 = static_cast<uint8_t>(version);

    if (write(seg_fd, &magic_num, 8) != 8)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write magic to {}", file_name);

    if (write(seg_fd, &version_uint8, 1) != 1)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write version to {}", file_name);

    file_size.fetch_add(MAGIC_AND_VERSION_SIZE, std::memory_order_release);
}

void NuRaftLogSegment::load()
{
    openFileIfNeeded();

    /// get file size
    struct stat st_buf;
    if (fstat(seg_fd, &st_buf) != 0)
        throwFromErrno(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Fail to get the stat of log segment file {}", file_name);

    size_t file_size_read = st_buf.st_size;

    /// load header
    readHeader();
    size_t entry_off = version == LogVersion::V0 ? 0 : MAGIC_AND_VERSION_SIZE;

    /// load log entry
    UInt64 last_index_read = first_index - 1;
    for (; entry_off < file_size_read;)
    {
        LogEntryHeader header = loadEntryHeader(entry_off);

        const UInt64 log_entry_len = sizeof(LogEntryHeader) + header.data_length;

        if (entry_off + log_entry_len > file_size_read)
            throw Exception(ErrorCodes::CORRUPTED_LOG, "Corrupted log segment file {}.", file_name);

        offset_term.push_back(std::make_pair(entry_off, header.term));
        ++last_index_read;
        entry_off += log_entry_len;

        if (last_index_read << 20 == 0)
        {
            LOG_DEBUG(
                log,
                "Load log segment {}, entry_off {}, log_entry_len {}, file_size {}, log_index {}",
                file_name,
                entry_off,
                log_entry_len,
                file_size_read,
                last_index_read);
        }
    }

    const UInt64 curr_last_index = last_index.load(std::memory_order_relaxed);

    if (!is_open)
    {
        if (last_index_read != curr_last_index)
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "Corrupted log segment {}, last_index_read {}, last_index {}",
                file_name,
                last_index_read,
                curr_last_index);
    }
    else
    {
        LOG_INFO(log, "Read last log index {} for an open segment {}.", last_index_read, file_name);
        last_index = last_index_read;
    }

    if (entry_off != file_size_read)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_LOG,
            "{} is corrupted, entry_off {} != file_size {}, maybe the last log entry is incomplete.",
            file_name,
            entry_off,
            file_size_read);
        /// ftruncateUninterrupted(seg_fd, entry_off);
    }

    file_size = entry_off;

    /// seek to end of file if it is open
    if (is_open)
        ::lseek(seg_fd, entry_off, SEEK_SET);
}

void NuRaftLogSegment::readHeader()
{
    if (seg_fd < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} not open yet", file_name);

    ptr<buffer> buf = buffer::alloc(MAGIC_AND_VERSION_SIZE);
    buf->pos(0);

    ssize_t size_read = pread(seg_fd, buf->data(), MAGIC_AND_VERSION_SIZE, 0);
    if (size_read != MAGIC_AND_VERSION_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Corrupted log segment file {}.", file_name);

    buffer_serializer bs(buf);
    bs.pos(0);
    uint64_t magic = bs.get_u64();

    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {0, 'R', 'a', 'f', 't', 'L', 'o', 'g'};
    };

    if (magic == magic_num)
    {
        version = static_cast<LogVersion>(bs.get_u8());
    }
    else
    {
        LOG_INFO(log, "{} does not have magic num, its version is V0", file_name);
        version = LogVersion::V0;
    }
}

void NuRaftLogSegment::close(bool is_full)
{
    std::lock_guard write_lock(log_mutex);

    closeFileIfNeeded();

    if (!is_open)
        return;

    if (is_full)
    {
        String old_path = getOpenPath();
        String new_path = getClosedPath();

        LOG_INFO(log, "Closing a full segment {} and rename it to {}.", getOpenFileName(), getClosedFileName());

        Poco::File(old_path).renameTo(new_path);
        file_name = getClosedFileName();
    }

    is_open = false;
}

UInt64 NuRaftLogSegment::flush() const
{
    std::lock_guard write_lock(log_mutex);

    int ret;
#if defined(OS_DARWIN)
    ret = ::fsync(seg_fd);
#else
    ret = ::fdatasync(seg_fd);
#endif
    if (ret == -1)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to flush log segment {}", file_name);

    return last_index;
}

void NuRaftLogSegment::remove()
{
    std::lock_guard write_lock(log_mutex);
    closeFileIfNeeded();
    String full_path = getPath();
    Poco::File f(full_path);
    if (f.exists())
        f.remove();
}

UInt64 NuRaftLogSegment::appendEntry(const ptr<log_entry> & entry, std::atomic<UInt64> & last_log_index)
{
    LogEntryHeader header;
    struct iovec vec[2];

    ptr<buffer> entry_buf;
    char * data_in_buf;
    {
        if (!is_open || seg_fd  == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Append log but segment {} is not open.", file_name);

        entry_buf = LogEntryBody::serialize(entry);
        data_in_buf = reinterpret_cast<char *>(entry_buf->data_begin());

        size_t data_size = entry_buf->size();

        if (data_in_buf == nullptr || data_size == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Append log but it is empty");


        header.term = entry->get_term();
        header.data_length = data_size;
        header.data_crc = RK::getCRC32(data_in_buf, header.data_length);

        vec[0].iov_base = &header;
        vec[0].iov_len = LogEntryHeader::HEADER_SIZE;
        vec[1].iov_base = reinterpret_cast<void *>(data_in_buf);
        vec[1].iov_len = header.data_length;
    }

    {
        std::lock_guard write_lock(log_mutex);
        header.index = last_index.load(std::memory_order_acquire) + 1;
        ssize_t size_written = writev(seg_fd, vec, 2);

        if (size_written != static_cast<ssize_t>(vec[0].iov_len + vec[1].iov_len))
            throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to append log entry to {}", file_name);

        offset_term.push_back(std::make_pair(file_size.load(std::memory_order_relaxed), entry->get_term()));
        file_size.fetch_add(LogEntryHeader::HEADER_SIZE + header.data_length, std::memory_order_release);

        last_index.fetch_add(1, std::memory_order_release);
        last_log_index.store(last_index, std::memory_order_release);
    }

    LOG_TRACE(
        log,
        "Append log term {}, index {}, length {}, crc {}, file {}, entry type {}.",
        header.term,
        header.index,
        header.data_length,
        header.data_crc,
        file_size,
        entry->get_val_type());

    return header.index;
}


ptr<NuRaftLogSegment::LogMeta> NuRaftLogSegment::getMeta(UInt64 index) const
{
    if (last_index == first_index - 1 || index > last_index.load(std::memory_order_relaxed) || index < first_index)
        return nullptr;

    UInt64 meta_index = index - first_index;
    UInt64 file_offset = offset_term[meta_index].first;

    UInt64 next_offset;

    if (index < last_index.load(std::memory_order_relaxed))
        next_offset = offset_term[meta_index + 1].first;
    else
        next_offset = file_size;

    ptr<LogMeta> meta = cs_new<LogMeta>();
    meta->offset = file_offset;
    meta->term = offset_term[meta_index].second;
    meta->length = next_offset - file_offset;

    return meta;
}

LogEntryHeader NuRaftLogSegment::loadEntryHeader(off_t offset) const
{
    ptr<buffer> buf = buffer::alloc(LogEntryHeader::HEADER_SIZE);
    buf->pos(0);

    ssize_t size = pread(seg_fd, buf->data(), LogEntryHeader::HEADER_SIZE, offset);

    if (size != LogEntryHeader::HEADER_SIZE)
        throwFromErrno(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Fail to read header of log segment {}", file_name);

    buffer_serializer bs(buf);
    bs.pos(0);

    LogEntryHeader header;
    header.term = bs.get_u64();
    header.index = bs.get_u64();

    header.data_length = bs.get_u32();
    header.data_crc = bs.get_u32();

    return header;
}

ptr<log_entry> NuRaftLogSegment::loadEntry(const LogMeta & meta) const
{
    LogEntryHeader header = loadEntryHeader(meta.offset);

    ptr<buffer> buf = buffer::alloc(header.data_length);
    ssize_t size_read = pread(seg_fd, buf->data_begin(), header.data_length, meta.offset + LogEntryHeader::HEADER_SIZE);

    if (size_read != header.data_length)
        throwFromErrno(ErrorCodes::CORRUPTED_LOG, "Fail to read log entry with offset {} from log segment {}", meta.offset, file_name);

    if (!verifyCRC32(reinterpret_cast<const char *>(buf->data_begin()), header.data_length, header.data_crc))
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Checking CRC32 failed for log segment {}.", file_name);

    auto entry = LogEntryBody::deserialize(buf);
    entry->set_term(header.term);

    return entry;
}

ptr<log_entry> NuRaftLogSegment::getEntry(UInt64 index)
{
    {
        std::lock_guard write_lock(log_mutex);
        openFileIfNeeded();
    }

    std::shared_lock read_lock(log_mutex);
    auto meta = getMeta(index);
    return meta == nullptr ? nullptr : loadEntry(*meta);
}

bool NuRaftLogSegment::truncate(const UInt64 last_index_kept)
{
    UInt64 file_size_to_keep = 0;
    UInt64 first_log_offset_to_truncate = 0;

    /// Truncate on a full segment need to rename back to open segment again,
    /// because the node may crash before truncate.
    auto reopen_closed_segment = [this]()
    {
        if (!is_open)
        {
            LOG_INFO(
                log,
                "Truncate a closed segment, should re-open it. Current first index {}, last index {}, rename file from {} to {}.",
                first_index,
                last_index,
                getClosedFileName(),
                getOpenFileName());

            closeFileIfNeeded();

            String old_path = getClosedPath();
            String new_path = getOpenPath();

            Poco::File(old_path).renameTo(new_path);
            file_name = getOpenFileName();

            openFileIfNeeded();

            is_open = true;
        }
    };

    {
        std::lock_guard write_lock(log_mutex);
        if (last_index <= last_index_kept)
        {
            LOG_INFO(log, "Log segment {} truncates nothing, last_index {}, last_index_kept {}", file_name, last_index, last_index_kept);
            reopen_closed_segment();
            return false;
        }

        first_log_offset_to_truncate = last_index_kept + 1 - first_index;
        file_size_to_keep = offset_term[first_log_offset_to_truncate].first;

        LOG_INFO(
            log,
            "Truncating {}, offset {}, first_index {}, last_index from {} to {}, truncate_size to {} ",
            file_name,
            first_log_offset_to_truncate,
            first_index,
            last_index,
            last_index_kept,
            file_size_to_keep);
    }

    reopen_closed_segment();

    if (ftruncate(seg_fd, file_size_to_keep) != 0)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to truncate log segment {}", file_name);

    LOG_INFO(log, "Truncate file {} with fd {}, from {} to size {}", file_name, seg_fd, file_size_to_keep, file_size);

    /// seek fd
    off_t ret_off = lseek(seg_fd, file_size_to_keep, SEEK_SET);

    if (ret_off < 0)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to seek to {} for log segment {}", file_size_to_keep, file_name);

    std::lock_guard write_lock(log_mutex);
    offset_term.resize(first_log_offset_to_truncate);
    last_index.store(last_index_kept, std::memory_order_release);
    file_size = file_size_to_keep;

    return true;
}

ptr<LogSegmentStore> LogSegmentStore::getInstance(const String & log_dir_, bool force_new)
{
    static ptr<LogSegmentStore> segment_store;
    if (segment_store == nullptr || force_new)
        segment_store = cs_new<LogSegmentStore>(log_dir_);
    return segment_store;
}

void LogSegmentStore::init(UInt32 max_segment_file_size_)
{
    LOG_INFO(log, "Initializing log segment store, max segment file size {} bytes.", max_segment_file_size_);

    max_segment_file_size = max_segment_file_size_;

    Poco::File(log_dir).createDirectories();

    first_log_index.store(1);
    last_log_index.store(0);

    open_segment = nullptr;

    loadSegmentMetaData();
    loadSegments();
    openNewSegmentIfNeeded();
}

void LogSegmentStore::close()
{
    std::lock_guard write_lock(seg_mutex);

    if (open_segment)
    {
        open_segment->close(false);
        open_segment = nullptr;
    }

    /// When we getEntry from closed segments, we may open it.
    for (auto & segment : closed_segments)
        segment->close(false);
}

UInt64 LogSegmentStore::flush()
{
    std::lock_guard shared_lock(seg_mutex);
    if (open_segment)
        return open_segment->flush();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Flush log segment store failed, open segment is nullptr.");
}

void LogSegmentStore::openNewSegmentIfNeeded()
{
    {
        std::shared_lock read_lock(seg_mutex);
        if (open_segment && open_segment->getFileSize() <= max_segment_file_size && open_segment->getVersion() >= CURRENT_LOG_VERSION)
            return;
    }

    std::lock_guard write_lock(seg_mutex);
    if (open_segment)
    {
        open_segment->close(true);
        closed_segments.push_back(open_segment);
        open_segment = nullptr;
    }

    UInt64 next_idx = last_log_index.load(std::memory_order_acquire) + 1;
    ptr<NuRaftLogSegment> new_seg = cs_new<NuRaftLogSegment>(log_dir, next_idx);

    open_segment = new_seg;
    open_segment->writeHeader();
}

ptr<NuRaftLogSegment> LogSegmentStore::getSegment(UInt64 index)
{
    UInt64 first_index = first_log_index.load(std::memory_order_acquire);
    UInt64 last_index = last_log_index.load(std::memory_order_acquire);

    /// No log
    if (first_index > last_index)
        return nullptr;

    if (index < first_index || index > last_index)
    {
        LOG_WARNING(log, "Attempted to access log {} who is outside of range [{}, {}].", index, first_index, last_index);
        return nullptr;
    }

    ptr<NuRaftLogSegment> seg;
    if (open_segment && index >= open_segment->firstIndex())
    {
        seg = open_segment;
    }
    else
    {
        for (auto & segment : closed_segments)
        {
            if (index >= segment->firstIndex() && index <= segment->lastIndex())
                seg = segment;
        }
    }

    return seg;
}

LogVersion LogSegmentStore::getVersion(UInt64 index)
{
    ptr<NuRaftLogSegment> seg = getSegment(index);
    return seg->getVersion();
}

UInt64 LogSegmentStore::appendEntry(const ptr<log_entry> & entry)
{
    openNewSegmentIfNeeded();
    std::shared_lock read_lock(seg_mutex);
    return open_segment->appendEntry(entry, last_log_index);
}

UInt64 LogSegmentStore::writeAt(UInt64 index, const ptr<log_entry> & entry)
{
    truncateLog(index - 1);
    if (index == lastLogIndex() + 1)
        return appendEntry(entry);

    LOG_WARNING(log, "writeAt log index {} failed, firstLogIndex {}, lastLogIndex {}.", index, firstLogIndex(), lastLogIndex());
    return -1;
}

ptr<log_entry> LogSegmentStore::getEntry(UInt64 index)
{
    std::shared_lock read_lock(seg_mutex);
    ptr<NuRaftLogSegment> seg = getSegment(index);
    if (!seg)
        return nullptr;
    return seg->getEntry(index);
}

void LogSegmentStore::getEntries(UInt64 start_index, UInt64 end_index, ptr<std::vector<ptr<log_entry>>> & entries)
{
    if (entries == nullptr)
    {
        LOG_ERROR(log, "Entry vector is nullptr.");
        return;
    }
    for (UInt64 index = start_index; index <= end_index; index++)
    {
        auto entry_pt = getEntry(index);
        entries->push_back(entry_pt);
    }
}

int LogSegmentStore::removeSegment(UInt64 first_index_kept)
{
    if (first_log_index.load(std::memory_order_acquire) >= first_index_kept)
    {
        LOG_INFO(
            log,
            "Remove 0 log segments, since first_log_index {} >= first_index_kept {}",
            first_log_index.load(std::memory_order_relaxed),
            first_index_kept);
        return 0;
    }

    std::vector<ptr<NuRaftLogSegment>> to_be_removed;
    {
        std::lock_guard write_lock(seg_mutex);

        first_log_index.store(first_index_kept, std::memory_order_release);
        for (auto it = closed_segments.begin(); it != closed_segments.end();)
        {
            ptr<NuRaftLogSegment> & segment = *it;
            if (segment->lastIndex() < first_index_kept)
            {
                to_be_removed.push_back(segment);
                it = closed_segments.erase(it);
            }
            else
            {
                if (segment->firstIndex() < first_log_index)
                {
                    first_log_index.store(segment->firstIndex(), std::memory_order_release);
                    if (last_log_index == 0 || (last_log_index - 1) < first_log_index)
                        last_log_index.store(segment->lastIndex(), std::memory_order_release);
                }
                it++;
            }
        }

        //remove open segment.
        // Because when adding a node, you may directly synchronize the snapshot and do log compaction.
        // At this time, the log of the new node is smaller than the last log index of the snapshot.
        // So remove the open segment.
        if (open_segment)
        {
            if (open_segment->lastIndex() < first_index_kept)
            {
                to_be_removed.push_back(open_segment);
                open_segment = nullptr;
            }
            else if (open_segment->firstIndex() < first_log_index)
            {
                first_log_index.store(open_segment->firstIndex(), std::memory_order_release);
                if (last_log_index == 0 || (last_log_index - 1) < first_log_index)
                    last_log_index.store(open_segment->lastIndex(), std::memory_order_release);
            }
        }
    }

    for (auto & seg : to_be_removed)
    {
        LOG_INFO(log, "Remove log segment, file {}", seg->getFileName());
        seg->remove();
    }

    /// reset last_log_index
    if (last_log_index == 0 || (last_log_index - 1) < first_log_index)
        last_log_index.store(first_log_index - 1, std::memory_order_release);

    return to_be_removed.size();
}

bool LogSegmentStore::truncateLog(UInt64 last_index_kept)
{
    if (last_log_index.load(std::memory_order_acquire) <= last_index_kept)
    {
        LOG_INFO(
            log,
            "Nothing is going to happen since last_log_index {} <= last_index_kept {}",
            last_log_index.load(std::memory_order_relaxed),
            last_index_kept);
        return false;
    }

    std::vector<ptr<NuRaftLogSegment>> to_removed_segments;
    ptr<NuRaftLogSegment> last_segment;

    std::lock_guard write_lock(seg_mutex);
    /// remove finished segment
    for (auto it = closed_segments.begin(); it != closed_segments.end();)
    {
        ptr<NuRaftLogSegment> & segment = *it;
        if (segment->firstIndex() > last_index_kept)
        {
            to_removed_segments.push_back(segment);
            it = closed_segments.erase(it);
        }
        /// Get the segment to last_index_kept belongs
        else if (last_index_kept >= segment->firstIndex() && last_index_kept <= segment->lastIndex())
        {
            last_segment = segment;
            it++;
        }
        else
            it++;
    }

    /// remove open segment if needed
    if (open_segment)
    {
        if (open_segment->firstIndex() > last_index_kept)
        {
            to_removed_segments.push_back(open_segment);
            open_segment = nullptr;
        }
        else if (last_index_kept >= open_segment->firstIndex() && last_index_kept <= open_segment->lastIndex())
        {
            last_segment = open_segment;
        }
    }

    if (!last_segment)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found a segment to truncate, last_index_kept {}.", last_index_kept);

    /// remove files
    for (auto & to_removed : to_removed_segments)
    {
        LOG_INFO(log, "Removing file for segment {}", to_removed->getFileName());
        to_removed->remove();
        to_removed = nullptr;
    }

    bool is_open_before_truncate = last_segment->isOpen();
    bool removed_something = last_segment->truncate(last_index_kept);

    if (!removed_something && last_segment->lastIndex() != last_index_kept)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Truncate log to last_index_kept {}, but nothing removed from log segment {}.", last_index_kept, last_segment->getFileName());

    if (!is_open_before_truncate && !last_segment->isOpen())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Truncate a closed log segment {}, but the truncated log segment is not open.", last_segment->getFileName());

    if (!is_open_before_truncate)
    {
        open_segment = last_segment;
        if (!closed_segments.empty())
            closed_segments.erase(closed_segments.end() - 1);
    }

    last_log_index.store(last_index_kept, std::memory_order_release);
    return true;
}

void LogSegmentStore::loadSegmentMetaData()
{
    Poco::File file_dir(log_dir);
    if (!file_dir.exists())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Log directory {} does not exist.", log_dir);

    std::vector<String> files;
    file_dir.list(files);

    for (const auto & file_name : files)
    {
        if (file_name.find("log_") == String::npos)
            continue;

        LOG_INFO(log, "Find log segment file {}", file_name);

        int match;
        UInt64 first_index = 0;
        UInt64 last_index = 0;
        char create_time[128];

        /// Closed log segment
        match = sscanf(file_name.c_str(), NuRaftLogSegment::LOG_FINISH_FILE_NAME, &first_index, &last_index, create_time);
        if (match == 3)
        {
            ptr<NuRaftLogSegment> segment = cs_new<NuRaftLogSegment>(log_dir, first_index, last_index, file_name, String(create_time));
            closed_segments.push_back(segment);
            continue;
        }

        /// Open log segment
        match = sscanf(file_name.c_str(), NuRaftLogSegment::LOG_OPEN_FILE_NAME, &first_index, create_time);
        if (match == 2)
        {
            if (open_segment)
                throwFromErrno(ErrorCodes::CORRUPTED_LOG, "Find more than one open segment in {}", log_dir);
            open_segment = cs_new<NuRaftLogSegment>(log_dir, first_index, file_name, String(create_time));
            continue;
        }
    }

    std::sort(closed_segments.begin(), closed_segments.end(), compareSegment);

    /// 0 close/open segment
    /// 1 open segment
    /// N close segment + 1 open segment
    if (open_segment)
    {
        if (!closed_segments.empty())
            first_log_index.store((*closed_segments.begin())->firstIndex(), std::memory_order_release);
        else
            first_log_index.store(open_segment->firstIndex(), std::memory_order_release);

        last_log_index.store(open_segment->lastIndex(), std::memory_order_release);
    }

    /// check segment
    /// last_log_index = 0;

    ptr<NuRaftLogSegment> prev_seg;
    ptr<NuRaftLogSegment> segment;

    for (auto it = closed_segments.begin(); it != closed_segments.end();)
    {
        segment = *it;
        LOG_INFO(
            log,
            "first log index {}, last log index {}, current segment first index {}, last index {}",
            first_log_index.load(std::memory_order_relaxed),
            last_log_index.load(std::memory_order_relaxed),
            segment->firstIndex(),
            segment->lastIndex());

        if (segment->firstIndex() > segment->lastIndex())
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "Invalid segment {}, first index {} > last index {}",
                segment->getFileName(),
                segment->firstIndex(),
                segment->lastIndex());

        if (prev_seg && segment->firstIndex() != prev_seg->lastIndex() + 1)
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "Segment {} does not connect correctly, prev segment last index {}, current segment first index {}",
                log_dir,
                prev_seg->lastIndex(),
                segment->firstIndex());

        ++it;
    }

    if (open_segment)
    {
        if (prev_seg && open_segment->firstIndex() != prev_seg->lastIndex() + 1)
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "Open segment does not connect correctly, prev segment last index {}, open segment first index {}",
                prev_seg->lastIndex(),
                open_segment->firstIndex());
    }
}

void LogSegmentStore::loadSegments()
{
    /// 1. Load closed segments in parallel

    size_t thread_num = std::min(closed_segments.size(), LOAD_THREAD_NUM);
    ThreadPool load_thread_pool(thread_num);

    for (size_t thread_id = 0; thread_id < thread_num; thread_id++)
    {
        load_thread_pool.trySchedule([this, thread_id, thread_num]
        {
            Poco::Logger * thread_log = &(Poco::Logger::get("LoadClosedLogSegmentThread#" + std::to_string(thread_id)));
            for (size_t seg_id = 0; seg_id < closed_segments.size(); seg_id++)
            {
                if (seg_id % thread_num == thread_id)
                {
                    ptr<NuRaftLogSegment> segment = closed_segments[seg_id];
                    LOG_INFO(thread_log, "Loading closed segment, first_index {}, last_index {}", segment->firstIndex(), segment->lastIndex());
                    segment->load();
                }
            }
        });
    }

    /// Update last_log_index from closed segments
    if (!closed_segments.empty())
        last_log_index = closed_segments.back()->lastIndex();

    load_thread_pool.wait();

    /// 2. Load open segment

    if (open_segment)
    {
        LOG_INFO(log, "Loading open segment {} ", log_dir, open_segment->getFileName());
        open_segment->load();

        if (first_log_index.load() > open_segment->lastIndex())
        {
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "First log index {} > last index {} of open segment {}",
                first_log_index.load(),
                open_segment->lastIndex(),
                open_segment->getFileName());
        }
        else
        {
            LOG_INFO(log, "The last index of open segment {} is {}", open_segment->lastIndex(), open_segment->getFileName());
            last_log_index.store(open_segment->lastIndex(), std::memory_order_release);
        }
    }

    if (last_log_index == 0)
        last_log_index = first_log_index - 1;
}

}
