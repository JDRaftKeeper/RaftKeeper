#include <charconv>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <Poco/File.h>

#include <Common/ThreadPool.h>
#include <common/scope_guard.h>

#include <Service/Crc32.h>
#include <Service/KeeperCommon.h>
#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/ZstdLogCodec.h>


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
    extern const int INVALID_LOG_SEGMENT_FILE_NAME;
}

using namespace nuraft;

bool compareSegment(const ptr<NuRaftLogSegment> & lhs, const ptr<NuRaftLogSegment> & rhs)
{
    return lhs->firstIndex() < rhs->firstIndex();
}

NuRaftLogSegment::NuRaftLogSegment(const String & log_dir_, UInt64 first_index_, LogEntryCodec write_codec_)
    : log_dir(log_dir_)
    , first_index(first_index_)
    , last_index(first_index_ - 1)
    , is_open(true)
    , version(CURRENT_LOG_VERSION)
    , write_codec(write_codec_)
    , log(&(Poco::Logger::get("NuRaftLogSegment")))
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
    , version(LogVersion::UNKNOWN)
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
    , version(LogVersion::UNKNOWN)
    , log(&(Poco::Logger::get("NuRaftLogSegment")))
{
}

NuRaftLogSegment::~NuRaftLogSegment()
{
    try
    {
        if (seg_fd != -1)
            closeFileIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to close log segment");
    }
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

void NuRaftLogSegment::openFileIfNeeded(bool writable)
{
    if (seg_fd != -1)
        return;

    LOG_INFO(log, "Opening log segment file {}", file_name);

    String full_path = getPath();
    if (!Poco::File(full_path).exists())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Log segment file {} does not exist.", file_name);

    seg_fd = ::open(full_path.c_str(), writable ? O_RDWR : O_RDONLY);
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

void NuRaftLogSegment::load(bool repair_tail)
{
    openFileIfNeeded(repair_tail);
    offsets.clear();

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
        /// If fewer bytes remain than a full header, treat as partial-entry trailing garbage:
        /// loadEntryHeader would otherwise short-read and surface a misleading "fail to read header" error.
        const UInt64 bytes_remaining = file_size_read - entry_off;
        if (bytes_remaining < LogEntryHeader::HEADER_SIZE)
        {
            if (!is_open)
                throw Exception(
                    ErrorCodes::CORRUPTED_LOG,
                    "Closed log segment {} is corrupted: trailing {} bytes (less than header) at offset {}.",
                    file_name,
                    bytes_remaining,
                    entry_off);

            LOG_WARNING(
                log,
                "{} has trailing {} bytes (less than header size {}) at offset {}, truncating.",
                file_name,
                bytes_remaining,
                LogEntryHeader::HEADER_SIZE,
                entry_off);
            if (repair_tail && ftruncate(seg_fd, entry_off) != 0)
                throwFromErrno(ErrorCodes::CORRUPTED_LOG, "Failed to truncate trailing partial header in {}", file_name);
            break;
        }

        LogEntryHeader header = loadEntryHeader(entry_off);

        const UInt64 log_entry_len = sizeof(LogEntryHeader) + header.data_length;

        if (entry_off + log_entry_len > file_size_read)
        {
            /// A closed segment with a truncated tail means real disk corruption,
            /// not a crash mid-write (writes only target the open segment).
            if (!is_open)
                throw Exception(
                    ErrorCodes::CORRUPTED_LOG,
                    "Closed log segment {} is corrupted: incomplete entry at offset {}, file size {}, would need {} bytes.",
                    file_name,
                    entry_off,
                    file_size_read,
                    entry_off + log_entry_len);

            /// The last log entry in the open segment is incomplete — the server crashed during a write.
            /// Truncate the partial entry; it was never committed, so skipping it
            /// is safe (same behavior as ZooKeeper's FileTxnIterator).
            LOG_WARNING(
                log,
                "{} has an incomplete tail entry at offset {}, truncating (file size {}, would need {} bytes).",
                file_name,
                entry_off,
                file_size_read,
                entry_off + log_entry_len);
            if (repair_tail && ftruncate(seg_fd, entry_off) != 0)
                throwFromErrno(ErrorCodes::CORRUPTED_LOG, "Failed to truncate incomplete tail entry in {}", file_name);
            break;
        }

        if (header.index != last_index_read + 1)
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "Nonconsecutive index {} in segment {}, expected {}",
                header.index,
                file_name,
                last_index_read + 1);
        offsets.push_back(entry_off);
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

    /// After the loop, entry_off should equal the file size for a clean segment.
    /// The only case where they differ is an incomplete tail entry (crash during write),
    /// which was already handled with ftruncate + break inside the loop above.
    file_size = entry_off;

    /// seek to end of file if it is open
    if (is_open)
        ::lseek(seg_fd, entry_off, SEEK_SET);
}

void NuRaftLogSegment::seal()
{
    std::lock_guard lock(log_mutex);
    is_open = false;
}

void NuRaftLogSegment::resumeWriting()
{
    std::lock_guard lock(log_mutex);
    closeFileIfNeeded();
    openFileIfNeeded();
    /// Only the selected active tail may be repaired, after recovery validation succeeds.
    if (ftruncate(seg_fd, file_size.load()) != 0 || lseek(seg_fd, file_size.load(), SEEK_SET) < 0)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot resume log segment {}", file_name);
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
    ptr<buffer> on_disk_buf; /// [codec:1][body], written verbatim to file
    char * data_in_buf;
    {
        if (!is_open || seg_fd  == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Append log but segment {} is not open.", file_name);

        entry_buf = LogEntryBody::serialize(entry);
        char * raw_body = reinterpret_cast<char *>(entry_buf->data_begin());
        size_t raw_body_size = entry_buf->size();

        if (raw_body == nullptr || raw_body_size == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Append log but it is empty");

        LogEntryCodec used_codec = LogEntryCodec::RAW;
        ptr<buffer> compressed;
        if (write_codec == LogEntryCodec::ZSTD)
        {
            compressed = ZstdLogCodec::compress(raw_body, raw_body_size);
            /// Fall back to raw if compression grew the payload (unusual but possible for tiny entries).
            if (compressed && compressed->size() < raw_body_size)
                used_codec = LogEntryCodec::ZSTD;
            else
                compressed = nullptr;
        }

        if (used_codec == LogEntryCodec::ZSTD)
        {
            on_disk_buf = buffer::alloc(1 + compressed->size());
            *on_disk_buf->data_begin() = static_cast<uint8_t>(used_codec);
            memcpy(on_disk_buf->data_begin() + 1, compressed->data_begin(), compressed->size());
        }
        else
        {
            on_disk_buf = buffer::alloc(1 + raw_body_size);
            *on_disk_buf->data_begin() = static_cast<uint8_t>(LogEntryCodec::RAW);
            memcpy(on_disk_buf->data_begin() + 1, raw_body, raw_body_size);
        }

        data_in_buf = reinterpret_cast<char *>(on_disk_buf->data_begin());
        header.term = entry->get_term();
        header.data_length = on_disk_buf->size();
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

        offsets.push_back(file_size.load(std::memory_order_relaxed));
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
        file_size.load(),
        toString(entry->get_val_type()));

    return header.index;
}


int64_t NuRaftLogSegment::getEntryOffset(UInt64 index) const
{
    if (last_index == first_index - 1 || index > last_index.load(std::memory_order_relaxed) || index < first_index)
        return -1;

    UInt64 inner_index = index - first_index;
    return offsets[inner_index];
}

LogEntryHeader NuRaftLogSegment::loadEntryHeader(int64_t offset) const
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

ptr<log_entry> NuRaftLogSegment::loadEntry(int64_t offset) const
{
    LogEntryHeader header = loadEntryHeader(offset);

    if (header.data_length == 0)
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Zero-length payload in log segment {} at offset {}", file_name, offset);

    ptr<buffer> buf = buffer::alloc(header.data_length);
    ssize_t size_read = pread(seg_fd, buf->data_begin(), header.data_length, offset + LogEntryHeader::HEADER_SIZE);

    if (size_read != static_cast<ssize_t>(header.data_length))
        throwFromErrno(ErrorCodes::CORRUPTED_LOG, "Fail to read log entry with offset {} from log segment {}", offset, file_name);

    if (!verifyCRC32(reinterpret_cast<const char *>(buf->data_begin()), header.data_length, header.data_crc))
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Checking CRC32 failed for log segment {}.", file_name);

    ptr<buffer> body;
    if (version >= LogVersion::V2)
    {
        /// First byte is the codec tag; body follows.
        auto codec = static_cast<LogEntryCodec>(*buf->data_begin());
        const char * payload = reinterpret_cast<const char *>(buf->data_begin()) + 1;
        size_t payload_size = header.data_length - 1;

        if (codec == LogEntryCodec::ZSTD)
            body = ZstdLogCodec::decompress(payload, payload_size);
        else
        {
            body = buffer::alloc(payload_size);
            memcpy(body->data_begin(), payload, payload_size);
            body->pos(0);
        }
    }
    else
    {
        /// V0/V1: raw body, no codec byte.
        body = buf;
    }

    auto entry = LogEntryBody::deserialize(body);
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
    auto offset = getEntryOffset(index);
    return offset == -1 ? nullptr : loadEntry(offset);
}

bool NuRaftLogSegment::truncate(const UInt64 last_index_kept)
{
    UInt64 file_size_to_keep;
    UInt64 first_log_offset_to_truncate;

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
                last_index.load(),
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
            LOG_INFO(log, "Log segment {} truncates nothing, last_index {}, last_index_kept {}", file_name, last_index.load(), last_index_kept);
            reopen_closed_segment();
            return false;
        }

        first_log_offset_to_truncate = last_index_kept + 1 - first_index;
        file_size_to_keep = offsets[first_log_offset_to_truncate];

        LOG_INFO(
            log,
            "Truncating {}, offset {}, first_index {}, last_index from {} to {}, truncate_size to {} ",
            file_name,
            first_log_offset_to_truncate,
            first_index,
            last_index.load(),
            last_index_kept,
            file_size_to_keep);
    }

    reopen_closed_segment();

    if (ftruncate(seg_fd, file_size_to_keep) != 0)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to truncate log segment {}", file_name);

    LOG_INFO(log, "Truncate file {} with fd {}, from {} to size {}", file_name, seg_fd, file_size_to_keep, file_size.load());

    /// seek fd
    off_t ret_off = lseek(seg_fd, file_size_to_keep, SEEK_SET);

    if (ret_off < 0)
        throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to seek to {} for log segment {}", file_size_to_keep, file_name);

    std::lock_guard write_lock(log_mutex);
    offsets.resize(first_log_offset_to_truncate);
    last_index.store(last_index_kept, std::memory_order_release);
    file_size = file_size_to_keep;

    return true;
}

ptr<LogSegmentStore> LogSegmentStore::getInstance(const String & log_dir_, bool force_new, UInt32 max_log_segment_file_size_, LogEntryCodec write_codec_)
{
    static ptr<LogSegmentStore> segment_store;
    if (segment_store == nullptr || force_new)
        segment_store = cs_new<LogSegmentStore>(log_dir_, max_log_segment_file_size_, write_codec_);
    return segment_store;
}

void LogSegmentStore::init()
{
    scan();
    finishRecovery();
}

void LogSegmentStore::finishRecovery()
{
    Poco::File(log_dir).createDirectories();
    if (open_segment)
        open_segment->resumeWriting();
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
    /// Compaction can detach the open segment; there is nothing to flush until the next append.
    return 0;
}

void LogSegmentStore::openNewSegmentIfNeeded()
{
    {
        std::shared_lock read_lock(seg_mutex);
        if (open_segment && open_segment->getFileSize() <= max_log_segment_file_size && open_segment->getVersion() >= CURRENT_LOG_VERSION)
            return;
    }

    std::lock_guard write_lock(seg_mutex);
    if (open_segment && open_segment->getFileSize() <= max_log_segment_file_size && open_segment->getVersion() >= CURRENT_LOG_VERSION)
        return;

    if (open_segment)
    {
        open_segment->close(true);
        closed_segments.push_back(open_segment);
        open_segment = nullptr;
    }

    UInt64 next_idx = last_log_index.load(std::memory_order_acquire) + 1;
    ptr<NuRaftLogSegment> new_seg = cs_new<NuRaftLogSegment>(log_dir, next_idx, write_codec);

    open_segment = new_seg;
    open_segment->writeHeader();
}

ptr<NuRaftLogSegment> LogSegmentStore::getSegment(UInt64 index) const
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
        for (const auto & segment : closed_segments)
        {
            if (index >= segment->firstIndex() && index <= segment->lastIndex())
                seg = segment;
        }
    }

    return seg;
}

LogVersion LogSegmentStore::getVersion(UInt64 index)
{
    std::shared_lock read_lock(seg_mutex);
    ptr<NuRaftLogSegment> seg = getSegment(index);
    return seg ? seg->getVersion() : LogVersion::UNKNOWN;
}

UInt64 LogSegmentStore::appendEntry(const ptr<log_entry> & entry)
{
    while (true)
    {
        openNewSegmentIfNeeded();
        std::shared_lock read_lock(seg_mutex);
        /// Compaction may have detached the open segment between the two lock acquisitions.
        if (open_segment)
            return open_segment->appendEntry(entry, last_log_index);
    }
}

void LogSegmentStore::writeAt(UInt64 index, const ptr<log_entry> & entry)
{
    truncateLog(index - 1);
    if (index == lastLogIndex() + 1)
        appendEntry(entry);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fail to write log at {}, log store index range [{}, {}].", index, firstLogIndex(), lastLogIndex());
}

ptr<log_entry> LogSegmentStore::getEntry(UInt64 index) const
{
    std::shared_lock read_lock(seg_mutex);
    ptr<NuRaftLogSegment> seg = getSegment(index);
    if (!seg)
        return nullptr;
    return seg->getEntry(index);
}

std::vector<ptr<log_entry>> LogSegmentStore::getEntries(UInt64 start_index, UInt64 end_index) const
{
    std::vector<ptr<log_entry>> entries;
    for (UInt64 index = start_index; index <= end_index; index++)
    {
        auto entry_pt = getEntry(index);
        entries.push_back(entry_pt);
    }
    return entries;
}

int LogSegmentStore::removeSegment(UInt64 first_index_kept)
{
    auto segments = detachSegments(first_index_kept);
    for (auto & segment : segments)
        segment->remove();
    return segments.size();
}

LogSegmentStore::Segments LogSegmentStore::detachObsoleteSegments()
{
    const auto boundary = std::min(first_log_index.load(std::memory_order_relaxed), retention_boundary);
    auto end = std::find_if(
        closed_segments.begin(), closed_segments.end(), [boundary](const auto & segment) { return segment->lastIndex() >= boundary; });
    Segments removed(closed_segments.begin(), end);
    closed_segments.erase(closed_segments.begin(), end);
    return removed;
}

LogSegmentStore::Segments LogSegmentStore::detachSegments(UInt64 first_index_kept)
{
    std::lock_guard lock(seg_mutex);
    if (first_index_kept > first_log_index.load(std::memory_order_relaxed))
    {
        if (open_segment && open_segment->lastIndex() < first_index_kept && open_segment->firstIndex() < first_index_kept)
        {
            /// The file may still be needed by an older snapshot. Retirement changes only memory,
            /// and its original open-file name is understood by snapshot-aware recovery.
            closed_segments.push_back(open_segment);
            open_segment->seal();
            open_segment.reset();
        }
        if (last_log_index.load(std::memory_order_relaxed) < first_index_kept)
            last_log_index.store(first_index_kept - 1, std::memory_order_release);
        first_log_index.store(first_index_kept, std::memory_order_release);
    }
    return detachObsoleteSegments();
}

LogSegmentStore::Segments LogSegmentStore::setRetentionBoundary(UInt64 oldest_snapshot_index)
{
    std::lock_guard lock(seg_mutex);
    /// New completed snapshots and successful retention pruning can only move this forward.
    if (oldest_snapshot_index < retention_boundary)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Snapshot retention boundary cannot move backwards");
    retention_boundary = oldest_snapshot_index;
    return detachObsoleteSegments();
}

bool LogSegmentStore::truncateLog(UInt64 last_index_kept)
{
    if (last_index_kept + 1 < firstLogIndex())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot truncate below the logical log boundary {}", firstLogIndex());
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
        else if (
            segment->lastIndex() >= first_log_index.load(std::memory_order_relaxed) && last_index_kept >= segment->firstIndex()
            && last_index_kept <= segment->lastIndex())
        {
            last_segment = segment;
            ++it;
        }
        else
            ++it;
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

    /// remove files
    for (auto & to_removed : to_removed_segments)
    {
        LOG_INFO(log, "Removing file for segment {}", to_removed->getFileName());
        to_removed->remove();
        to_removed = nullptr;
    }

    if (last_segment)
    {
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
    }

    last_log_index.store(last_index_kept, std::memory_order_release);
    return true;
}

bool parseSegmentFileName(const String & file_name, UInt64 & first_index, UInt64 & last_index, String & create_time, bool & is_open)
{
    auto tryReadUInt64Text = [] (const String & str, UInt64 & num)
    {
        auto [_, ec] = std::from_chars(str.data(), str.data() + str.size(), num);
        return ec == std::errc();
    };

    Strings tokens;
    splitInto<'_'>(tokens, file_name);

    if (tokens.size() != 4)
        return false;

    if (!tryReadUInt64Text(tokens[1], first_index))
        return false;

    if (tokens[2] == "open")
    {
        is_open = true;
    }
    else
    {
        is_open = false;
        if (!tryReadUInt64Text(tokens[2], last_index))
            return false;
    }

    create_time = std::move(tokens[3]);

    return true;
}

void LogSegmentStore::scan(const std::optional<LogRecoveryContext> & recovery)
{
    /// The experimental marker was never released. Silently ignoring it could change recovery.
    if (Poco::File(log_dir + "/compacted_to").exists() || Poco::File(log_dir + "/compacted_to.tmp").exists())
        throw Exception(
            ErrorCodes::CORRUPTED_LOG,
            "Unsupported experimental compacted_to metadata in {}. Recover the test directory with its original build; do not discard the "
            "marker.",
            log_dir);

    Segments segments;
    std::vector<String> files;
    if (Poco::File(log_dir).exists())
        Poco::File(log_dir).list(files);
    for (const auto & file : files)
    {
        if (!file.starts_with("log_"))
            continue;
        UInt64 first_index = 0;
        UInt64 last_index = 0;
        String create_time;
        bool is_open_segment = false;
        if (!parseSegmentFileName(file, first_index, last_index, create_time, is_open_segment) || first_index == 0)
            throw Exception(ErrorCodes::INVALID_LOG_SEGMENT_FILE_NAME, "Invalid log segment name {}", file);
        if (is_open_segment)
            segments.push_back(cs_new<NuRaftLogSegment>(log_dir, first_index, file, create_time));
        else
        {
            if (last_index < first_index)
                throw Exception(ErrorCodes::CORRUPTED_LOG, "Invalid index range in {}", file);
            segments.push_back(cs_new<NuRaftLogSegment>(log_dir, first_index, last_index, file, create_time));
        }
    }
    std::sort(segments.begin(), segments.end(), compareSegment);

    const UInt64 snapshot_index = recovery ? recovery->snapshot_index : 0;
    const UInt64 wanted = recovery && snapshot_index >= recovery->logs_to_keep ? snapshot_index - recovery->logs_to_keep + 1 : 1;
    Segments readable;
    for (const auto & segment : segments)
    {
        if (!segment->isOpen() && segment->lastIndex() < wanted)
            continue;
        try
        {
            segment->load(false);
            readable.push_back(segment);
        }
        catch (...)
        {
            /// A known closed range below the chosen snapshot is not required for recovery.
            /// Keep the file for conservative retention, but never expose its damaged entries.
            if (!recovery || segment->isOpen() || segment->lastIndex() > snapshot_index)
                throw;
            tryLogCurrentException(log, "Ignoring damaged historical segment covered by the selected snapshot");
        }
    }

    UInt64 covered = snapshot_index;
    if (!recovery && !readable.empty())
        covered = readable.front()->firstIndex() - 1;
    for (const auto & segment : readable)
    {
        if (segment->lastIndex() < segment->firstIndex() || segment->lastIndex() <= snapshot_index)
            continue;
        const auto required_first = std::max(segment->firstIndex(), snapshot_index + 1);
        if (required_first != covered + 1)
            throw Exception(
                ErrorCodes::CORRUPTED_LOG,
                "Log gap or overlap after snapshot {}: expected {}, found {} in {}",
                snapshot_index,
                covered + 1,
                required_first,
                segment->getFileName());
        if (recovery)
        {
            /// Validate the required suffix, including checksums and entry framing, before
            /// repairing any tail or allowing physical cleanup. Snapshot-covered history
            /// need not be decoded just to recover a newer snapshot.
            for (UInt64 index = required_first; index <= segment->lastIndex(); ++index)
                if (!segment->getEntry(index))
                    throw Exception(ErrorCodes::CORRUPTED_LOG, "Missing required log {} in {}", index, segment->getFileName());
        }
        covered = segment->lastIndex();
    }
    if (recovery && covered < recovery->committed_index)
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Logs end at {}, before committed index {}", covered, recovery->committed_index);

    /// Retain only a contiguous, unambiguous suffix in NuRaft's visible range.
    /// Earlier physical files remain available for recovery from an older snapshot.
    UInt64 visible_start = covered + 1;
    UInt64 previous = covered;
    for (auto it = readable.rbegin(); it != readable.rend(); ++it)
    {
        const auto & segment = *it;
        if (segment->lastIndex() < segment->firstIndex())
            continue;
        if (segment->lastIndex() != previous)
        {
            if (segment->lastIndex() > previous)
                visible_start = std::max(visible_start, segment->lastIndex() + 1);
            break;
        }
        visible_start = segment->firstIndex();
        previous = visible_start - 1;
    }
    visible_start = std::max(visible_start, wanted);

    ptr<NuRaftLogSegment> active;
    for (const auto & segment : readable)
    {
        if (segment->isOpen() && segment->lastIndex() == covered)
        {
            if (active && active->firstIndex() == segment->firstIndex())
                throw Exception(ErrorCodes::CORRUPTED_LOG, "Ambiguous open log files at {}", segment->firstIndex());
            active = segment;
        }
    }

    Segments retained;
    retained.reserve(segments.size());
    for (auto & segment : segments)
    {
        if (segment != active)
        {
            segment->seal();
            retained.push_back(std::move(segment));
        }
    }
    open_segment = std::move(active);
    closed_segments = std::move(retained);
    first_log_index.store(visible_start, std::memory_order_release);
    last_log_index.store(covered, std::memory_order_release);
    retention_boundary = 0;
}
}
