#include <Poco/File.h>

#include <Common/IO/ReadBufferFromFileDescriptor.h>
#include <Common/IO/ReadHelpers.h>
#include <Common/IO/WriteBufferFromFileDescriptor.h>
#include <Common/IO/WriteHelpers.h>
#include <Common/setThreadName.h>

#include <Service/KeeperUtils.h>
#include <Service/LastCommittedIndexManager.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

inline UInt64 getCurrentTimeMicroseconds()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

LastCommittedIndexManager::LastCommittedIndexManager(const String & log_dir) : log(&Poco::Logger::get("LastCommittedIndexManager"))
{
    if (!Poco::File(log_dir).exists())
        Poco::File(log_dir).createDirectories();

    persist_file_name = log_dir + "/" + std::string(FILE_NAME);
    persist_file_fd = ::open(persist_file_name.c_str(), O_RDWR | O_CREAT, 0644);

    if (persist_file_fd < 0)
        throwFromErrno("Failed to open committed log index file", ErrorCodes::CANNOT_OPEN_FILE);

    previous_persist_time = getCurrentTimeMicroseconds();
    persist_thread = ThreadFromGlobalPool([this] { persistThread(); });
}

LastCommittedIndexManager::~LastCommittedIndexManager()
{
    shutDown();
}

void LastCommittedIndexManager::push(UInt64 index)
{
    last_committed_index = index;
}

UInt64 LastCommittedIndexManager::get()
{
    off_t file_size = lseek(persist_file_fd, 0, SEEK_END);
    if (-1 == file_size)
        throwFromErrno("Cannot seek committed log index file " + persist_file_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    /// blank file
    if (file_size == 0)
    {
        LOG_INFO(log, "Last committed log index file {} is empty", persist_file_name);
        return 0;
    }

    /// seek to file start
    lseek(persist_file_fd, 0, SEEK_SET);
    ReadBufferFromFileDescriptor in(persist_file_fd);

    UInt64 index{};
    readIntBinary(index, in);

    LOG_INFO(log, "Read last committed index {}", index);

    return index;
}

void LastCommittedIndexManager::persistThread()
{
    LOG_INFO(log, "Starting background last committed index persist thread");
    setThreadName("pst_lst_idx");

    while (!is_shut_down)
    {
        while (!is_shut_down)
        {
            std::unique_lock lock(mutex);
            if (cv.wait_for(
                    lock,
                    std::chrono::microseconds(PERSIST_INTERVAL_US),
                    [this]
                    {
                        auto now = getCurrentTimeMicroseconds();
                        return last_committed_index >= previous_persist_index + BATCH_COUNT
                            || (previous_persist_time && now - previous_persist_time > PERSIST_INTERVAL_US);
                    }))
                break;
        }

        /// Update previous_persist_time to avoid endless loop.
        previous_persist_time = getCurrentTimeMicroseconds();

        uint64_t current_index = last_committed_index.load();
        if (previous_persist_index == current_index)
            continue;

        WriteBufferFromFileDescriptor out(persist_file_fd);
        out.seek(0, SEEK_SET);

        writeIntBinary(current_index, out);
        out.next();

        previous_persist_index = current_index;
    }
}

void LastCommittedIndexManager::shutDown()
{
    LOG_INFO(log, "Shutting down last committed index persist thread");
    if (!is_shut_down)
    {
        is_shut_down = true;
        persist_thread.join();

        ::close(persist_file_fd);
    }
}

}
