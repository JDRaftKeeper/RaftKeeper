#include <fcntl.h>
#include <stdio.h>
#include <sys/uio.h>

#include <Poco/File.h>

#include <Service/KeeperUtils.h>
#include <Service/RaftTaskManager.h>

#include <Common/IO/WriteHelpers.h>
#include <Common/IO/ReadHelpers.h>
#include <Common/IO/ReadBufferFromFileDescriptor.h>
#include <Common/IO/WriteBufferFromFileDescriptor.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
}

CommittedLogIndexManager::CommittedLogIndexManager(const String & log_dir) : log(&Poco::Logger::get("RaftTaskManager"))
{
    if (!Poco::File(log_dir).exists())
        Poco::File(log_dir).createDirectories();

    persist_file_name = log_dir+ "/" + FILE_NAME;
    persist_file_fd = ::open(persist_file_name.c_str(), O_RDWR | O_CREAT, 0644);

    if (persist_file_fd < 0)
        throwFromErrno("Failed to open committed log index file", ErrorCodes::CANNOT_OPEN_FILE);

    persist_thread = ThreadFromGlobalPool([this] { persist(); });
}

CommittedLogIndexManager::~CommittedLogIndexManager()
{
    shutDown();
}

void CommittedLogIndexManager::push(nuraft::ulong last_committed_index)
{
    to_persist_index = last_committed_index;
}

void CommittedLogIndexManager::get(nuraft::ulong & last_committed_index)
{
    ReadBufferFromFileDescriptor in(persist_file_fd);
    in.seek(0,SEEK_SET);

    /// blank file
    if (in.eof())
    {
        LOG_INFO(log, "Committed log index file {} is empty", persist_file_name);
        last_committed_index = 0;
    }

    readIntBinary(last_committed_index, in);
    in.close();

    LOG_INFO(log, "Read last committed index {}", last_committed_index);
}

void CommittedLogIndexManager::persist()
{
    while (!is_shut_down)
    {
        auto current_index = to_persist_index;
        if (previous_persist_index && current_index <= previous_persist_index + BATCH_COUNT)
            continue;

        WriteBufferFromFileDescriptor out(fd);
        out.seek(0, SEEK_SET);

        writeIntBinary(index, out);
        out.close();

        previous_persist_index = current_index;

        std::lock_guard lock(mutex);
        cv.wait(lock, [this] { return to_persist_index != previous_persist_index; });
    }
}

void CommittedLogIndexManager::shutDown()
{
    LOG_INFO(log, "Shutting down CommittedLogIndexManager");
    if (!is_shut_down)
    {
        is_shut_down = true;
        task_thread.join();

        if (persist_file_fd >= 0)
        {
            ::close(persist_file_fd);
        }
    }
}

}
