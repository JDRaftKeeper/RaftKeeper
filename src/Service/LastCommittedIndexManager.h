#pragma once

#include <Common/ThreadPool.h>
#include <common/logger_useful.h>


namespace RK
{

/**
 * Asynchronously persistThread committed log index into disk. When startup state machine will
 * apply log to this log index, then RaftKeeper provide service to client.
 *
 * This is designed to avoid client read stale data when restarting.
 *
 * Note:
 *  1. LastCommittedIndexManager will not persist every index, it will batch persistThread.
 *  2. LastCommittedIndexManager work asynchronously, it will not block log committing.
 */
class LastCommittedIndexManager
{
public:
    explicit LastCommittedIndexManager(const String & log_dir);
    ~LastCommittedIndexManager();

    /// Push last_committed_index into queue
    void push(ulong index);

    /// Get last committed log index.
    /// Used when replaying logs after restart.
    void get(ulong & index);

    void persistThread();

    /// Shutdown background thread
    void shutDown();

private:
    UInt32 static constexpr BATCH_COUNT = 1000;
    UInt64 static constexpr PERSIST_INTERVAL_US = 100 * 1000;
    std::string_view static constexpr FILE_NAME = "last_committed_index.bin";

    ThreadFromGlobalPool persist_thread;
    std::atomic<bool> is_shut_down{false};

    String persist_file_name;
    int persist_file_fd = -1;

    std::atomic_uint64_t last_committed_index{0};
    uint64_t previous_persist_index = 0;
    uint64_t previous_persist_time = 0;

    std::mutex mutex;
    std::condition_variable cv;

    Poco::Logger * log;
};

}
