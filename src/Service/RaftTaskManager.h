#pragma once

#include <Common/ThreadFromGlobalPool.h>
#include <common/logger_useful.h>


namespace RK
{

/**
 * Asynchronously persist committed log index into disk. When startup state machine will
 * apply log to this log index, then RaftKeeper provide service to client.
 *
 * This is designed to avoid client read stale data when restarting.
 *
 * Note:
 *  1. CommittedLogIndexManager will not persist every index, it will batch persist.
 *  2. CommittedLogIndexManager work asynchronously, it will not block log committing.
 */
class CommittedLogIndexManager
{
public:
    explicit CommittedLogIndexManager(const String & log_dir);
    ~CommittedLogIndexManager();

    /// Push last_committed_index into queue
    void push(nuraft::ulong last_committed_index);

    /// Get last committed log index.
    /// Used when replaying logs after restart.
    void get(nuraft::ulong & last_committed_index);

    void persist();

    /// Shutdown background thread
    void shutDown();

private:
    UInt32 static constexpr BATCH_COUNT = 1000;
    std::string_view static constexpr FILE_NAME = "committed.task";

    ThreadFromGlobalPool persist_thread;
    std::atomic<bool> is_shut_down{false};

    String persist_file_name;
    int persist_file_fd = -1;

    std::atomic_uint64_t to_persist_index{0};
    std::atomic_uint64_t previous_persist_index = 0;

    std::mutex mutex;
    std::condition_variable cv;

    Poco::Logger * log;
};

}
