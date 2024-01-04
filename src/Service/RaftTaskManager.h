#pragma once

#include <fstream>
#include <Service/ThreadSafeQueue.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/Util/LayeredConfiguration.h>
#include "Common/StringUtils.h"
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>

namespace RK
{

/// Only support backend async task
enum TaskType
{
    TYPE_IDLE [[maybe_unused]] = -1,
    TYPE_COMMITTED = 0,
    TYPE_ERROR [[maybe_unused]] = 99
};

class BaseTask
{
public:
    explicit BaseTask(TaskType type) : task_type(type) { }
    TaskType task_type;
};

class CommittedTask : public BaseTask
{
public:
    CommittedTask() : BaseTask(TaskType::TYPE_COMMITTED), size(sizeof(nuraft::ulong)) { }
    nuraft::ulong last_committed_index;
    int write(int & fd);
    int read(int & fd);

private:
    int size;
};

/**
 * Asynchronously persist committed log index into disk.
 * When startup state machine apply log to this log index.
 */
class RaftTaskManager
{
public:
    explicit RaftTaskManager(const String & snapshot_dir);
    ~RaftTaskManager();

    /// save last index after commit log to state machine
    void afterCommitted(nuraft::ulong last_committed_index);

    /// get last committed index
    void getLastCommitted(nuraft::ulong & last_committed_index);

    ///server shut down
    void shutDown();

private:
    ThreadPool thread_pool;

    ThreadSafeQueue<std::shared_ptr<BaseTask>> task_queue;
    std::mutex write_file;

    std::atomic<bool> is_shut_down{false};
    std::vector<String> task_file_names;

    std::vector<int> task_files;
    Poco::Logger * log;

    UInt32 get_task_timeout_ms = 100;
    UInt32 batch_size = 1000;
};

}
