/**
 * Copyright 2021-2023 JD.com, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <fcntl.h>
#include <stdio.h>
#include <Service/KeeperCommon.h>
#include <Service/RaftTaskManager.h>
#include <sys/uio.h>
#include <Poco/File.h>

namespace RK
{
class CommittedTask : public BaseTask
{
public:
    CommittedTask() : BaseTask(TaskType::COMMITTED), size(sizeof(nuraft::ulong)) { }
    nuraft::ulong last_committed_index;
    int write(int & fd);
    int read(int & fd);

private:
    int size;
};

int CommittedTask::write(int & fd)
{
    struct iovec vec[1];
    vec[0].iov_base = &last_committed_index;
    vec[0].iov_len = size;
    errno = 0;

    off_t ret_off = lseek(fd, 0, SEEK_SET);
    if (ret_off < 0)
    {
        return ret_off;
    }

    ssize_t ret = writev(fd, vec, 1);
    if (ret < 0 || ret != size)
    {
        return -1;
    }

    ret = ::fsync(fd);
    if (ret < 0)
        return ret;

    return 0;
}

int CommittedTask::read(int & fd)
{
    nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(size);
    buf->pos(0);
    errno = 0;
    ssize_t ret = pread(fd, buf->data(), size, 0);
    if (ret < 0 || ret != size)
    {
        return -1;
    }

    last_committed_index = buf->get_ulong();
    return 0;
}

RaftTaskManager::RaftTaskManager(const std::string & snapshot_dir) : thread_pool(1), log(&Poco::Logger::get("RaftTaskManager"))
{
    if (!Poco::File(snapshot_dir).exists())
    {
        if (Directory::createDir(snapshot_dir) != 0)
        {
            LOG_ERROR(log, "Fail to create directory {}", snapshot_dir);
            return;
        }
    }

    //task file names
    task_file_names.push_back(snapshot_dir + "/committed.task");
    //task file description
    for (auto fileName : task_file_names)
    {
        errno = 0;
        int fd = ::open(fileName.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0)
        {
            LOG_ERROR(log, "Fail to open {}, error:{}", fileName, strerror(errno));
            return;
        }
        task_files.push_back(fd);
        LOG_INFO(log, "Open task for task, fd {}, path {}", fd, fileName);
    }
    thread_pool.trySchedule([this] {
        auto thread_log = &Poco::Logger::get("RaftTaskManager");
        LOG_INFO(thread_log, "try schedule, shut down {}", is_shut_down);
        std::shared_ptr<BaseTask> task;
        while (!is_shut_down)
        {
            //LOG_INFO(log, "begin write index");
            UInt32 batchSize = 0;
            while (true)
            {
                if (!task_queue.tryPop(task, GetTaskTimeoutMS))
                {
                    break;
                }
                batchSize++;
                if (task_queue.size() == 0 || batchSize == BatchSize)
                {
                    if (task->task_type == TaskType::COMMITTED)
                    {
                        auto committedTask = std::static_pointer_cast<CommittedTask>(task);
                        std::lock_guard write_lock(write_file);
                        if (committedTask->write(task_files[task->task_type]) < 0)
                        {
                            LOG_WARNING(
                                thread_log,
                                "write committed failed {}, fd {}",
                                committedTask->last_committed_index,
                                task_files[task->task_type]);
                        }
                        //else
                        //    LOG_INFO(thread_log, "write committed {}", committedTask->lastCommittedIndex);
                    }
                    break;
                }
            }
            //sleep 100ms
            usleep(100000);
            //LOG_INFO(thread_log, "write batch size {}", batchSize);
        }
    });
}

RaftTaskManager::~RaftTaskManager()
{
    shutDown();
}

void RaftTaskManager::afterCommitted(nuraft::ulong last_committed_index)
{
    auto task = std::make_shared<CommittedTask>();
    task->last_committed_index = last_committed_index;
    task_queue.push(task);
    //LOG_INFO(log, "After committed {}", task->lastCommittedIndex);
}

void RaftTaskManager::getLastCommitted(nuraft::ulong & last_committed_index)
{
    if (task_files.size() == 0)
    {
        LOG_WARNING(log, "Task files is empty");
        return;
    }
    int fd = task_files[TaskType::COMMITTED];
    if (fd < 0)
    {
        LOG_WARNING(log, "Task file fd is {}", fd);
        return;
    }
    CommittedTask task;
    std::lock_guard write_lock(write_file);
    int ret = task.read(fd);
    if (ret < 0)
    {
        LOG_WARNING(log, "Read last committed index failed {}", ret);
    }
    else
    {
        last_committed_index = task.last_committed_index;
        LOG_INFO(log, "Read last committed index {}", last_committed_index);
    }
}

void RaftTaskManager::shutDown()
{
    LOG_INFO(log, "Task manager shut down");    
    if (!is_shut_down)
    {
        is_shut_down = true;
        thread_pool.wait();
        std::lock_guard write_lock(write_file);
        for (auto fd : task_files)
        {
            if (fd >= 0)
            {
                ::close(fd);
            }
        }
    }
}

}
