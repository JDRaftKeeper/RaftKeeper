/**
 * Copyright 2016-2023 ClickHouse, Inc.
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
#include <memory>

#include "CurrentThread.h"
#include <common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Common/TaskStatsInfoGetter.h>
#include <common/getThreadId.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void CurrentThread::updatePerformanceCounters()
{
    if ((!current_thread))
        return;
    current_thread->updatePerformanceCounters();
}

bool CurrentThread::isInitialized()
{
    return current_thread;
}

ThreadStatus & CurrentThread::get()
{
    if ((!current_thread))
        throw Exception("Thread #" + std::to_string(getThreadId()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    return *current_thread;
}

ProfileEvents::Counters & CurrentThread::getProfileEvents()
{
    return current_thread ? current_thread->performance_counters : ProfileEvents::global_counters;
}

MemoryTracker * CurrentThread::getMemoryTracker()
{
    if ((!current_thread))
        return nullptr;
    return &current_thread->memory_tracker;
}

void CurrentThread::attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue)
{
    if ((!current_thread))
        return;
    current_thread->attachInternalTextLogsQueue(logs_queue);
}

void CurrentThread::setFatalErrorCallback(std::function<void()> callback)
{
    if ((!current_thread))
        return;
    current_thread->setFatalErrorCallback(callback);
}

std::shared_ptr<InternalTextLogsQueue> CurrentThread::getInternalTextLogsQueue()
{
    /// NOTE: this method could be called at early server startup stage
    if ((!current_thread))
        return nullptr;

    if (current_thread->getCurrentState() == ThreadStatus::ThreadState::Died)
        return nullptr;

    return current_thread->getInternalTextLogsQueue();
}

ThreadGroupStatusPtr CurrentThread::getGroup()
{
    if ((!current_thread))
        return nullptr;

    return current_thread->getThreadGroup();
}

}
