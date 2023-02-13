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
#pragma once
#if defined(OS_LINUX)

#include <sys/epoll.h>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>

namespace RK
{

using AsyncCallback = std::function<void(int, const Poco::Timespan &, const std::string &)>;

class Epoll
{
public:
    Epoll();

    Epoll(const Epoll &) = delete;
    Epoll & operator=(const Epoll &) = delete;

    Epoll & operator=(Epoll && other);
    Epoll(Epoll && other);

    /// Add new file descriptor to epoll. If ptr set to nullptr, epoll_event.data.fd = fd,
    /// otherwise epoll_event.data.ptr = ptr.
    void add(int fd, void * ptr = nullptr);

    /// Remove file descriptor to epoll.
    void remove(int fd);

    /// Get events from epoll. Events are written in events_out, this function returns an amount of ready events.
    /// If blocking is false and there are no ready events,
    /// return empty vector, otherwise wait for ready events.
    size_t getManyReady(int max_events, epoll_event * events_out, bool blocking) const;

    int getFileDescriptor() const { return epoll_fd; }

    int size() const { return events_count; }

    bool empty() const { return events_count == 0; }

    const std::string & getDescription() const { return fd_description; }

    ~Epoll();

private:
    int epoll_fd;
    std::atomic<int> events_count;
    const std::string fd_description = "epoll";
};

}
#endif
