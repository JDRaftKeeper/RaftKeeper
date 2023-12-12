/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#include <set>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#include <Poco/Logger.h>
#include <Poco/Mutex.h>
#include <Poco/Net/Socket.h>
#include <Poco/Net/SocketImpl.h>
#include <Poco/Thread.h>

#include <Common/NIO/PollSet.h>
#include <common/logger_useful.h>

using Poco::Net::SocketImpl;

namespace RK
{

/// PollSet implementation with epoll
class PollSetImpl
{
public:
    PollSetImpl();
    ~PollSetImpl();

    void add(const Socket & socket, int mode);
    void remove(const Socket & socket);

    bool has(const Socket & socket) const;
    bool empty() const;

    void update(const Socket & socket, int mode);
    void clear();

    PollSet::SocketModeMap poll(const Poco::Timespan & timeout);

    void wakeUp();
    int count() const;

private:
    int addImpl(int fd, int mode, void * data);

    mutable Poco::FastMutex mutex;

    /// Monitored epoll events
    std::map<SocketImpl *, Socket> socket_map;

    /// epoll fd
    int epoll_fd;

    /// Monitored epoll events
    std::vector<struct epoll_event> events;

    /// Only used to wake up poll set by writing 8 bytes.
    int waking_up_fd;

    Poco::Logger * log;
};


PollSetImpl::PollSetImpl()
    : epoll_fd(epoll_create(1)), events(1024), waking_up_fd(eventfd(0, EFD_NONBLOCK)), log(&Poco::Logger::get("PollSet"))
{
    /// Monitor waking up fd, use this as waking up event marker.
    int err = addImpl(waking_up_fd, PollSet::POLL_READ, this);
    if ((err) || (epoll_fd < 0))
    {
        errno;
    }
}


PollSetImpl::~PollSetImpl()
{
    if (epoll_fd >= 0)
        ::close(epoll_fd);
}

void PollSetImpl::add(const Socket & socket, int mode)
{
    Poco::FastMutex::ScopedLock lock(mutex);
    SocketImpl * socket_impl = socket.impl();
    int err = addImpl(socket_impl->sockfd(), mode, socket_impl);

    if (err)
    {
        if (errno == EEXIST)
            update(socket, mode);
        else
            errno;
    }

    if (socket_map.find(socket_impl) == socket_map.end())
        socket_map[socket_impl] = socket;
}

int PollSetImpl::addImpl(int fd, int mode, void * data)
{
    struct epoll_event ev;
    ev.events = 0;
    if (mode & PollSet::POLL_WRITE)
        ev.events |= EPOLLOUT;
    if (mode & PollSet::POLL_ERROR)
        ev.events |= EPOLLERR;
    if (mode & PollSet::POLL_READ)
        ev.events |= EPOLLIN;
    ev.data.ptr = data;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

void PollSetImpl::remove(const Socket & socket)
{
    Poco::FastMutex::ScopedLock lock(mutex);

    poco_socket_t fd = socket.impl()->sockfd();
    struct epoll_event ev;
    ev.events = 0;
    ev.data.ptr = nullptr;
    int err = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &ev);
    if (err)
        errno;

    socket_map.erase(socket.impl());
}

bool PollSetImpl::has(const Socket & socket) const
{
    Poco::FastMutex::ScopedLock lock(mutex);
    SocketImpl * socket_impl = socket.impl();
    return socket_impl && (socket_map.find(socket_impl) != socket_map.end());
}

bool PollSetImpl::empty() const
{
    Poco::FastMutex::ScopedLock lock(mutex);
    return socket_map.empty();
}

void PollSetImpl::update(const Socket & socket, int mode)
{
    poco_socket_t fd = socket.impl()->sockfd();
    struct epoll_event ev;
    ev.events = 0;
    if (mode & PollSet::POLL_READ)
        ev.events |= EPOLLIN;
    if (mode & PollSet::POLL_WRITE)
        ev.events |= EPOLLOUT;
    if (mode & PollSet::POLL_ERROR)
        ev.events |= EPOLLERR;
    ev.data.ptr = socket.impl();
    int err = epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    if (err)
    {
        errno;
    }
}

void PollSetImpl::clear()
{
    Poco::FastMutex::ScopedLock lock(mutex);

    ::close(epoll_fd);
    socket_map.clear();
    epoll_fd = epoll_create(1);
    if (epoll_fd < 0)
    {
        errno;
    }
}

PollSet::SocketModeMap PollSetImpl::poll(const Poco::Timespan & timeout)
{
    PollSet::SocketModeMap result;
    Poco::Timespan remaining_time(timeout);
    int rc;
    do
    {
        Poco::Timestamp start;
        rc = epoll_wait(epoll_fd, &events[0], events.size(), remaining_time.totalMilliseconds());
        if (rc == 0)
            return result;
        if (rc < 0 && errno == POCO_EINTR)
        {
            Poco::Timestamp end;
            Poco::Timespan waited = end - start;
            if (waited < remaining_time)
                remaining_time -= waited;
            else
                remaining_time = 0;
            LOG_TRACE(log, "Poll wait encounter error EINTR {}ms", remaining_time.totalMilliseconds());
        }
    } while (rc < 0 && errno == POCO_EINTR);
    if (rc < 0)
        errno;

    Poco::FastMutex::ScopedLock lock(mutex);

    for (int i = 0; i < rc; i++)
    {
        if (events[i].data.ptr == this)
        {
            uint64_t val;
            auto n = ::read(waking_up_fd, &val, sizeof(val));
            LOG_TRACE(
                log, "Poll wakeup {} {} {} {}", Poco::Thread::current() ? Poco::Thread::current()->name() : "main", waking_up_fd, n, errno);
            if (n < 0)
                errno;
        }
        else if (events[i].data.ptr)
        {
            std::map<SocketImpl *, Socket>::iterator it = socket_map.find(static_cast<SocketImpl *>(events[i].data.ptr));
            if (it != socket_map.end())
            {
                if (events[i].events & EPOLLIN)
                    result[it->second] |= PollSet::POLL_READ;
                if (events[i].events & EPOLLOUT)
                    result[it->second] |= PollSet::POLL_WRITE;
                if (events[i].events & EPOLLERR)
                    result[it->second] |= PollSet::POLL_ERROR;
            }
        }
        else
        {
            LOG_ERROR(log, "Poll receive null data socket event {}", static_cast<unsigned int>(events[i].events));
        }
    }

    return result;
}

void PollSetImpl::wakeUp()
{
    uint64_t val = 1;
    int n = ::write(waking_up_fd, &val, sizeof(val));
    LOG_TRACE(log, "Poll trigger wakeup {} {} {}", Poco::Thread::current() ? Poco::Thread::current()->name() : "main", waking_up_fd, n);
    if (n < 0)
        errno;
}

int PollSetImpl::count() const
{
    Poco::FastMutex::ScopedLock lock(mutex);
    return static_cast<int>(socket_map.size());
}


PollSet::PollSet() : impl(new PollSetImpl)
{
}


PollSet::~PollSet()
{
    delete impl;
}


void PollSet::add(const Socket & socket, int mode)
{
    impl->add(socket, mode);
}


void PollSet::remove(const Socket & socket)
{
    impl->remove(socket);
}


void PollSet::update(const Socket & socket, int mode)
{
    impl->update(socket, mode);
}


bool PollSet::has(const Socket & socket) const
{
    return impl->has(socket);
}


bool PollSet::empty() const
{
    return impl->empty();
}


void PollSet::clear()
{
    impl->clear();
}


PollSet::SocketModeMap PollSet::poll(const Poco::Timespan & timeout)
{
    return impl->poll(timeout);
}


int PollSet::count() const
{
    return impl->count();
}


void PollSet::wakeUp()
{
    impl->wakeUp();
}

}
