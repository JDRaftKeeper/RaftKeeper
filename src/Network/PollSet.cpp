/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#include <set>
#if defined(POCO_HAVE_FD_EPOLL)
#    include <sys/epoll.h>
#    include <sys/eventfd.h>
#else
#    include <poll.h>
#    include <Poco/Pipe.h>
#endif

#include <Poco/Logger.h>
#include <Poco/Mutex.h>
#include <Poco/Net/Socket.h>
#include <Poco/Net/SocketImpl.h>
#include <Poco/Thread.h>

#include <Common/Exception.h>
#include <Network/PollSet.h>

using Poco::Net::SocketImpl;

namespace RK
{

namespace ErrorCodes
{
    extern const int EPOLL_ERROR;
    extern const int EPOLL_CTL;
    extern const int EPOLL_CREATE;
    extern const int EPOLL_WAIT;
    extern const int POLL_EVENT;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Return local address string for server socket, or return remote address for stream socket.
    [[maybe_unused]] std::string getAddressName(const Socket & sock)
    {
        return sock.isStream() ? sock.peerAddress().toString() : sock.address().toString();
    }
}

#if defined(POCO_HAVE_FD_EPOLL)

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
        throwFromErrno("Error when initializing poll set", ErrorCodes::EPOLL_ERROR, errno);
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
            throwFromErrno("Error when updating epoll event to " + getAddressName(socket), ErrorCodes::EPOLL_CTL, errno);
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
        throwFromErrno("Error when updating epoll event to " + getAddressName(socket), ErrorCodes::EPOLL_CTL, errno);

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
        throwFromErrno("Error when updating epoll event to " + getAddressName(socket), ErrorCodes::EPOLL_CTL, errno);
}

void PollSetImpl::clear()
{
    Poco::FastMutex::ScopedLock lock(mutex);

    ::close(epoll_fd);
    socket_map.clear();
    epoll_fd = epoll_create(1);
    if (epoll_fd < 0)
    {
        throwFromErrno("Error when creating epoll fd", ErrorCodes::EPOLL_CREATE, errno);
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
        {
            return result;
        }

        if (rc < 0 && errno == POCO_EINTR)
        {
            Poco::Timestamp end;
            Poco::Timespan waited = end - start;
            if (waited < remaining_time)
                remaining_time -= waited;
            else
                break; /// timeout
        }
    } while (rc < 0 && errno == POCO_EINTR);

    if (rc < 0 && errno != POCO_EINTR)
        throwFromErrno("Error when epoll waiting", ErrorCodes::EPOLL_WAIT, errno);

    Poco::FastMutex::ScopedLock lock(mutex);

    for (int i = 0; i < rc; i++)
    {
        /// Read data from 'wakeUp' method
        if (events[i].data.ptr == this)
        {
            uint64_t val;
            auto n = ::read(waking_up_fd, &val, sizeof(val));
            if (n < 0)
                throwFromErrno("Error when reading data from 'wakeUp' method", ErrorCodes::EPOLL_CREATE, errno);
        }
        /// Handle IO events
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Should never reach here.");
        }
    }

    return result;
}

void PollSetImpl::wakeUp()
{
    uint64_t val = 0;
    int n = ::write(waking_up_fd, &val, sizeof(val));
    if (n < 0)
        throwFromErrno("Error when trying to wakeup poll set", ErrorCodes::EPOLL_CREATE, errno);
}

int PollSetImpl::count() const
{
    Poco::FastMutex::ScopedLock lock(mutex);
    return static_cast<int>(socket_map.size());
}

#else

/// BSD implementation using poll
class PollSetImpl
{
public:
    PollSetImpl()
    {
        pollfd fd{_pipe.readHandle(), POLLIN, 0};
        poll_fds.push_back(fd);
    }

    ~PollSetImpl() { _pipe.close(); }

    void add(const Socket & socket, int mode)
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        poco_socket_t fd = socket.impl()->sockfd();
        add_map[fd] |= mode;
        remove_set.erase(fd);
        socket_map[fd] = socket;
    }

    void remove(const Socket & socket)
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        poco_socket_t fd = socket.impl()->sockfd();
        remove_set.insert(fd);
        add_map.erase(fd);
        socket_map.erase(fd);
    }

    bool has(const Socket & socket) const
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        SocketImpl * sockImpl = socket.impl();
        return sockImpl && (socket_map.find(sockImpl->sockfd()) != socket_map.end());
    }

    bool empty() const
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        return socket_map.empty();
    }

    void update(const Socket & socket, int mode)
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        poco_socket_t fd = socket.impl()->sockfd();
        for (auto it = poll_fds.begin(); it != poll_fds.end(); ++it)
        {
            if (it->fd == fd)
            {
                it->events = 0;
                it->revents = 0;
                setMode(it->events, mode);
            }
        }
    }

    void clear()
    {
        Poco::FastMutex::ScopedLock lock(mutex);

        socket_map.clear();
        add_map.clear();
        remove_set.clear();
        poll_fds.reserve(1);
    }

    PollSet::SocketModeMap poll(const Poco::Timespan & timeout)
    {
        PollSet::SocketModeMap result;
        {
            Poco::FastMutex::ScopedLock lock(mutex);

            if (!remove_set.empty())
            {
                for (auto it = poll_fds.begin(); it != poll_fds.end();)
                {
                    if (remove_set.find(it->fd) != remove_set.end())
                    {
                        it = poll_fds.erase(it);
                    }
                    else
                        ++it;
                }
                remove_set.clear();
            }

            poll_fds.reserve(poll_fds.size() + add_map.size());
            for (auto it = add_map.begin(); it != add_map.end(); ++it)
            {
                pollfd pfd;
                pfd.fd = it->first;
                pfd.events = 0;
                pfd.revents = 0;
                setMode(pfd.events, it->second);
                poll_fds.push_back(pfd);
            }
            add_map.clear();
        }

        if (poll_fds.empty())
            return result;

        Poco::Timespan remainingTime(timeout);
        int rc;
        do
        {
            Poco::Timestamp start;
            rc = ::poll(&poll_fds[0], poll_fds.size(), remainingTime.totalMilliseconds());
            if (rc < 0 && errno == POCO_EINTR)
            {
                Poco::Timestamp end;
                Poco::Timespan waited = end - start;
                if (waited < remainingTime)
                    remainingTime -= waited;
                else
                    remainingTime = 0;
            }
        } while (rc < 0 && errno == POCO_EINTR);
        if (rc < 0)
            throwFromErrno("Error when polling event", ErrorCodes::POLL_EVENT, errno);

        {
            if (poll_fds[0].revents & POLLIN)
            {
                char c;
                _pipe.readBytes(&c, 1);
            }

            Poco::FastMutex::ScopedLock lock(mutex);

            if (!socket_map.empty())
            {
                for (auto it = poll_fds.begin() + 1; it != poll_fds.end(); ++it)
                {
                    std::map<poco_socket_t, Socket>::const_iterator its = socket_map.find(it->fd);
                    if (its != socket_map.end())
                    {
                        if (it->revents & POLLIN)
                            result[its->second] |= PollSet::POLL_READ;
                        if (it->revents & POLLOUT)
                            result[its->second] |= PollSet::POLL_WRITE;
                        if (it->revents & POLLERR || (it->revents & POLLHUP))
                            result[its->second] |= PollSet::POLL_ERROR;
                    }
                    it->revents = 0;
                }
            }
        }

        return result;
    }

    void wakeUp()
    {
        char c = 1;
        _pipe.writeBytes(&c, 1);
    }

    int count() const
    {
        Poco::FastMutex::ScopedLock lock(mutex);
        return static_cast<int>(socket_map.size());
    }

private:
    void setMode(short & target, int mode)
    {
        if (mode & PollSet::POLL_READ)
            target |= POLLIN;

        if (mode & PollSet::POLL_WRITE)
            target |= POLLOUT;
    }

    mutable Poco::FastMutex mutex;
    std::map<poco_socket_t, Socket> socket_map;
    std::map<poco_socket_t, int> add_map;
    std::set<poco_socket_t> remove_set;
    std::vector<pollfd> poll_fds;
    Poco::Pipe _pipe;
};

#endif


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
