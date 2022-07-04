#include <set>
#include <sys/poll.h>
#include <Service/PollSet.h>
#include <Poco/Mutex.h>
#include <Poco/Net/SocketImpl.h>


#if defined(POCO_HAVE_FD_EPOLL)
	#include <sys/epoll.h>
	#include <sys/eventfd.h>
#elif defined(POCO_HAVE_FD_POLL)
    #include <Poco/Pipe.h>
#endif

using Poco::Net::SocketImpl;

namespace DB {

#if defined(POCO_HAVE_FD_EPOLL)


//
// Linux implementation using epoll
//
class PollSetImpl
{
public:
	PollSetImpl(): _epollfd(epoll_create(1)),
		_events(1024),
		_eventfd(eventfd(0, 0))
	{
		int err = addImpl(_eventfd, PollSet::POLL_READ, nullptr);
		if ((err) || (_epollfd < 0))
		{
			errno;
		}
	}

	~PollSetImpl()
	{
		if (_epollfd >= 0) ::close(_epollfd);
	}

	void add(const Socket& socket, int mode)
	{
		Poco::FastMutex::ScopedLock lock(_mutex);

		SocketImpl* sockImpl = socket.impl();

		int err = addImpl(sockImpl->sockfd(), mode, sockImpl);

		if (err)
		{
			if (errno == EEXIST) update(socket, mode);
			else errno;
		}

		if (_socketMap.find(sockImpl) == _socketMap.end())
			_socketMap[sockImpl] = socket;
	}

	void remove(const Socket& socket)
	{
		Poco::FastMutex::ScopedLock lock(_mutex);

		poco_socket_t fd = socket.impl()->sockfd();
		struct epoll_event ev;
		ev.events = 0;
		ev.data.ptr = nullptr;
		int err = epoll_ctl(_epollfd, EPOLL_CTL_DEL, fd, &ev);
		if (err) errno;

		_socketMap.erase(socket.impl());
	}

	bool has(const Socket& socket) const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		SocketImpl* sockImpl = socket.impl();
		return sockImpl &&
			(_socketMap.find(sockImpl) != _socketMap.end());
	}

	bool empty() const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		return _socketMap.empty();
	}

	void update(const Socket& socket, int mode)
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
		int err = epoll_ctl(_epollfd, EPOLL_CTL_MOD, fd, &ev);
		if (err)
		{
			errno;
		}
	}

	void clear()
	{
		Poco::FastMutex::ScopedLock lock(_mutex);

		::close(_epollfd);
		_socketMap.clear();
		_epollfd = epoll_create(1);
		if (_epollfd < 0)
		{
			errno;
		}
	}

	PollSet::SocketModeMap poll(const Poco::Timespan& timeout)
	{
		PollSet::SocketModeMap result;
		Poco::Timespan remainingTime(timeout);
		int rc;
		do
		{
			Poco::Timestamp start;
			rc = epoll_wait(_epollfd, &_events[0], _events.size(), remainingTime.totalMilliseconds());
			if (rc == 0) return result;
			if (rc < 0 && errno == POCO_EINTR)
			{
				Poco::Timestamp end;
				Poco::Timespan waited = end - start;
				if (waited < remainingTime)
					remainingTime -= waited;
				else
					break;
			}
		}
		while (rc < 0 && errno == POCO_EINTR);
		if (rc < 0) errno;

		Poco::FastMutex::ScopedLock lock(_mutex);

		for (int i = 0; i < rc; i++)
		{
			if (_events[i].data.ptr) // skip eventfd
			{
				std::map<void *, Socket>::iterator it = _socketMap.find(_events[i].data.ptr);
				if (it != _socketMap.end())
				{
					if (_events[i].events & EPOLLIN)
						result[it->second] |= PollSet::POLL_READ;
					if (_events[i].events & EPOLLOUT)
						result[it->second] |= PollSet::POLL_WRITE;
					if (_events[i].events & EPOLLERR)
						result[it->second] |= PollSet::POLL_ERROR;
				}
			}
		}

		return result;
	}

	void wakeUp()
	{
		uint64_t val = 1;
		int n = ::write(_eventfd, &val, sizeof(val));
		if (n < 0) errno;
	}

	int count() const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		return static_cast<int>(_socketMap.size());
	}

private:
	int addImpl(int fd, int mode, void* ptr)
	{
		struct epoll_event ev;
		ev.events = 0;
		if (mode & PollSet::POLL_READ)
			ev.events |= EPOLLIN;
		if (mode & PollSet::POLL_WRITE)
			ev.events |= EPOLLOUT;
		if (mode & PollSet::POLL_ERROR)
			ev.events |= EPOLLERR;
		ev.data.ptr = ptr;
		return epoll_ctl(_epollfd, EPOLL_CTL_ADD, fd, &ev);
	}

	mutable Poco::FastMutex         _mutex;
	int                             _epollfd;
	std::map<void*, Socket>         _socketMap;
	std::vector<struct epoll_event> _events;
	int                             _eventfd;
};


#elif defined(POCO_HAVE_FD_POLL)


//
// BSD/Windows implementation using poll/WSAPoll
//
class PollSetImpl
{
public:
    PollSetImpl()
    {
        pollfd fd{_pipe.readHandle(), POLLIN, 0};
        _pollfds.push_back(fd);
    }

    ~PollSetImpl()
    {
        _pipe.close();
    }

	void add(const Socket& socket, int mode)
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		poco_socket_t fd = socket.impl()->sockfd();
		_addMap[fd] = mode;
		_removeSet.erase(fd);
		_socketMap[fd] = socket;
	}

	void remove(const Socket& socket)
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		poco_socket_t fd = socket.impl()->sockfd();
		_removeSet.insert(fd);
		_addMap.erase(fd);
		_socketMap.erase(fd);
	}

	bool has(const Socket& socket) const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		SocketImpl* sockImpl = socket.impl();
		return sockImpl &&
			(_socketMap.find(sockImpl->sockfd()) != _socketMap.end());
	}

	bool empty() const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		return _socketMap.empty();
	}

	void update(const Socket& socket, int mode)
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		poco_socket_t fd = socket.impl()->sockfd();
		for (auto it = _pollfds.begin(); it != _pollfds.end(); ++it)
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
		Poco::FastMutex::ScopedLock lock(_mutex);

		_socketMap.clear();
		_addMap.clear();
		_removeSet.clear();
        _pollfds.reserve(1);
	}

	PollSet::SocketModeMap poll(const Poco::Timespan& timeout)
	{
		PollSet::SocketModeMap result;
		{
			Poco::FastMutex::ScopedLock lock(_mutex);

			if (!_removeSet.empty())
			{
				for (auto it = _pollfds.begin(); it != _pollfds.end();)
				{
					if (_removeSet.find(it->fd) != _removeSet.end())
					{
						it = _pollfds.erase(it);
					}
					else ++it;
				}
				_removeSet.clear();
			}

			_pollfds.reserve(_pollfds.size() + _addMap.size());
			for (auto it = _addMap.begin(); it != _addMap.end(); ++it)
			{
				pollfd pfd;
				pfd.fd = it->first;
				pfd.events = 0;
				pfd.revents = 0;
				setMode(pfd.events, it->second);
				_pollfds.push_back(pfd);
			}
			_addMap.clear();
		}

		if (_pollfds.empty()) return result;

		Poco::Timespan remainingTime(timeout);
		int rc;
		do
		{
			Poco::Timestamp start;
#ifdef _WIN32
			rc = WSAPoll(&_pollfds[0], static_cast<ULONG>(_pollfds.size()), static_cast<INT>(remainingTime.totalMilliseconds()));
#else
			rc = ::poll(&_pollfds[0], _pollfds.size(), remainingTime.totalMilliseconds());
#endif
			if (rc < 0 && errno == POCO_EINTR)
			{
				Poco::Timestamp end;
				Poco::Timespan waited = end - start;
				if (waited < remainingTime)
					remainingTime -= waited;
				else
					remainingTime = 0;
			}
		}
		while (rc < 0 && errno == POCO_EINTR);
		if (rc < 0) errno;

		{
            if (_pollfds[0].revents & POLLIN)
            {
                char c;
                _pipe.readBytes(&c, 1);
            }

			Poco::FastMutex::ScopedLock lock(_mutex);

			if (!_socketMap.empty())
			{
				for (auto it = _pollfds.begin() + 1; it != _pollfds.end(); ++it)
				{
					std::map<poco_socket_t, Socket>::const_iterator its = _socketMap.find(it->fd);
					if (its != _socketMap.end())
					{
						if ((it->revents & POLLIN)
#ifdef _WIN32
							|| (it->revents & POLLHUP)
#endif
							)
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
		Poco::FastMutex::ScopedLock lock(_mutex);
		return static_cast<int>(_socketMap.size());
	}

private:

	void setMode(short& target, int mode)
	{
		if (mode & PollSet::POLL_READ)
			target |= POLLIN;

		if (mode & PollSet::POLL_WRITE)
			target |= POLLOUT;
	}

	mutable Poco::FastMutex         _mutex;
	std::map<poco_socket_t, Socket> _socketMap;
	std::map<poco_socket_t, int>    _addMap;
	std::set<poco_socket_t>         _removeSet;
	std::vector<pollfd>             _pollfds;
    Poco::Pipe _pipe;
    /// Add _pipe to head of _pollfds used to wake up poll blocking
};


#else



#endif


PollSet::PollSet():
	_pImpl(new PollSetImpl)
{
}


PollSet::~PollSet()
{
	delete _pImpl;
}


void PollSet::add(const Socket& socket, int mode)
{
	_pImpl->add(socket, mode);
}


void PollSet::remove(const Socket& socket)
{
	_pImpl->remove(socket);
}


void PollSet::update(const Socket& socket, int mode)
{
	_pImpl->update(socket, mode);
}


bool PollSet::has(const Socket& socket) const
{
	return _pImpl->has(socket);
}


bool PollSet::empty() const
{
	return _pImpl->empty();
}


void PollSet::clear()
{
	_pImpl->clear();
}


PollSet::SocketModeMap PollSet::poll(const Poco::Timespan& timeout)
{
	return _pImpl->poll(timeout);
}


int PollSet::count() const
{
	return _pImpl->count();
}


void PollSet::wakeUp()
{
	_pImpl->wakeUp();
}


}
