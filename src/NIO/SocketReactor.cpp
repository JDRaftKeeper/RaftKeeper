/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#include <Poco/ErrorHandler.h>
#include <Poco/Exception.h>
#include <Poco/Thread.h>

#include <Common/Exception.h>
#include <NIO/SocketNotification.h>
#include <NIO/SocketNotifier.h>
#include <NIO/SocketReactor.h>


using Poco::ErrorHandler;
using Poco::Exception;
using Poco::Thread;
using Poco::Net::SocketImpl;


namespace RK
{

SocketReactor::SocketReactor() : SocketReactor(DEFAULT_TIMEOUT)
{
}


SocketReactor::SocketReactor(const Poco::Timespan & timeout_)
    : timeout(timeout_)
    , stopped(false)
    , rnf(new ReadableNotification(this))
    , wnf(new WritableNotification(this))
    , enf(new ErrorNotification(this))
    , tnf(new TimeoutNotification(this))
    , inf(new IdleNotification(this))
    , snf(new ShutdownNotification(this))
    , log(&Poco::Logger::get("SocketReactor"))
{
}


void SocketReactor::run()
{
    while (!stopped)
    {
        try
        {
            if (!hasSocketHandlers())
            {
                onIdle();
                sleep();
            }
            else
            {
                bool readable = false;
                PollSet::SocketModeMap sm = poll_set.poll(timeout);

                if (!sm.empty())
                {
                    onBusy();
                    for (auto & socket_and_events : sm)
                    {
                        if (socket_and_events.second & PollSet::POLL_READ)
                        {
                            dispatch(socket_and_events.first, *rnf);
                            readable = true;
                        }
                        if (socket_and_events.second & PollSet::POLL_WRITE)
                        {
                            dispatch(socket_and_events.first, *wnf);
                        }
                        if (socket_and_events.second & PollSet::POLL_ERROR)
                        {
                            dynamic_cast<ErrorNotification *>(enf.get())->setErrorNo(errno);
                            dispatch(socket_and_events.first, *enf);
                        }
                    }
                }

                if (!readable)
                    onTimeout();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to handle socket event");
        }
    }
    onShutdown();
}


bool SocketReactor::hasSocketHandlers()
{
    ScopedLock lock(mutex);
    if (!notifiers.empty())
    {
        for (auto & p : notifiers)
        {
            if (p.second->accepts(*rnf) || p.second->accepts(*wnf) || p.second->accepts(*enf))
                return true;
        }
    }

    return false;
}


void SocketReactor::stop()
{
    stopped = true;
    wakeUp();
}

void SocketReactor::wakeUp()
{
    if (stopped)
        return;
    poll_set.wakeUp();
    event.set();
}

void SocketReactor::sleep()
{
    event.tryWait(timeout.totalMilliseconds());
}


void SocketReactor::setTimeout(const Poco::Timespan & timeout_)
{
    timeout = timeout_;
}


const Poco::Timespan & SocketReactor::getTimeout() const
{
    return timeout;
}


void SocketReactor::addEventHandler(const Socket & socket, const AbstractObserver & observer)
{
    SocketNotifierPtr notifier = getNotifier(socket, true);
    if (notifier->addObserverIfNotExist(observer))
    {
        int mode = 0;
        if (notifier->accepts(*rnf))
            mode |= PollSet::POLL_READ;
        if (notifier->accepts(*wnf))
            mode |= PollSet::POLL_WRITE;
        if (notifier->accepts(*enf))
            mode |= PollSet::POLL_ERROR;
        if (mode)
            poll_set.add(socket, mode);
    }
}

void SocketReactor::addEventHandlers(const Socket & socket, const std::vector<AbstractObserver *> & observers)
{
    SocketNotifierPtr notifier = getNotifier(socket, true);

    int mode = 0;
    for (auto * observer : observers)
    {
        notifier->addObserverIfNotExist(*observer);

        if (notifier->accepts(*rnf))
            mode |= PollSet::POLL_READ;
        if (notifier->accepts(*wnf))
            mode |= PollSet::POLL_WRITE;
        if (notifier->accepts(*enf))
            mode |= PollSet::POLL_ERROR;
    }
    if (mode)
        poll_set.add(socket, mode);
}


[[maybe_unused]] bool SocketReactor::hasEventHandler(const Socket & socket, const AbstractObserver & observer)
{
    SocketNotifierPtr notifier = getNotifier(socket);
    if (!notifier)
        return false;
    if (notifier->hasObserver(observer))
        return true;
    return false;
}


SocketNotifierPtr SocketReactor::getNotifier(const Socket & socket, bool makeNew)
{
    const SocketImpl * impl = socket.impl();
    if (impl == nullptr)
        return nullptr;

    poco_socket_t sock_fd = impl->sockfd();
    ScopedLock lock(mutex);

    SocketNotifierMap::iterator it = notifiers.find(sock_fd);
    if (it != notifiers.end())
        return it->second;
    else if (makeNew)
        return (notifiers[sock_fd] = std::make_shared<SocketNotifier>(socket));

    return nullptr;
}


void SocketReactor::removeEventHandler(const Socket & socket, const AbstractObserver & observer)
{
    const SocketImpl * impl = socket.impl();
    if (impl == nullptr)
        return;

    SocketNotifierPtr notifier;
    {
        ScopedLock lock(mutex);
        SocketNotifierMap::iterator it = notifiers.find(impl->sockfd());
        if (it != notifiers.end())
            notifier = it->second;

        if (notifier && notifier->onlyHas(observer))
        {
            notifiers.erase(impl->sockfd());
            poll_set.remove(socket);
        }
    }

    if (notifier)
    {
        notifier->removeObserverIfExist(observer);
        if (notifier->size() > 0 && socket.impl()->sockfd() > 0)
        {
            int mode = 0;
            if (notifier->accepts(*rnf))
                mode |= PollSet::POLL_READ;
            if (notifier->accepts(*wnf))
                mode |= PollSet::POLL_WRITE;
            if (notifier->accepts(*enf))
                mode |= PollSet::POLL_ERROR;
            poll_set.update(socket, mode);
        }
    }
}


bool SocketReactor::has(const Socket & socket) const
{
    return poll_set.has(socket);
}


void SocketReactor::onTimeout()
{
    dispatch(*tnf);
}


void SocketReactor::onIdle()
{
    dispatch(*inf);
}


void SocketReactor::onShutdown()
{
    dispatch(*snf);
}


void SocketReactor::onBusy()
{
}

void SocketReactor::dispatch(const Socket & socket, const Notification & notification)
{
    SocketNotifierPtr notifier = getNotifier(socket);
    if (!notifier)
        return;
    dispatch(notifier, notification);
}


void SocketReactor::dispatch(const Notification & notification)
{
    std::vector<SocketNotifierPtr> copied;
    {
        ScopedLock lock(mutex);
        copied.reserve(notifiers.size());
        for (auto & notifier : notifiers)
            copied.push_back(notifier.second);
    }
    for (auto & notifier : copied)
    {
        dispatch(notifier, notification);
    }
}


void SocketReactor::dispatch(SocketNotifierPtr & notifier, const Notification & notification)
{
    try
    {
        const auto & sock = notifier->getSocket();
        const auto socket_name = sock.isStream() ? sock.address().toString() /// use local address for server socket
                                                 : sock.peerAddress().toString(); /// use remote address for server socket
        LOG_TRACE(log, "Dispatch event {} for {} ", notification.name(), socket_name);
        notifier->dispatch(notification);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to dispatch socket event " + notification.name());
    }
}


AsyncSocketReactor::AsyncSocketReactor(const Poco::Timespan & timeout, const std::string & name_) : SocketReactor(timeout), name(name_)
{
    startup();
}

void AsyncSocketReactor::startup()
{
    thread.start(*this);
}

void AsyncSocketReactor::run()
{
    if (!name.empty())
    {
        setThreadName(name.c_str());
        Poco::Thread::current()->setName(name);
    }
    SocketReactor::run();
}

AsyncSocketReactor::~AsyncSocketReactor()
{
    try
    {
        this->stop();
        thread.join();
    }
    catch (...)
    {
    }
}

void AsyncSocketReactor::onIdle()
{
    SocketReactor::onIdle();
    Poco::Thread::yield();
}

}
