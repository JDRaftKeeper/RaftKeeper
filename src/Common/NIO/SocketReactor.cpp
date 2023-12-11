/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#include <Poco/ErrorHandler.h>
#include <Poco/Exception.h>
#include <Poco/Thread.h>

#include <Common/NIO/SocketNotification.h>
#include <Common/NIO/SocketNotifier.h>
#include <Common/NIO/SocketReactor.h>


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
    : stopped(false)
    , timeout(timeout_)
    , rnf(new ReadableNotification(this))
    , wnf(new WritableNotification(this))
    , enf(new ErrorNotification(this))
    , tnf(new TimeoutNotification(this))
    , inf(new IdleNotification(this))
    , snf(new ShutdownNotification(this))
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
                    PollSet::SocketModeMap::iterator it = sm.begin();
                    PollSet::SocketModeMap::iterator end = sm.end();
                    for (; it != end; ++it)
                    {
                        if (it->second & PollSet::POLL_READ)
                        {
                            dispatch(it->first, rnf);
                            readable = true;
                        }
                        if (it->second & PollSet::POLL_WRITE)
                            dispatch(it->first, wnf);
                        if (it->second & PollSet::POLL_ERROR)
                            dispatch(it->first, enf);
                    }
                }
                if (!readable)
                    onTimeout();
            }
        }
        catch (Exception & exc)
        {
            ErrorHandler::handle(exc);
        }
        catch (std::exception & exc)
        {
            ErrorHandler::handle(exc);
        }
        catch (...)
        {
            ErrorHandler::handle();
        }
    }
    onShutdown();
}


bool SocketReactor::hasSocketHandlers()
{
    if (!poll_set.empty())
    {
        ScopedLock lock(mutex);
        for (auto & p : handlers)
        {
            if (p.second->accepts(rnf) || p.second->accepts(wnf) || p.second->accepts(enf))
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
    NotifierPtr notifier = getNotifier(socket, true);
    if (notifier->addObserverIfNotExist(this, observer))
    {
        int mode = 0;
        if (notifier->accepts(rnf))
            mode |= PollSet::POLL_READ;
        if (notifier->accepts(wnf))
            mode |= PollSet::POLL_WRITE;
        if (notifier->accepts(enf))
            mode |= PollSet::POLL_ERROR;
        if (mode)
            poll_set.add(socket, mode);
    }
}

void SocketReactor::addEventHandlers(const Socket & socket, const std::vector<AbstractObserver *> & observers)
{
    NotifierPtr notifier = getNotifier(socket, true);

    int mode = 0;
    for (auto * observer : observers)
    {
        notifier->addObserverIfNotExist(this, *observer);

        if (notifier->accepts(rnf))
            mode |= PollSet::POLL_READ;
        if (notifier->accepts(wnf))
            mode |= PollSet::POLL_WRITE;
        if (notifier->accepts(enf))
            mode |= PollSet::POLL_ERROR;
    }
    if (mode)
        poll_set.add(socket, mode);
}


bool SocketReactor::hasEventHandler(const Socket & socket, const AbstractObserver & observer)
{
    NotifierPtr notifier = getNotifier(socket);
    if (!notifier)
        return false;
    if (notifier->hasObserver(observer))
        return true;
    return false;
}


SocketReactor::NotifierPtr SocketReactor::getNotifier(const Socket & socket, bool makeNew)
{
    const SocketImpl * impl = socket.impl();
    if (impl == nullptr)
        return nullptr;
    poco_socket_t sock_fd = impl->sockfd();
    ScopedLock lock(mutex);

    EventHandlerMap::iterator it = handlers.find(sock_fd);
    if (it != handlers.end())
        return it->second;
    else if (makeNew)
        return (handlers[sock_fd] = new SocketNotifier(socket));

    return nullptr;
}


void SocketReactor::removeEventHandler(const Socket & socket, const AbstractObserver & observer)
{
    const SocketImpl * impl = socket.impl();
    if (impl == nullptr)
        return;

    NotifierPtr notifier;
    {
        ScopedLock lock(mutex);
        EventHandlerMap::iterator it = handlers.find(impl->sockfd());
        if (it != handlers.end())
            notifier = it->second;

        if (notifier && notifier->onlyHas(observer))
        {
            handlers.erase(impl->sockfd());
            poll_set.remove(socket);
        }
    }

    if (notifier)
    {
        notifier->removeObserverIfExist(this, observer);
        if (notifier->countObservers() > 0 && socket.impl()->sockfd() > 0)
        {
            int mode = 0;
            if (notifier->accepts(rnf))
                mode |= PollSet::POLL_READ;
            if (notifier->accepts(wnf))
                mode |= PollSet::POLL_WRITE;
            if (notifier->accepts(enf))
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
    dispatch(tnf);
}


void SocketReactor::onIdle()
{
    dispatch(inf);
}


void SocketReactor::onShutdown()
{
    dispatch(snf);
}


void SocketReactor::onBusy()
{
}


void SocketReactor::dispatch(const Socket & socket, SocketNotification * pNotification)
{
    NotifierPtr notifier = getNotifier(socket);
    if (!notifier)
        return;
    dispatch(notifier, pNotification);
}


void SocketReactor::dispatch(SocketNotification * pNotification)
{
    std::vector<NotifierPtr> delegates;
    {
        ScopedLock lock(mutex);
        delegates.reserve(handlers.size());
        for (auto & handler : handlers)
            delegates.push_back(handler.second);
    }
    for (auto & delegate : delegates)
    {
        dispatch(delegate, pNotification);
    }
}


void SocketReactor::dispatch(NotifierPtr & notifier, SocketNotification * pNotification)
{
    try
    {
        notifier->dispatch(pNotification);
    }
    catch (Exception & exc)
    {
        ErrorHandler::handle(exc);
    }
    catch (std::exception & exc)
    {
        ErrorHandler::handle(exc);
    }
    catch (...)
    {
        ErrorHandler::handle();
    }
}


}
