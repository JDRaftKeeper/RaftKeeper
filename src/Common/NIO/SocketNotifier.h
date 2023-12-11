/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#pragma once

#include <Poco/Net/Net.h>
#include <Poco/Net/Socket.h>
#include <Poco/RefCountedObject.h>

#include <Common/NIO/NotificationCenter.h>
#include <Common/NIO/Observer.h>

namespace RK
{

class SocketReactor;
class SocketNotification;

using Poco::Net::Socket;


/// This class is used internally by SocketReactor
/// to notify registered event handlers of socket events.
class Net_API SocketNotifier : public Poco::RefCountedObject
{
public:
    /// Creates the SocketNotifier for the given socket.
    explicit SocketNotifier(const Socket & socket);

    /// Adds the given observer.
    bool addObserverIfNotExist(SocketReactor * reactor, const AbstractObserver & observer);

    /// Removes the given observer.
    bool removeObserverIfExist(SocketReactor * reactor, const AbstractObserver & observer);

    /// Returns true if the given observer is registered.
    bool hasObserver(const AbstractObserver & observer) const;

    /// Returns true if only has the given observer
    bool onlyHas(const AbstractObserver & observer) const;

    /// Returns true if there is at least one observer for the given notification.
    bool accepts(SocketNotification * pNotification);

    /// Dispatches the notification to all observers.
    void dispatch(SocketNotification * pNotification);

    /// Returns true if there are subscribers.
    [[maybe_unused]] bool hasObservers() const;

    /// Returns the number of subscribers;
    std::size_t countObservers() const;

protected:
    ~SocketNotifier() override = default;

private:
    using MutexType = Poco::FastMutex;
    using ScopedLock = MutexType::ScopedLock;

    NotificationCenter nc;
    Socket socket;
};


inline bool SocketNotifier::accepts(SocketNotification * pNotification)
{
    return nc.accept(pNotification);
}


inline bool SocketNotifier::hasObserver(const AbstractObserver & observer) const
{
    return nc.hasObserver(observer);
}

inline bool SocketNotifier::onlyHas(const AbstractObserver & observer) const
{
    return nc.onlyHas(observer);
}


[[maybe_unused]] inline bool SocketNotifier::hasObservers() const
{
    return nc.hasObservers();
}


inline std::size_t SocketNotifier::countObservers() const
{
    return nc.countObservers();
}


}
