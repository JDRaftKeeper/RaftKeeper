/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#pragma once

#include "Poco/Net/Net.h"
#include "Poco/Net/Socket.h"
#include "Poco/RefCountedObject.h"
#include <Common/NIO/NotificationCenter.h>
#include "Poco/Observer.h"

namespace RK {

class SocketReactor;
class SocketNotification;

using Poco::Net::Socket;


class Net_API SocketNotifier: public Poco::RefCountedObject
/// This class is used internally by SocketReactor
/// to notify registered event handlers of socket events.
{
public:
    explicit SocketNotifier(const Socket& socket);
    /// Creates the SocketNotifier for the given socket.

    bool addObserverIfNotExist(SocketReactor* pReactor, const Poco::AbstractObserver& observer);
    /// Adds the given observer.

    bool removeObserverIfExist(SocketReactor* pReactor, const Poco::AbstractObserver& observer);
    /// Removes the given observer.

    bool hasObserver(const Poco::AbstractObserver& observer) const;
    /// Returns true if the given observer is registered.

    bool onlyHas(const Poco::AbstractObserver& observer) const;
    /// Returns true if only has the given observer

    bool accepts(SocketNotification* pNotification);
    /// Returns true if there is at least one observer for the given notification.

    void dispatch(SocketNotification* pNotification);
    /// Dispatches the notification to all observers.

    bool hasObservers() const;
    /// Returns true if there are subscribers.

    std::size_t countObservers() const;
    /// Returns the number of subscribers;

protected:
    ~SocketNotifier() override = default;
    /// Destroys the SocketNotifier.

private:
    using MutexType =  Poco::FastMutex;
    using ScopedLock = MutexType::ScopedLock;

    NotificationCenter _nc;
    Socket                   _socket;
};


inline bool SocketNotifier::accepts(SocketNotification* pNotification)
{
    return _nc.accept(pNotification);
}


inline bool SocketNotifier::hasObserver(const Poco::AbstractObserver& observer) const
{
    return _nc.hasObserver(observer);
}

inline bool SocketNotifier::onlyHas(const Poco::AbstractObserver& observer) const
{
    return _nc.onlyHas(observer);
}


inline bool SocketNotifier::hasObservers() const
{
    return _nc.hasObservers();
}


inline std::size_t SocketNotifier::countObservers() const
{
    return _nc.countObservers();
}


}
