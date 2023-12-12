/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*/
#pragma once

#include <Poco/Net/Net.h>
#include <Poco/Net/Socket.h>

#include <Common/NIO/NotificationCenter.h>
#include <Common/NIO/Observer.h>
#include <Common/NIO/SocketReactor.h>

namespace RK
{

/// This class is used internally by SocketReactor
/// to notify registered event handlers of socket events.
class SocketNotifier
{
public:
    using Socket = Poco::Net::Socket;
    using MutexType = Poco::FastMutex;
    using ScopedLock = MutexType::ScopedLock;

    /// Creates the SocketNotifier for the given socket.
    explicit SocketNotifier(const Socket & socket);

    /// Adds the given observer.
    bool addObserverIfNotExist(const AbstractObserver & observer);

    /// Removes the given observer.
    bool removeObserverIfExist(const AbstractObserver & observer);

    /// Returns true if the given observer is registered.
    bool hasObserver(const AbstractObserver & observer) const;

    /// Returns true if only has the given observer
    bool onlyHas(const AbstractObserver & observer) const;

    /// Returns true if there is at least one observer for the given notification.
    bool accepts(const Notification & notification);

    /// Dispatches the notification to all observers.
    void dispatch(const Notification & notification);

    /// Returns true if there are subscribers.
    [[maybe_unused]] bool hasObservers() const;

    /// Returns the number of subscribers;
    std::size_t size() const;

    ~SocketNotifier() = default;

private:
    NotificationCenter nc;
    Socket socket;
};

using SocketNotifierPtr = std::shared_ptr<SocketNotifier>;


inline bool SocketNotifier::accepts(const Notification & notification)
{
    return nc.accept(notification);
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


inline std::size_t SocketNotifier::size() const
{
    return nc.size();
}


}
