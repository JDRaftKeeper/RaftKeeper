// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
#pragma once

#include <cstddef>
#include <set>
#include <vector>

#include <Poco/Foundation.h>
#include <Poco/Mutex.h>
#include <Poco/SharedPtr.h>

#include <NIO/Observer.h>


namespace RK
{

/// A NotificationCenter is essentially a notification dispatcher.
/// It notifies all observers of notifications meeting specific criteria.
/// This information is encapsulated in Notification objects.
/// Client objects register themselves with the notification center as observers of
/// specific notifications posted by other objects. When an event occurs, an object
/// posts an appropriate notification to the notification center. The notification
/// center invokes the registered method on each matching observer, passing the notification
/// as argument.
class NotificationCenter
{
public:
    using Mutex = Poco::Mutex;

    NotificationCenter() = default;

    ~NotificationCenter() = default;

    /// Registers an observer with the NotificationCenter.
    /// Usage:
    ///     Observer<MyClass, MyNotification> obs(*this, &MyClass::handleNotification);
    ///     notificationCenter.addObserver(obs);
    ///
    /// Alternatively, the Observer template class can be used instead of Observer.
    bool addObserverIfNotExist(const AbstractObserver & observer);

    /// Unregisters an observer with the NotificationCenter.
    bool removeObserverIfExist(const AbstractObserver & observer);

    /// Returns true if the observer is registered with this NotificationCenter.
    bool hasObserver(const AbstractObserver & observer) const;

    /// Returns true if only has the given observer
    bool onlyHas(const AbstractObserver & observer) const;

    /// Posts a notification to the NotificationCenter.
    /// The NotificationCenter then delivers the notification
    /// to all interested observers.
    /// If an observer throws an exception, dispatching terminates
    /// and the exception is rethrown to the caller.
    void postNotification(const Notification & notification);

    /// Returns true iff there is at least one registered observer.
    ///
    /// Can be used to improve performance if an expensive notification
    /// shall only be created and posted if there are any observers.
    bool hasObservers() const;

    /// Returns the number of registered observers.
    std::size_t size() const;

    bool accept(const Notification & notification) const;

private:
    using AbstractObserverPtr = Poco::SharedPtr<AbstractObserver>;
    using Observers = std::vector<AbstractObserverPtr>;

    mutable Poco::Mutex mutex;
    Observers observers; /// All observers
};


}
