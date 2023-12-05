//
// NotificationCenter.cpp
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationCenter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
#include <Common/NIO/NotificationCenter.h>


namespace RK
{

bool NotificationCenter::addObserverIfNotExist(const AbstractObserver & observer)
{
    Mutex::ScopedLock lock(_mutex);
    for (const auto & p : _observers)
        if (observer.equals(*p))
            return false;
    _observers.push_back(observer.clone());
    return true;
}


bool NotificationCenter::removeObserverIfExist(const AbstractObserver & observer)
{
    Mutex::ScopedLock lock(_mutex);
    for (ObserverList::iterator it = _observers.begin(); it != _observers.end(); ++it)
    {
        if (observer.equals(**it))
        {
            (*it)->disable();
            _observers.erase(it);
            return true;
        }
    }
    return false;
}


bool NotificationCenter::hasObserver(const AbstractObserver & observer) const
{
    Mutex::ScopedLock lock(_mutex);
    for (const auto & p : _observers)
        if (observer.equals(*p))
            return true;

    return false;
}
bool NotificationCenter::onlyHas(const RK::NotificationCenter::AbstractObserver & observer) const
{
    Mutex::ScopedLock lock(_mutex);
    return _observers.size() == 1 && observer.equals(*_observers[0]);
}


bool NotificationCenter::accept(Poco::Notification * pNotification) const
{
    Mutex::ScopedLock lock(_mutex);
    for (const auto & observer : _observers)
    {
        if (observer->accepts(pNotification))
            return true;
    }
    return false;
}


void NotificationCenter::postNotification(Notification::Ptr pNotification)
{
    poco_check_ptr(pNotification);

    Poco::ScopedLockWithUnlock<Mutex> lock(_mutex);
    ObserverList copied(_observers);
    lock.unlock();
    for (auto & p : copied)
    {
        p->notify(pNotification);
    }
}


bool NotificationCenter::hasObservers() const
{
    Mutex::ScopedLock lock(_mutex);
    return !_observers.empty();
}


std::size_t NotificationCenter::countObservers() const
{
    Mutex::ScopedLock lock(_mutex);
    return _observers.size();
}

}
