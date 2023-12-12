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
    Mutex::ScopedLock lock(mutex);
    for (const auto & p : observers)
        if (observer.equals(*p))
            return false;
    observers.push_back(observer.clone());
    return true;
}


bool NotificationCenter::removeObserverIfExist(const AbstractObserver & observer)
{
    Mutex::ScopedLock lock(mutex);
    for (auto it = observers.begin(); it != observers.end(); ++it)
    {
        if (observer.equals(**it))
        {
            (*it)->disable();
            observers.erase(it);
            return true;
        }
    }
    return false;
}


bool NotificationCenter::hasObserver(const AbstractObserver & observer) const
{
    Mutex::ScopedLock lock(mutex);
    for (const auto & p : observers)
        if (observer.equals(*p))
            return true;

    return false;
}


bool NotificationCenter::onlyHas(const AbstractObserver & observer) const
{
    Mutex::ScopedLock lock(mutex);
    return observers.size() == 1 && observer.equals(*observers[0]);
}


bool NotificationCenter::accept(const Notification & notification) const
{
    Mutex::ScopedLock lock(mutex);
    for (const auto & observer : observers)
    {
        if (observer->accepts(notification))
            return true;
    }
    return false;
}


void NotificationCenter::postNotification(const Notification & notification)
{
    Observers copied;
    {
        Mutex::ScopedLock lock(mutex);
        for (auto & observer : observers)
        {
            if (observer->accepts(notification))
                copied.push_back(observer);
        }
    }

    for (auto & observer : copied)
        observer->notify(notification);
}


bool NotificationCenter::hasObservers() const
{
    Mutex::ScopedLock lock(mutex);
    return !observers.empty();
}


std::size_t NotificationCenter::size() const
{
    Mutex::ScopedLock lock(mutex);
    return observers.size();
}

}
