/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*/
#pragma once

#include <Common/NIO/Notification.h>

namespace RK
{

/// The base class for all instantiations of
/// the Observer template classes.
class AbstractObserver
{
public:
    AbstractObserver() = default;
    AbstractObserver(const AbstractObserver &) = default;
    virtual ~AbstractObserver() = default;

    AbstractObserver & operator=(const AbstractObserver &) = default;

    virtual void notify(const Notification & notification) const = 0;
    virtual bool accepts(const Notification & notification) const = 0;

    virtual bool equals(const AbstractObserver & observer) const = 0;
    virtual AbstractObserver * clone() const = 0;

    virtual void disable() = 0;
};


/// This template class implements an adapter that sits between
/// a NotificationCenter and an object receiving notifications
/// from it. It is quite similar in concept to the
/// RunnableAdapter, but provides some NotificationCenter
/// specific additional methods.
/// See the NotificationCenter class for information on how
/// to use this template class.
///
/// This class template is quite similar to the Observer class
/// template. The only difference is that the Observer
/// expects the callback function to accept a const std::shared_ptr&
/// instead of a plain pointer as argument, thus simplifying memory
/// management.
template <class C, class N>
class Observer : public AbstractObserver
{
public:
    using Callback = void (C::*)(const Notification &);

    Observer(C & object_, Callback method_) : object(&object_), method(method_) { }
    Observer(const Observer & observer) : AbstractObserver(observer), object(observer.object), method(observer.method) { }

    ~Observer() override = default;

    Observer & operator=(const Observer & observer)
    {
        if (&observer != this)
        {
            object = observer.object;
            method = observer.method;
        }
        return *this;
    }

    void notify(const Notification & notification) const override
    {
        if (const N * casted = dynamic_cast<const N *>(&notification))
        {
            Poco::Mutex::ScopedLock lock(mutex);
            if (object)
            {
                (object->*method)(*casted);
            }
        }
    }

    bool equals(const AbstractObserver & other) const override
    {
        const Observer * casted = dynamic_cast<const Observer *>(&other);
        return casted && casted->object == object && casted->method == method;
    }

    bool accepts(const Notification & notification) const override { return dynamic_cast<const N *>(&notification); }

    AbstractObserver * clone() const override { return new Observer(*this); }

    void disable() override
    {
        Poco::Mutex::ScopedLock lock(mutex);
        object = nullptr;
    }

private:
    Observer() = default;

    C * object;
    Callback method;

    mutable Poco::Mutex mutex;
};

}
