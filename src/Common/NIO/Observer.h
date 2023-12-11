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
    AbstractObserver(const AbstractObserver & observer) = default;
    virtual ~AbstractObserver() = default;

    AbstractObserver & operator=(const AbstractObserver & observer) = default;

    virtual void notify(Notification * nf) const = 0;
    virtual bool equals(const AbstractObserver & observer) const = 0;
    virtual bool accepts(Notification * nf) const = 0;
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
/// expects the callback function to accept a const AutoPtr&
/// instead of a plain pointer as argument, thus simplifying memory
/// management.
template <class C, class N>
class Observer : public AbstractObserver
{
public:
    using NotificationPtr = Poco::AutoPtr<N>;
    using Callback = void (C::*)(const NotificationPtr &);

    Observer(C & object_, Callback method_) : object(&object_), method(method_) { }
    Observer(const Observer & observer) : AbstractObserver(observer), object(observer.object), method(observer.method) { }

    ~Observer() = default;

    Observer & operator=(const Observer & observer)
    {
        if (&observer != this)
        {
            object = observer.object;
            method = observer.method;
        }
        return *this;
    }

    void notify(Notification * nf) const
    {
        Poco::Mutex::ScopedLock lock(mutex);
        if (object)
        {
            N * casted = dynamic_cast<N *>(nf);
            if (casted)
            {
                NotificationPtr ptr(casted, true);
                (object->*method)(ptr);
            }
        }
    }

    bool equals(const AbstractObserver & other) const
    {
        const Observer * casted = dynamic_cast<const Observer *>(&other);
        return casted && casted->object == object && casted->method == method;
    }

    bool accepts(Notification * nf) const { return dynamic_cast<N *>(nf); }

    AbstractObserver * clone() const { return new Observer(*this); }

    void disable()
    {
        Poco::Mutex::ScopedLock lock(mutex);
        object = 0;
    }

private:
    Observer() = default;

    C * object;
    Callback method;

    mutable Poco::Mutex mutex;
};

}
