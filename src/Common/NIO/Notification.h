#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/Foundation.h>
#include <Poco/Mutex.h>
#include <Poco/RefCountedObject.h>


namespace RK
{

/// The base class for all notification classes used
/// with the NotificationCenter and the NotificationQueue
/// classes.
/// The Notification class can be used with the AutoPtr
/// template class.
class Notification : public Poco::RefCountedObject
{
public:
    using Ptr = Poco::AutoPtr<Notification>;

    Notification() = default;

    virtual std::string name() const { return typeid(*this).name(); }

protected:
    virtual ~Notification() override = default;
};

}
