/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*/
#pragma once

#include <Poco/Foundation.h>
#include <Poco/Mutex.h>


namespace RK
{

/// The base class for all notification classes.
class Notification
{
public:
    Notification() = default;
    virtual std::string name() const { return typeid(*this).name(); }

protected:
    virtual ~Notification() = default;
};

using NotificationPtr = std::shared_ptr<Notification>;

}
