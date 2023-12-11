#pragma once

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/SharedPtr.h>
#include <Poco/Thread.h>

#include <Common/NIO/Observer.h>
#include <Common/NIO/SocketNotification.h>
#include <Common/NIO/SocketReactor.h>
#include <Common/setThreadName.h>


namespace RK
{

template <class SR>
class SvsSocketReactor : public SR
{
public:
    using Ptr = Poco::SharedPtr<SvsSocketReactor>;

    SvsSocketReactor(const std::string & name = "")
    {
        _thread.start(*this);
    }

    SvsSocketReactor(const Poco::Timespan & timeout, const std::string & name = "") : SR(timeout)
    {
        _thread.start(*this);
        if (!name.empty())
        {
            _thread.setName(name);
            setThreadName(name.c_str());
        }
    }

    ~SvsSocketReactor() override
    {
        try
        {
            this->stop();
            _thread.join();
        }
        catch (...)
        {
        }
    }

protected:
    void onIdle() override
    {
        SR::onIdle();
        Poco::Thread::yield();
    }

private:
    Poco::Thread _thread;
    std::string _name;
};

}
