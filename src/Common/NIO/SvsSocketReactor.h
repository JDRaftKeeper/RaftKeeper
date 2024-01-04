#pragma once

#include <Common/NIO/SocketReactor.h>
#include <Common/NIO/SocketNotification.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/NObserver.h>
#include <Poco/Thread.h>
#include <Poco/SharedPtr.h>
#include <Common/setThreadName.h>



namespace RK {

template <class SR>
class SvsSocketReactor : public SR
{
public:
    using Ptr = Poco::SharedPtr<SvsSocketReactor>;

    SvsSocketReactor(const std::string& name = "") : _name(name)
    {
        _thread.start(*this);
    }

    SvsSocketReactor(const Poco::Timespan& timeout, const std::string& name = ""):
        SR(timeout), _name(name)
    {
        _thread.start(*this);
    }

    void run() override
    {
        if (!_name.empty())
        {
            _thread.setName(_name);
            setThreadName(_name.c_str());
        }
        SR::run();
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
