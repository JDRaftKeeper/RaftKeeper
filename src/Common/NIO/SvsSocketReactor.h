#pragma once

#include <Poco/NObserver.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/SharedPtr.h>
#include <Poco/Thread.h>
#include <Common/setThreadName.h>
#include <Poco/Net/SocketNotification.h>
#include <Poco/Net/SocketReactor.h>


namespace RK {

    using namespace Poco::Net;

    template <class SR>
    class SvsSocketReactor : public SR
    {
    public:
        using Ptr = Poco::SharedPtr<SvsSocketReactor>;

        SvsSocketReactor(const std::string& name = "")
        {
            _thread.start(*this);
            if (!name.empty())
                _thread.setName(name);
        }

        SvsSocketReactor(const Poco::Timespan& timeout, const std::string& name = ""):
            SR(timeout)
        {
            _thread.start(*this);
            if (!name.empty())
                _thread.setName(name);
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
    };

}
