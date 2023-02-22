#pragma once

#include <Service/SocketReactor.h>
#include <Service/SocketNotification.h>
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
