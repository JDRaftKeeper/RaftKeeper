#pragma once

#include <Poco/Net/SocketReactor.h>
#include <Poco/Net/SocketNotification.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/NObserver.h>
#include <Poco/Thread.h>
#include <Poco/SharedPtr.h>
#include <Common/setThreadName.h>


using Poco::Net::Socket;
using Poco::Net::SocketReactor;
using Poco::Net::ReadableNotification;
using Poco::Net::ShutdownNotification;
using Poco::Net::ServerSocket;
using Poco::Net::StreamSocket;
using Poco::NObserver;
using Poco::AutoPtr;
using Poco::Thread;


namespace DB {


    template <class SR>
    class ParallelSocketReactor: public SR
    {
    public:
        using Ptr = Poco::SharedPtr<ParallelSocketReactor>;

        ParallelSocketReactor()
        {
            _thread.start(*this);
        }
	
        explicit ParallelSocketReactor(const Poco::Timespan& timeout):
            SR(timeout)
        {
            _thread.start(*this);
        }

        void run() override
        {
            static int thread_id = 0;
            static String THREAD_NAME_PREFIX = "NIO-Handler-";
            setThreadName((THREAD_NAME_PREFIX + std::to_string(thread_id++)).data());
            SR::run();
        }
	
        ~ParallelSocketReactor() override
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
