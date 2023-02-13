/**
 * Copyright 2021-2023 JD.com, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
