/**
 * Copyright 2016-2023 ClickHouse, Inc.
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

#include "IServer.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/TCPServer.h"
#include "Poco/Timestamp.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/ServerApplication.h"
#include <daemon/BaseDaemon.h>

using Poco::Util::Application;
using Poco::Util::ServerApplication;

namespace RK
{
class Server : public BaseDaemon, public IServer
{
public:
    using ServerApplication::run;

    Poco::Util::LayeredConfiguration & config() const override { return BaseDaemon::config(); }
    Poco::Logger & logger() const override { return BaseDaemon::logger(); }

    Context & context() const override { return *global_context_ptr; }

    bool isCancelled() const override { return BaseDaemon::isCancelled(); }
    int run() override;

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    void initialize(Application & self) override;
    void uninitialize() override;
    int main(const std::vector<std::string> & listen_port) override;

private:
    Context * global_context_ptr = nullptr;


    using CreateServerFunc = std::function<void(UInt16)>;
    void createServer(const std::string & listen_host, int port, bool listen_try, CreateServerFunc && func) const;
};

}
