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
#include "Context.h"
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <Service/KeeperDispatcher.h>
#include <Poco/Mutex.h>
#include <Poco/Util/Application.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/StackTrace.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>



namespace RK
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Context & Context::get()
{
    static Context context;
    return context;
}

void Context::setConfig(const ConfigurationPtr & config_)
{
    std::lock_guard lock(dispatcher_mutex);
    config = config_;
}

std::shared_ptr<KeeperDispatcher> Context::getDispatcher() const
{
    return dispatcher;
}

void Context::initializeDispatcher()
{
    std::lock_guard lock(dispatcher_mutex);
    if (dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize keeper multiple times");

    const auto & config_ = getConfigRef();
    dispatcher = std::make_shared<KeeperDispatcher>();
    dispatcher->initialize(config_);
}

void Context::shutdownDispatcher()
{
    std::lock_guard lock(dispatcher_mutex);
    if (dispatcher)
    {
        dispatcher->shutdown();
        dispatcher.reset();
    }
}

void Context::updateServiceKeeperConfiguration(const Poco::Util::AbstractConfiguration & config_)
{
    std::lock_guard lock(dispatcher_mutex);
    if (!dispatcher)
        return;
    dispatcher->updateConfiguration(config_);
}

void Context::setConfigReloadCallback(Context::ConfigReloadCallback && callback)
{
    config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    if (!config_reload_callback)
        throw Exception("Can't reload config because config_reload_callback is not set.", ErrorCodes::LOGICAL_ERROR);
    config_reload_callback();
}

void Context::shutdown()
{
    shutdownDispatcher();
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    std::lock_guard lock(config_mutex);
    return config ? *config : Poco::Util::Application::instance().config();
}

}
