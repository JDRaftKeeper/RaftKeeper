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

#include <common/types.h>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

#if !defined(ARCADIA_BUILD)
#    include <Core/config_core.h>
#endif


namespace RK
{

class Context;

class KeeperDispatcher;
using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/** A set of known objects that can be used in the query.
  * Consists of a shared part (always common to all sessions and queries)
  *  and copied part (which can be its own for each session or query).
  *
  * Everything is encapsulated for all sorts of checks and locks.
  */
class Context
{
public:
    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    static Context & get();
    ~Context() = default;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    std::shared_ptr<KeeperDispatcher> getDispatcher() const;
    void initializeDispatcher();
    void shutdownDispatcher();
    void updateServiceKeeperConfiguration(const Poco::Util::AbstractConfiguration & config);

    using ConfigReloadCallback = std::function<void()>;
    void setConfigReloadCallback(ConfigReloadCallback && callback);
    void reloadConfig() const;

    void shutdown();

private:
    Context() = default;

    mutable std::recursive_mutex dispatcher_mutex;
    std::shared_ptr<KeeperDispatcher> dispatcher;

    mutable std::recursive_mutex config_mutex;
    ConfigurationPtr config;

    Context::ConfigReloadCallback config_reload_callback;

};

}
