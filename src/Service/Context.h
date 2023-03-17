#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "common/types.h"

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
