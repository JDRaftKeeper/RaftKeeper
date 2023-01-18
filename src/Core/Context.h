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
#    include "config_core.h"
#endif


namespace DB
{

struct ContextShared;
class Context;

class SvsKeeperDispatcher;
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

    std::shared_ptr<SvsKeeperDispatcher> & getSvsKeeperStorageDispatcher() const;
    void initializeServiceKeeperStorageDispatcher();
    void shutdownServiceKeeperStorageDispatcher();
    void updateServiceKeeperConfiguration(const Poco::Util::AbstractConfiguration & config);

    using ConfigReloadCallback = std::function<void()>;
    void setConfigReloadCallback(ConfigReloadCallback && callback);
    void reloadConfig() const;

    void shutdown();

private:
    Context();

    void initGlobal();

    mutable std::recursive_mutex dispatcher_mutex;
    std::shared_ptr<SvsKeeperDispatcher> & dispatcher;

    mutable std::recursive_mutex config_mutex;
    ConfigurationPtr config;

    Context::ConfigReloadCallback config_reload_callback;

};

}
