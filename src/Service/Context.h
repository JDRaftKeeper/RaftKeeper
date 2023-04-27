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
#include <common/types.h>

namespace RK
{

class Context;

class KeeperDispatcher;
using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/** A set of known objects that can be used in global.
  */
class Context
{
public:
    static Context & get();
    ~Context() = default;

    /// Global application configuration settings.
    static const Poco::Util::AbstractConfiguration & getConfigRef();
    std::shared_ptr<KeeperDispatcher> getDispatcher() const;

    void initializeDispatcher();
    void shutdownDispatcher();

    void updateClusterConfiguration(const Poco::Util::AbstractConfiguration & config);
    void shutdown();

private:
    Context() = default;

    mutable std::recursive_mutex dispatcher_mutex;
    std::shared_ptr<KeeperDispatcher> dispatcher;
};

}
