#include "Context.h"
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <Service/SvsKeeperDispatcher.h>
#include <Poco/Mutex.h>
#include <Poco/Util/Application.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/StackTrace.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>



namespace DB
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

std::shared_ptr<SvsKeeperDispatcher> & Context::getSvsKeeperStorageDispatcher() const
{
    return dispatcher;
}

void Context::initializeServiceKeeperStorageDispatcher()
{
    std::lock_guard lock(dispatcher_mutex);
    if (dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize keeper multiple times");

    const auto & config_ = getConfigRef();
    dispatcher = std::make_shared<SvsKeeperDispatcher>();
    dispatcher->initialize(config_);
}

void Context::shutdownServiceKeeperStorageDispatcher()
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
    dispatcher->shutdown();
}
const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    std::lock_guard lock(config_mutex);
    return config ? *config : Poco::Util::Application::instance().config();
}

}
