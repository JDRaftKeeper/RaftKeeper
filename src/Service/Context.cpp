#include "Context.h"
#include <memory>
#include <Service/KeeperDispatcher.h>
#include <Poco/Util/Application.h>
#include <Common/Config/ConfigProcessor.h>


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

std::shared_ptr<KeeperDispatcher> Context::getDispatcher() const
{
    return dispatcher;
}

void Context::initializeDispatcher()
{
    std::lock_guard lock(dispatcher_mutex);
    if (dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize keeper multiple times");

    dispatcher = std::make_shared<KeeperDispatcher>();
    dispatcher->initialize(getConfigRef());
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

void Context::updateClusterConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(dispatcher_mutex);
    if (!dispatcher)
        return;
    dispatcher->updateConfiguration(config);
}

void Context::shutdown()
{
    shutdownDispatcher();
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef()
{
    return Poco::Util::Application::instance().config();
}

}
