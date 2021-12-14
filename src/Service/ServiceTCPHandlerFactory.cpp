#include "ServiceTCPHandlerFactory.h"
#include <common/logger_useful.h>

namespace DB
{
TCPServerConnection * ServiceTCPHandlerFactory::createConnection(const StreamSocket & socket)
{
    LOG_INFO(log, "new ServiceTCPHandler");
    return static_cast<TCPServerConnection *>(new ServiceTCPHandler(server, socket));
}

ServiceTCPHandlerFactory::ServiceTCPHandlerFactory(IServer & server_, bool secure_, bool parse_proxy_protocol_)
    : server(server_), parse_proxy_protocol(parse_proxy_protocol_), secure(secure_), log(&Poco::Logger::get("ServiceTCPHandlerFactory"))
{
    log = &Poco::Logger::get("ServiceTCPHandlerFactory");
    LOG_INFO(log, "new ServiceTCPHandlerFactory");
}

}
