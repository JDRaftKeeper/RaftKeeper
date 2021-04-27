#pragma once

#include <string>
#include <Server/IServer.h>
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/TCPServerConnection.h"
#include "Poco/Net/TCPServerConnectionFactory.h"
#include <Poco/Net/NetException.h>
#include "ServiceTCPHandler.h"

using Poco::Net::StreamSocket;
using Poco::Net::TCPServerConnection;

namespace DB
{
class ServiceTCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
public:
    IServer & server;
    bool parse_proxy_protocol = false;
    bool secure;
    Poco::Logger * log;

    ServiceTCPHandlerFactory(IServer & server_, bool secure_, bool parse_proxy_protocol_);

    TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override;
};

}
