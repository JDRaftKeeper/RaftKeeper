#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTPHandlerFactory.h>

#include "Server/IServer.h"
#include "SvsKeeperMetricsWriter.h"

namespace DB
{

class SvsKeeperPrometheusRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
    SvsKeeperMetricsWriterPtr metrics_writer;

public:
    explicit SvsKeeperPrometheusRequestHandler(IServer & server_, SvsKeeperMetricsWriterPtr metrics_writer_)
    : server(server_)
    , metrics_writer(metrics_writer_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

HTTPRequestHandlerFactoryPtr
createSvsKeeperPrometheusHandlerFactory(IServer & server, const std::string & config_prefix);

}
