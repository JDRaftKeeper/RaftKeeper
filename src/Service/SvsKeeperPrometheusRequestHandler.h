#ifndef CLICKHOUSE_SVSKEEPERPROMETHEUSREQUESTHANDLER_H
#define CLICKHOUSE_SVSKEEPERPROMETHEUSREQUESTHANDLER_H

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

#include "Server/IServer.h"
#include "SvsKeeperMetricsWriter.h"

namespace DB
{

class SvsKeeperPrometheusRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;
    const SvsKeeperMetricsWriter & metrics_writer;

public:
    explicit SvsKeeperPrometheusRequestHandler(IServer & server_, const SvsKeeperMetricsWriter & metrics_writer_)
    : server(server_)
    , metrics_writer(metrics_writer_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}


#endif //CLICKHOUSE_SVSKEEPERPROMETHEUSREQUESTHANDLER_H
