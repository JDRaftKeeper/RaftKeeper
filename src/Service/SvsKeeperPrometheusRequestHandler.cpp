#include "SvsKeeperPrometheusRequestHandler.h"
#include "SvsKeeperMetricsWriter.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Service/SvsKeeperMetrics.h>


namespace DB
{
void SvsKeeperPrometheusRequestHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        auto wb = WriteBufferFromHTTPServerResponse(request, response, keep_alive_timeout);
        metrics_writer.write(wb);
        wb.finalize();
    }
    catch (...)
    {
        tryLogCurrentException("SvsKeeperPrometheusRequestHandler");
    }
}

Poco::Net::HTTPRequestHandlerFactory *
createSvsKeeperPrometheusHandlerFactory(IServer & server, const std::string & name, const std::string & config_prefix)
{
    auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);
    auto handler = std::make_unique<HandlingRuleHTTPHandlerFactory<SvsKeeperPrometheusRequestHandler>>(
        server, SvsKeeperMetricsWriter(server.config(), config_prefix));
    std:: string endpoint = server.config().getString(config_prefix + ".endpoint", "/metrics");
    handler->attachStrictPath(endpoint)->allowGetAndHeadRequest();
    factory->addHandler(handler.release());
    return factory.release();
}

}
