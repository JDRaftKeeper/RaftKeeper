#include <Service/SvsKeeperPrometheusRequestHandler.h>
#include <Service/SvsKeeperMetricsWriter.h>

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <Server/HTTPHandlerRequestFilter.h>
#include <Service/SvsKeeperMetrics.h>

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/IServer.h>


namespace DB
{
void SvsKeeperPrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);
        try
        {
            metrics_writer.write(wb);
            wb.finalize();
        }
        catch (...)
        {
            wb.finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException("SvsKeeperPrometheusRequestHandler");
    }
}

HTTPRequestHandlerFactoryPtr
createSvsKeeperPrometheusHandlerFactory(IServer & server, const std::string & config_prefix)
{
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<SvsKeeperPrometheusRequestHandler>>(
        server, SvsKeeperMetricsWriter(server.config(), config_prefix + ".endpoint"));
    return factory;
}

}
