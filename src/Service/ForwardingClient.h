#pragma once

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Poco/Net/StreamSocket.h>
#include <common/logger_useful.h>
#include <Service/SvsKeeperStorage.h>

namespace DB
{
class ForwardingClient
{
public:
    ForwardingClient(String endpoint_, Poco::Timespan operation_timeout_ms) : endpoint(endpoint_), operation_timeout(operation_timeout_ms), log(&Poco::Logger::get("ForwardingClient")) {}

    void connect(Poco::Net::SocketAddress & address, Poco::Timespan connection_timeout);
    void send(SvsKeeperStorage::RequestForSession request_for_session);
    void disconnect();

    ~ForwardingClient()
    {
        try
        {
            disconnect();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    bool connected{false};
    String endpoint;
    Poco::Timespan operation_timeout;
    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;

};
}
