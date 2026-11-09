#include <chrono>
#include <future>
#include <stdexcept>

#include <Poco/Net/ServerSocket.h>

#include <gtest/gtest.h>

#include <Service/ForwardConnection.h>

using namespace RK;
using namespace std::chrono_literals;

namespace
{

class PausedSyncSessionsRequest : public ForwardSyncSessionsRequest
{
public:
    PausedSyncSessionsRequest(std::promise<void> & entered_, std::shared_future<void> resume_)
        : entered(entered_), resume(std::move(resume_))
    {
    }

    void writeImpl(WriteBuffer & buffer) const override
    {
        entered.set_value();
        if (resume.wait_for(5s) != std::future_status::ready)
            throw std::runtime_error("Timed out waiting to resume the send");
        ForwardSyncSessionsRequest::writeImpl(buffer);
    }

private:
    std::promise<void> & entered;
    std::shared_future<void> resume;
};

class ForwardConnectionTest : public ::testing::Test
{
protected:
    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0)};
    std::unique_ptr<ForwardConnection> connection;
    Poco::Net::StreamSocket peer;

    std::future<Poco::Net::StreamSocket> acceptHandshake()
    {
        return std::async(
            std::launch::async,
            [this]
            {
                if (!listener.poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ))
                    throw std::runtime_error("Timed out accepting the forwarding connection");
                auto socket = listener.acceptConnection();
                socket.setReceiveTimeout(Poco::Timespan(5, 0));
                socket.setSendTimeout(Poco::Timespan(5, 0));
                ReadBufferFromPocoSocket in(socket);
                int8_t type;
                Coordination::read(type, in);
                if (type != static_cast<int8_t>(ForwardType::Handshake))
                    throw std::runtime_error("Expected forwarding handshake");
                ForwardHandshakeRequest request;
                request.readImpl(in);
                WriteBufferFromPocoSocket out(socket);
                ForwardHandshakeResponse response;
                response.write(out);
                out.next();
                return socket;
            });
    }

    void SetUp() override
    {
        auto accepted = acceptHandshake();
        connection = std::make_unique<ForwardConnection>(1, 0, listener.address().toString(), Poco::Timespan(2, 0));
        ASSERT_NO_THROW(connection->connect());
        ASSERT_NO_THROW(peer = accepted.get());
    }

    void sendInvalidResponse()
    {
        WriteBufferFromPocoSocket out(peer);
        Coordination::write(static_cast<int8_t>(ForwardType::Unknown), out);
        out.next();
    }

    void readEmptySessionsRequest()
    {
        ReadBufferFromPocoSocket in(peer);
        int8_t type;
        int32_t length;
        Coordination::read(type, in);
        Coordination::read(length, in);
        ASSERT_EQ(type, static_cast<int8_t>(ForwardType::SyncSessions));
        ASSERT_EQ(length, 4);
        ForwardSyncSessionsRequest request;
        request.readImpl(in);
        EXPECT_TRUE(request.session_expiration_time.empty());
    }
};

}

TEST_F(ForwardConnectionTest, ReceiveFailureDoesNotCloseSocketDuringSend)
{
    std::promise<void> entered;
    auto sending = entered.get_future();
    std::promise<void> resume;
    auto request = std::make_shared<PausedSyncSessionsRequest>(entered, resume.get_future().share());
    auto sent = std::async(std::launch::async, [&] { connection->send(request); });
    ASSERT_EQ(sending.wait_for(5s), std::future_status::ready);

    sendInvalidResponse();
    ForwardResponsePtr response;
    EXPECT_ANY_THROW(connection->receive(response));
    EXPECT_FALSE(connection->isConnected());
    /// A receive error must not close the descriptor or reset buffers still used by the sender.
    resume.set_value();
    ASSERT_NO_THROW(sent.get());
    readEmptySessionsRequest();
}

TEST_F(ForwardConnectionTest, DisconnectWaitsForSend)
{
    std::promise<void> entered;
    auto sending = entered.get_future();
    std::promise<void> resume;
    auto request = std::make_shared<PausedSyncSessionsRequest>(entered, resume.get_future().share());
    auto sent = std::async(std::launch::async, [&] { connection->send(request); });
    ASSERT_EQ(sending.wait_for(5s), std::future_status::ready);

    auto disconnected = std::async(std::launch::async, [&] { connection->disconnect(); });
    EXPECT_EQ(disconnected.wait_for(50ms), std::future_status::timeout);
    resume.set_value();
    EXPECT_NO_THROW(sent.get());
    EXPECT_NO_THROW(disconnected.get());
    EXPECT_FALSE(connection->isConnected());
}

TEST_F(ForwardConnectionTest, ReconnectAfterReceiveFailure)
{
    sendInvalidResponse();
    ForwardResponsePtr response;
    EXPECT_ANY_THROW(connection->receive(response));
    ASSERT_FALSE(connection->isConnected());

    auto accepted = acceptHandshake();
    ASSERT_NO_THROW(connection->send(std::make_shared<ForwardSyncSessionsRequest>()));
    ASSERT_NO_THROW(peer = accepted.get());
    EXPECT_TRUE(connection->isConnected());
    readEmptySessionsRequest();
}

TEST_F(ForwardConnectionTest, PollAllowsConcurrentSend)
{
    auto readable = std::async(std::launch::async, [&] { return connection->poll(2000000); });
    ASSERT_NO_THROW(connection->send(std::make_shared<ForwardSyncSessionsRequest>()));
    readEmptySessionsRequest();
    WriteBufferFromPocoSocket out(peer);
    ForwardSyncSessionsResponse reply;
    reply.write(out);
    out.next();
    EXPECT_TRUE(readable.get());
    ForwardResponsePtr response;
    ASSERT_NO_THROW(connection->receive(response));
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->forwardType(), ForwardType::SyncSessions);
    EXPECT_TRUE(response->accepted);
}
