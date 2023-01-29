#pragma once

#include "IServer.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/TCPServer.h"
#include "Poco/Timestamp.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/ServerApplication.h"
#include <daemon/BaseDaemon.h>

using Poco::Util::Application;
using Poco::Util::ServerApplication;

namespace DB
{
class Service : public BaseDaemon, public IServer
{
public:
    using ServerApplication::run;

    Poco::Util::LayeredConfiguration & config() const override { return BaseDaemon::config(); }

    Poco::Logger & logger() const override { return BaseDaemon::logger(); }

    Context & context() const override { return *global_context_ptr; }

    bool isCancelled() const override { return BaseDaemon::isCancelled(); }

    //void init(int argc, char ** argv) { }
    int run() override;

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    void initialize(Application & self) override;
    void uninitialize() override;
    int main(const std::vector<std::string> & args) override;

private:
    Context * global_context_ptr = nullptr;


    Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * log) const;
    Poco::Net::SocketAddress
    socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure = false) const;

    using CreateServerFunc = std::function<void(UInt16)>;
    void createServer(const std::string & listen_host, const char * port_name, bool listen_try, CreateServerFunc && func) const;
};

}
