#include "Service.h"
#include <memory>
#include <errno.h>
#include <pwd.h>
#include <unistd.h>
#include <Access/AccessControlManager.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/HTTPCommon.h>
#include <IO/UseSSL.h>
#include <Interpreters/ProcessList.h>
#include <Server/HTTP/HTTPServer.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/ProtocolServerAdapter.h>
#include <Service/FourLetterCommand.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "Poco/Timestamp.h"
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>SvsKeeperServer.cpp
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Version.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/Macros.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/StatusFile.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TLDListsHolder.h>
#include <Common/ThreadFuzzer.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/config_version.h>
#include <Common/getExecutablePath.h>
#include <Common/getMappedArea.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/remapExecutable.h>
#include <common/ErrorHandlers.h>
#include <common/coverage.h>
#include <common/errnoToString.h>
#include <common/getFQDNOrHostName.h>
#include <common/getMemoryAmount.h>
#include <common/logger_useful.h>
#include <common/phdr_cache.h>
#include <ext/scope_guard.h>
#include <Service/ServiceTCPHandlerFactory.h>
#include <Service/FourLetterCommand.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SYSTEM_ERROR;
    extern const int FAILED_TO_GETPWUID;
    extern const int MISMATCHING_USERS_FOR_PROCESS_AND_DATA;
    extern const int NETWORK_ERROR;
}


void Service::initialize(Application & self)
{
    //ServerApplication::loadConfiguration();
    //ServerApplication::initialize(self);
    logger().information("starting up service server");
    BaseDaemon::initialize(self);
}

void Service::uninitialize()
{
    logger().information("shutting down service server");
    BaseDaemon::uninitialize();
    //ServerApplication::uninitialize();
}

void Service::createServer(const std::string & listen_host, const char * port_name, bool listen_try, CreateServerFunc && func) const
{
    /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
    if (!config().has(port_name))
        return;

    auto port = config().getInt(port_name);
    try
    {
        func(port);
    }
    catch (const Poco::Exception &)
    {
        std::string message = "Listen [" + listen_host + "]:" + std::to_string(port) + " failed: " + getCurrentExceptionMessage(false);

        if (listen_try)
        {
            LOG_WARNING(
                &logger(),
                "{}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                message);
        }
        else
        {
            throw Exception{message, ErrorCodes::NETWORK_ERROR};
        }
    }
}

int Service::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(Service::options());
        auto header_str = fmt::format(
            "{} [OPTION] [-- [ARG]...]\n"
            "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
            commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }


    if (config().hasOption("version"))
    {
        std::cout << DBMS_NAME << " server version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
        return 0;
    }

    return Application::run(); // NOLINT
}


int waitServersToFinish(std::vector<DB::ProtocolServerAdapter> & servers, size_t seconds_to_wait)
{
    const int sleep_max_ms = 1000 * seconds_to_wait;
    const int sleep_one_ms = 100;
    int sleep_current_ms = 0;
    int current_connections = 0;
    while (sleep_current_ms < sleep_max_ms)
    {
        current_connections = 0;
        for (auto & server : servers)
        {
            server.stop();
            current_connections += server.currentConnections();
        }
        if (!current_connections)
            break;
        sleep_current_ms += sleep_one_ms;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_one_ms));
    }
    return current_connections;
}


int Service::main(const std::vector<std::string> & /*args*/)
{
    Poco::Logger * log = &logger();

    auto shared_context = Context::createShared();
    auto global_context = std::make_unique<Context>(Context::createGlobal(shared_context.get()));
    const Settings & settings = global_context->getSettingsRef();
    global_context_ptr = global_context.get();

    // 1. Bind a ServerSocket with an address
    //Poco::Net::ServerSocket serverSocket(port);
    //1 ProtocolServerAdapter
    //ProtocolServerAdapterPtr server = std::make_shared<ProtocolServerAdapter>();
    auto servers = std::make_shared<std::vector<ProtocolServerAdapter>>();

    Poco::Timespan keep_alive_timeout(config().getUInt("keep_alive_timeout", 10), 0);

    Poco::ThreadPool server_pool(10, config().getUInt("max_connections", 1024));
    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings.http_receive_timeout);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    //get port from config
    std::string listen_host = config().getString("service.host", "0.0.0.0");
    //unsigned short listen_port = config().getInt("frontend_port", ServerPort);
    bool listen_try = config().getBool("listen_try", false);

    //Init global thread pool
    GlobalThreadPool::initialize(config().getUInt("max_thread_pool_size", 10000));

    const char * port_name = "service.service_port";
    global_context->initializeServiceKeeperStorageDispatcher();
    createServer(listen_host, port_name, listen_try, [&](UInt16 port) {
        Poco::Net::ServerSocket socket;
        auto address = socketBindListen(socket, listen_host, port);
        socket.setReceiveTimeout(settings.receive_timeout);
        socket.setSendTimeout(settings.send_timeout);
        servers->emplace_back(
            port_name,
            std::make_unique<Poco::Net::TCPServer>(
                new ServiceTCPHandlerFactory(*this, false, true), server_pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(log, "Listening for connections to fake zookeeper (tcp): {}", address.toString());
    });
    //    }

    FourLetterCommandFactory::registerCommands(*global_context->getSvsKeeperStorageDispatcher());

    /// 3. Start the TCPServer
    for (auto & server : *servers)
        server.start();
    {
        String level_str = config().getString("text_log.level", "");
        int level = level_str.empty() ? INT_MAX : Poco::Logger::parseLevel(level_str);
        setTextLog(global_context->getTextLog(), level);
    }

    buildLoggers(config(), logger());
    LOG_INFO(log, "Ready for connections.");

    SCOPE_EXIT({
        LOG_DEBUG(log, "Received termination signal.");
        LOG_DEBUG(log, "Waiting for current connections to close.");

        is_cancelled = true;

        int current_connections = 0;
        for (auto & server : *servers)
        {
            server.stop();
            current_connections += server.currentConnections();
        }

        if (current_connections)
            LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
        else
            LOG_INFO(log, "Closed all listening sockets.");

        /// shutdown storage dispatcher
        global_context->shutdownServiceKeeperStorageDispatcher();

        if (current_connections)
            current_connections = waitServersToFinish(*servers, config().getInt("shutdown_wait_unfinished", 5));

        if (current_connections)
            LOG_INFO(
                log,
                "Closed connections. But {} remain."
                " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>",
                current_connections);
        else
            LOG_INFO(log, "Closed connections.");

        if (current_connections)
        {
            /// There is no better way to force connections to close in Poco.
            /// Otherwise connection handlers will continue to live
            /// (they are effectively dangling objects, but they use global thread pool
            ///  and global thread pool destructor will wait for threads, preventing server shutdown).

            /// Dump coverage here, because std::atexit callback would not be called.
            dumpCoverageReportIfPossible();
            LOG_INFO(log, "Will shutdown forcefully.");
            _exit(Application::EXIT_OK);
        }
    });


    // 4. Wait for termination
    waitForTerminationRequest();

    return Application::EXIT_OK;
}

void Service::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(Poco::Util::Option("help", "h", "show help and exit").required(false).repeatable(false).binding("help"));
    options.addOption(Poco::Util::Option("version", "V", "show version and exit").required(false).repeatable(false).binding("version"));
    BaseDaemon::defineOptions(options);
}

Poco::Net::SocketAddress Service::makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * log) const
{
    Poco::Net::SocketAddress socket_address;
    try
    {
        socket_address = Poco::Net::SocketAddress(host, port);
    }
    catch (const Poco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
            || code == EAI_ADDRFAMILY
#endif
        )
        {
            LOG_ERROR(
                log,
                "Cannot resolve listen_host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                "specify IPv4 address to listen in <listen_host> element of configuration "
                "file. Example: <listen_host>0.0.0.0</listen_host>",
                host,
                e.code(),
                e.message());
        }

        throw;
    }
    return socket_address;
}

Poco::Net::SocketAddress
Service::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
{
    auto address = makeSocketAddress(host, port, &logger());
#if !defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION < 0x01090100
    if (secure)
        /// Bug in old (<1.9.1) poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
        /// https://github.com/pocoproject/poco/pull/2257
        socket.bind(address, /* reuseAddress = */ true);
    else
#endif
#if POCO_VERSION < 0x01080000
        socket.bind(address, /* reuseAddress = */ true);
#else
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config().getBool("listen_reuse_port", false));
#endif

    socket.listen(/* backlog = */ config().getUInt("listen_backlog", 64));

    return address;
}


}


int mainEntryClickHouseService(int argc, char ** argv)
{
    try
    {
        DB::Service service;
        //master.init(argc, argv);
        return service.run(argc, argv);
        //return master.run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
