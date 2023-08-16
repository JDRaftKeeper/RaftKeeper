#include <Service/FourLetterCommand.h>

#include <Common/IO/Operators.h>
#include <Common/IO/WriteHelpers.h>
#include <Service/ConnectionHandler.h>
#include <Service/Keeper4LWInfo.h>
#include <Service/KeeperDispatcher.h>
#include <Poco/Environment.h>
#include <Poco/Path.h>
#include <Poco/String.h>
#include "Common/StringUtils.h"
#include <Common/config_version.h>
#include <Common/getCurrentProcessFDCount.h>
#include <Common/getMaxFileDescriptorCount.h>
#include <common/find_symbols.h>
#include <common/logger_useful.h>

#include <unistd.h>

namespace RK
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IFourLetterCommand::IFourLetterCommand(KeeperDispatcher & keeper_dispatcher_)
    : keeper_dispatcher(keeper_dispatcher_)
{
}

int32_t IFourLetterCommand::code()
{
    return toCode(name());
}

String IFourLetterCommand::toName(int32_t code)
{
    int reverted_code = __builtin_bswap32(code);
    return String(reinterpret_cast<char *>(&reverted_code), 4);
}

int32_t IFourLetterCommand::toCode(const String & name)
{
    int32_t res = *reinterpret_cast<const int32_t *>(name.data());
    /// keep consistent with Coordination::read method by changing big endian to little endian.
    return __builtin_bswap32(res);
}

IFourLetterCommand::~IFourLetterCommand() = default;

FourLetterCommandFactory & FourLetterCommandFactory::instance()
{
    static FourLetterCommandFactory factory;
    return factory;
}

void FourLetterCommandFactory::checkInitialization() const
{
    if (!initialized)
        throw Exception("Four letter command  not initialized", ErrorCodes::LOGICAL_ERROR);
}

bool FourLetterCommandFactory::isKnown(int32_t code)
{
    checkInitialization();
    return commands.contains(code);
}

FourLetterCommandPtr FourLetterCommandFactory::get(int32_t code)
{
    checkInitialization();
    return commands.at(code);
}

void FourLetterCommandFactory::registerCommand(FourLetterCommandPtr & command)
{
    if (commands.contains(command->code()))
        throw Exception("Four letter command " + command->name() + " already registered", ErrorCodes::LOGICAL_ERROR);

    auto * log = &Poco::Logger::get("FourLetterCommandFactory");
    LOG_INFO(log, "Register four letter command {}", command->name());
    commands.emplace(command->code(), std::move(command));
}

void FourLetterCommandFactory::registerCommands(KeeperDispatcher & keeper_dispatcher)
{
    FourLetterCommandFactory & factory = FourLetterCommandFactory::instance();

    if (!factory.isInitialized())
    {
        FourLetterCommandPtr ruok_command = std::make_shared<RuokCommand>(keeper_dispatcher);
        factory.registerCommand(ruok_command);

        FourLetterCommandPtr mntr_command = std::make_shared<MonitorCommand>(keeper_dispatcher);
        factory.registerCommand(mntr_command);

        FourLetterCommandPtr conf_command = std::make_shared<ConfCommand>(keeper_dispatcher);
        factory.registerCommand(conf_command);

        FourLetterCommandPtr cons_command = std::make_shared<ConsCommand>(keeper_dispatcher);
        factory.registerCommand(cons_command);

        FourLetterCommandPtr brief_watch_command = std::make_shared<BriefWatchCommand>(keeper_dispatcher);
        factory.registerCommand(brief_watch_command);

        FourLetterCommandPtr data_size_command = std::make_shared<DataSizeCommand>(keeper_dispatcher);
        factory.registerCommand(data_size_command);

        FourLetterCommandPtr dump_command = std::make_shared<DumpCommand>(keeper_dispatcher);
        factory.registerCommand(dump_command);

        FourLetterCommandPtr envi_command = std::make_shared<EnviCommand>(keeper_dispatcher);
        factory.registerCommand(envi_command);

        FourLetterCommandPtr is_rad_only_command = std::make_shared<IsReadOnlyCommand>(keeper_dispatcher);
        factory.registerCommand(is_rad_only_command);

        FourLetterCommandPtr rest_conn_stats_command = std::make_shared<RestConnStatsCommand>(keeper_dispatcher);
        factory.registerCommand(rest_conn_stats_command);

        FourLetterCommandPtr server_stat_command = std::make_shared<ServerStatCommand>(keeper_dispatcher);
        factory.registerCommand(server_stat_command);

        FourLetterCommandPtr stat_command = std::make_shared<StatCommand>(keeper_dispatcher);
        factory.registerCommand(stat_command);

        FourLetterCommandPtr stat_reset_command = std::make_shared<StatResetCommand>(keeper_dispatcher);
        factory.registerCommand(stat_reset_command);

        FourLetterCommandPtr watch_by_path_command = std::make_shared<WatchByPathCommand>(keeper_dispatcher);
        factory.registerCommand(watch_by_path_command);

        FourLetterCommandPtr watch_command = std::make_shared<WatchCommand>(keeper_dispatcher);
        factory.registerCommand(watch_command);

        FourLetterCommandPtr create_snapshot_command = std::make_shared<CreateSnapshotCommand>(keeper_dispatcher);
        factory.registerCommand(create_snapshot_command);

        FourLetterCommandPtr log_info_command = std::make_shared<LogInfoCommand>(keeper_dispatcher);
        factory.registerCommand(log_info_command);

        FourLetterCommandPtr request_leader_command = std::make_shared<RequestLeaderCommand>(keeper_dispatcher);
        factory.registerCommand(request_leader_command);

        FourLetterCommandPtr uptime_command = std::make_shared<UpTimeCommand>(keeper_dispatcher);
        factory.registerCommand(uptime_command);

        factory.initializeWhiteList(keeper_dispatcher);
        factory.setInitialize(true);
    }
}

bool FourLetterCommandFactory::isEnabled(int32_t code)
{
    checkInitialization();
    if (!white_list.empty() && *white_list.cbegin() == WHITE_LIST_ALL)
        return true;

    return std::find(white_list.begin(), white_list.end(), code) != white_list.end();
}

void FourLetterCommandFactory::initializeWhiteList(KeeperDispatcher & keeper_dispatcher)
{
    const auto & keeper_settings = keeper_dispatcher.getKeeperConfigurationAndSettings();

    String list_str = keeper_settings->four_letter_word_white_list;
    Strings tokens;
    splitInto<','>(tokens, list_str);

    for (String token: tokens)
    {
        Poco::trim(token);

        if (token == "*")
        {
            white_list.clear();
            white_list.push_back(WHITE_LIST_ALL);
            return;
        }
        else
        {
            if (commands.contains(IFourLetterCommand::toCode(token)))
            {
                white_list.push_back(IFourLetterCommand::toCode(token));
            }
            else
            {
                auto * log = &Poco::Logger::get("FourLetterCommandFactory");
                LOG_WARNING(log, "Find invalid keeper 4lw command {} when initializing, ignore it.", token);
            }
        }
    }
}

String RuokCommand::run()
{
    return "imok";
}

namespace
{

void print(IFourLetterCommand::StringBuffer & buf, const String & key, const String & value)
{
    writeText("zk_", buf);
    writeText(key, buf);
    writeText("\t", buf);
    writeText(value, buf);
    writeText("\n", buf);
}

void print(IFourLetterCommand::StringBuffer & buf, const String & key, uint64_t value)
{
    print(buf, key, toString(value));
}

}

String MonitorCommand::run()
{
ConnectionStats stats = keeper_dispatcher.getKeeperConnectionStats();
    Keeper4LWInfo keeper_info = keeper_dispatcher.getKeeper4LWInfo();

    if (!keeper_info.has_leader)
        return "This instance is not currently serving requests";

    const auto & state_machine = keeper_dispatcher.getStateMachine();

    StringBuffer ret;

    WriteBufferFromOwnString version;
    writeText(VERSION_FULL, version);
    writeText("-", version);
    writeText(GIT_COMMIT_HASH, version);
    writeText(", built on ", version);
    writeText(BUILD_TIME, version);

    print(ret, "version", version.str());

    print(ret, "avg_latency", stats.getAvgLatency());
    print(ret, "max_latency", stats.getMaxLatency());
    print(ret, "min_latency", stats.getMinLatency());
    print(ret, "packets_received", stats.getPacketsReceived());
    print(ret, "packets_sent", stats.getPacketsSent());

    print(ret, "num_alive_connections", keeper_info.alive_connections_count);
    print(ret, "outstanding_requests", keeper_info.outstanding_requests_count);

    print(ret, "server_state", keeper_info.getRole());

    print(ret, "znode_count", state_machine.getNodesCount());
    print(ret, "watch_count", state_machine.getTotalWatchesCount());
    print(ret, "ephemerals_count", state_machine.getTotalEphemeralNodesCount());
    print(ret, "approximate_data_size", state_machine.getApproximateDataSize());
    print(ret, "snap_count", state_machine.getSnapshotCount());
    print(ret, "snap_time_ms", state_machine.getSnapshotTimeMs());
    print(ret, "in_snapshot", state_machine.getSnapshoting());

#if defined(__linux__) || defined(__APPLE__)
    print(ret, "open_file_descriptor_count", getCurrentProcessFDCount());
    print(ret, "max_file_descriptor_count", getMaxFileDescriptorCount());
#endif

    if (keeper_info.is_leader)
    {
        print(ret, "followers", keeper_info.follower_count);
        print(ret, "synced_followers", keeper_info.synced_follower_count);
    }

    return ret.str();
}

String StatResetCommand::run()
{
    keeper_dispatcher.resetConnectionStats();
    return "Server stats reset.\n";
}

String NopCommand::run()
{
    return "";
}

String ConfCommand::run()
{
    StringBuffer buf;
    keeper_dispatcher.getKeeperConfigurationAndSettings()->dump(buf);
    return buf.str();
}

String ConsCommand::run()
{
    StringBuffer buf;
    ConnectionHandler::dumpConnections(buf, false);
    return buf.str();
}

String RestConnStatsCommand::run()
{
    ConnectionHandler::resetConnsStats();
    return "Connection stats reset.\n";
}

String ServerStatCommand::run()
{
    StringBuffer buf;

    auto write = [&buf](const String & key, const String & value)
    {
        writeText(key, buf);
        writeText(": ", buf);
        writeText(value, buf);
        writeText("\n", buf);
    };

    ConnectionStats stats = keeper_dispatcher.getKeeperConnectionStats();
    Keeper4LWInfo keeper_info = keeper_dispatcher.getKeeper4LWInfo();

    write("RaftKeeper version", VERSION_FULL);

    StringBuffer latency;
    latency << stats.getMinLatency() << "/" << stats.getAvgLatency() << "/" << stats.getMaxLatency();
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats.getPacketsReceived()));
    write("Sent ", toString(stats.getPacketsSent()));
    write("Connections", toString(keeper_info.alive_connections_count));
    write("Outstanding", toString(keeper_info.outstanding_requests_count));
    write("Zxid", toString(keeper_info.last_zxid));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(keeper_info.total_nodes_count));

    return buf.str();
}

String StatCommand::run()
{
    StringBuffer buf;

    auto write = [&buf] (const String & key, const String & value) { buf << key << ": " << value << '\n'; };

    ConnectionStats stats = keeper_dispatcher.getKeeperConnectionStats();
    Keeper4LWInfo keeper_info = keeper_dispatcher.getKeeper4LWInfo();

    write("RaftKeeper version", VERSION_FULL);

    buf << "Clients:\n";
    ConnectionHandler::dumpConnections(buf, true);
    buf << '\n';

    StringBuffer latency;
    latency << stats.getMinLatency() << "/" << stats.getAvgLatency() << "/" << stats.getMaxLatency();
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats.getPacketsReceived()));
    write("Sent ", toString(stats.getPacketsSent()));
    write("Connections", toString(keeper_info.alive_connections_count));
    write("Outstanding", toString(keeper_info.outstanding_requests_count));
    write("Zxid", toString(keeper_info.last_zxid));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(keeper_info.total_nodes_count));

    return buf.str();
}

String BriefWatchCommand::run()
{
    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    buf << state_machine.getSessionsWithWatchesCount() << " connections watching "
        << state_machine.getWatchedPathsCount() << " paths\n";
    buf << "Total watches:" << state_machine.getTotalWatchesCount() << "\n";
    return buf.str();
}

String WatchCommand::run()
{
    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    state_machine.dumpWatches(buf);
    return buf.str();
}

String WatchByPathCommand::run()
{
    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    state_machine.dumpWatchesByPath(buf);
    return buf.str();
}

String DataSizeCommand::run()
{
    StringBuffer buf;
    buf << "snapshot_dir_size: " << keeper_dispatcher.getSnapDirSize() << '\n';
    buf << "log_dir_size: " << keeper_dispatcher.getLogDirSize() << '\n';
    return buf.str();
}

String DumpCommand::run()
{
    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    state_machine.dumpSessionsAndEphemerals(buf);
    return buf.str();
}

String EnviCommand::run()
{
    using Poco::Environment;
    using Poco::Path;

    StringBuffer buf;
    buf << "Environment:\n";
    buf << "raftkeeper.version=" << VERSION_FULL << '\n';

    buf << "host.name=" << Environment::nodeName() << '\n';
    buf << "os.name=" << Environment::osDisplayName() << '\n';
    buf << "os.arch=" << Environment::osArchitecture() << '\n';
    buf << "os.version=" << Environment::osVersion() << '\n';
    buf << "cpu.count=" << Environment::processorCount() << '\n';

    String os_user;
    os_user.resize(256, '\0');
    if (0 == getlogin_r(os_user.data(), os_user.size() - 1))
        os_user.resize(strlen(os_user.c_str()));
    else
        os_user.clear();    /// Don't mind if we cannot determine user login.

    buf << "user.name=" << os_user << '\n';

    buf << "user.home=" << Path::home() << '\n';
    buf << "user.dir=" << Path::current() << '\n';
    buf << "user.tmp=" << Path::temp() << '\n';

    return buf.str();
}

String IsReadOnlyCommand::run()
{
    if (keeper_dispatcher.isObserver())
        return "ro";
    else
        return "rw";
}

String CreateSnapshotCommand::run()
{
    auto log_index = keeper_dispatcher.createSnapshot();
    return log_index > 0 ? std::to_string(log_index) : "Failed to schedule snapshot creation task.";
}

String LogInfoCommand::run()
{
    KeeperLogInfo log_info = keeper_dispatcher.getKeeperLogInfo();
    StringBuffer ret;

    auto append = [&ret] (String key, uint64_t value) -> void
    {
        writeText(key, ret);
        writeText("\t", ret);
        writeText(std::to_string(value), ret);
        writeText("\n", ret);
    };
    append("first_log_idx", log_info.first_log_idx);
    append("first_log_term", log_info.first_log_term);
    append("last_log_idx", log_info.last_log_idx);
    append("last_log_term", log_info.last_log_term);
    append("last_committed_log_idx", log_info.last_committed_log_idx);
    append("leader_committed_log_idx", log_info.leader_committed_log_idx);
    append("target_committed_log_idx", log_info.target_committed_log_idx);
    append("last_snapshot_idx", log_info.last_snapshot_idx);
    return ret.str();
}

String RequestLeaderCommand::run()
{
    return keeper_dispatcher.requestLeader() ? "Sent leadership request to leader." : "Failed to send leadership request to leader.";
}

String UpTimeCommand::run()
{
    return std::to_string(keeper_dispatcher.uptimeFromStartup() / 1000 / 1000);
}

}
