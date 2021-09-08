#include <Service/FourLetterCommand.h>
#include <Service/SvsKeeperDispatcher.h>

namespace DB
{
FourLetterCommand::FourLetterCommand(const Context & global_context_) : global_context(global_context_)
{
}

Int32 FourLetterCommand::code()
{
    Int32 res = *reinterpret_cast<Int32 *>(name().data());
    /// keep consistent with Coordination::read method by changing big endian to little endian.
    return __builtin_bswap32(res);
}

void FourLetterCommand::printSet(FourLetterCommand::StringBuffer & ss, std::unordered_set<String> & target, String && prefix)
{
    for (const auto & str : target)
    {
        ss << prefix << str << std::endl;
    }
}
FourLetterCommand::~FourLetterCommand() = default;

String MntrCommand::name()
{
    return "mntr";
}
void MntrCommand::run(String & res)
{
    /// TODO implementation
    res.data();
}

MntrCommand::~MntrCommand() = default;

String WchsCommand::name()
{
    return "wchs";
}

/**
 * Example:
 * 18 connections watching 54 paths
 * Total watches:54
 */
void WchsCommand::run(String & res)
{
    SessionAndWatcherPtr watch_info = global_context.getSvsKeeperStorageDispatcher()->getWatchInfo();

    int connection_num = watch_info->size();
    int watch_num{};
    std::unordered_set<String> watch_paths;

    for (auto & watch_elem : *watch_info)
    {
        watch_num += watch_elem.second.size();
        watch_paths.insert(watch_elem.second.begin(), watch_elem.second.end());
    }

    StringBuffer ss;
    ss << connection_num << " connections watching " << watch_paths.size() << " paths" << std::endl;
    ss << "Total watches:" << watch_num << std::endl;

    res = ss.str();
}

WchsCommand::~WchsCommand() = default;

String WchcCommand::name()
{
    return "wchc";
}

void WchcCommand::run(String & res)
{
    SessionAndWatcherPtr watch_info = global_context.getSvsKeeperStorageDispatcher()->getWatchInfo();

    int connection_num = watch_info->size();
    int watch_num{};
    std::unordered_set<String> watch_paths;

    StringBuffer watches_out;
    watches_out << std::hex;

    for (auto & watch_elem : *watch_info)
    {
        watch_num += watch_elem.second.size();
        watch_paths.insert(watch_elem.second.begin(), watch_elem.second.end());

        watches_out << "\tSession id 0x" << watch_elem.first << ": " << std::endl;
        printSet(watches_out, watch_elem.second, "\t\t");
    }

    StringBuffer ss;
    ss << connection_num << " connections watching " << watch_paths.size() << " paths" << std::endl;
    ss << "Total watches:" << watch_num << std::endl;

    ss << std::endl << "Details: " << std::endl;
    ss << watches_out.str() << std::endl;
    res = ss.str();
}

WchcCommand::~WchcCommand() = default;

String DumpCommand::name()
{
    return "dump";
}
void DumpCommand::run(String & res)
{
    /// TODO limit to 1000
    EphemeralsPtr ephemerals = global_context.getSvsKeeperStorageDispatcher()->getEphemeralInfo();
    int connection_num = ephemerals->size();
    int watch_num{};
    std::unordered_set<String> ephemeral_paths;

    StringBuffer ephemerals_out;
    ephemerals_out << std::hex;

    for (auto & ephemeral_elem : *ephemerals)
    {
        watch_num += ephemeral_elem.second.size();
        ephemeral_paths.insert(ephemeral_elem.second.begin(), ephemeral_elem.second.end());

        ephemerals_out << "\tSession id 0x" << ephemeral_elem.first << ": " << std::endl;
        printSet(ephemerals_out, ephemeral_elem.second, "\t\t");
    }

    StringBuffer ss;
    ss << connection_num << " connections creating " << ephemeral_paths.size() << " ephemeral paths" << std::endl;
    ss << "Total ephemerals:" << watch_num << std::endl;

    ss << std::endl << "Details: " << std::endl;
    ss << ephemerals_out.str() << std::endl;
    res = ss.str();
}
DumpCommand::~DumpCommand() = default;

String RuokCommand::name()
{
    return "ruok";
}
void RuokCommand::run(String & res)
{
    res = "imok";
}
RuokCommand::~RuokCommand() = default;

volatile bool FourLetterCommands::initialized = false;
std::unordered_map<Int32, FourLetterCommandPtr> FourLetterCommands::commands = {};

bool FourLetterCommands::isKnown(Int32 code)
{
    if (!initialized)
    {
        throw Exception("Four letter command " + std::to_string(code) + " not initialized", ErrorCodes::LOGICAL_ERROR);
    }
    return commands.contains(code);
}

FourLetterCommandPtr FourLetterCommands::getCommand(Int32 code)
{
    if (!initialized)
    {
        throw Exception("Four letter command " + std::to_string(code) + " not initialized", ErrorCodes::LOGICAL_ERROR);
    }
    return commands.at(code);
}

void FourLetterCommands::registerCommand(FourLetterCommandPtr & command)
{
    if (commands.contains(command->code()))
    {
        throw Exception("Four letter command " + std::to_string(command->code()) + " already registered", ErrorCodes::LOGICAL_ERROR);
    }
    auto * log = &Poco::Logger::get("FourLetterCommands");
    LOG_INFO(log, "Register four letter command " + command->name() + " - " + std::to_string(command->code()));
    commands.emplace(command->code(), std::move(command));
}

void FourLetterCommands::registerCommands(const Context & global_context)
{
    if (!initialized)
    {
        FourLetterCommandPtr mntr_command = std::make_shared<MntrCommand>(global_context);
        registerCommand(mntr_command);

        FourLetterCommandPtr wchs_command = std::make_shared<WchsCommand>(global_context);
        registerCommand(wchs_command);

        FourLetterCommandPtr wchc_command = std::make_shared<WchcCommand>(global_context);
        registerCommand(wchc_command);

        FourLetterCommandPtr dump_command = std::make_shared<DumpCommand>(global_context);
        registerCommand(dump_command);

        FourLetterCommandPtr ruok_command = std::make_shared<RuokCommand>(global_context);
        registerCommand(ruok_command);

        initialized = true;
    }
}

}
