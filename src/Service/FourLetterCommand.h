#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <sstream>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct FourLetterCommand;
using FourLetterCommandPtr = std::shared_ptr<DB::FourLetterCommand>;

struct FourLetterCommand
{
public:
    using StringBuffer = std::stringstream;
    explicit FourLetterCommand(const Context & global_context_);

    virtual String name() = 0;
    virtual void run(String & res) = 0;
    virtual ~FourLetterCommand();

    Int32 code();
    static void printSet(StringBuffer & ss, std::unordered_set<String> & target, String && prefix);

protected:
    const Context & global_context;
};

/***
 * Tests if server is running in a non-error state. The server will respond with imok if it is running.
 * Otherwise it will not respond at all.
 *
 * A response of "imok" does not necessarily indicate that the server has joined the quorum,
 * just that the server process is active and bound to the specified client port.
 * Use "stat" for details on state wrt quorum and client connection information.
 */
struct RuokCommand : public FourLetterCommand
{
    explicit RuokCommand(const Context & global_context_) : FourLetterCommand(global_context_) { }
    String name() override;
    void run(String & res) override;
    ~RuokCommand() override;
};

struct MntrCommand : public FourLetterCommand
{
    explicit MntrCommand(const Context & global_context_) : FourLetterCommand(global_context_) { }
    String name() override;
    void run(String & res) override;
    ~MntrCommand() override;
};

/**
 * Lists the outstanding sessions and ephemeral nodes. This only works on the leader.
 */
struct DumpCommand : public FourLetterCommand
{
    explicit DumpCommand(const Context & global_context_) : FourLetterCommand(global_context_) { }
    String name() override;
    void run(String & res) override;
    ~DumpCommand() override;
};

/**
 * Lists brief information on watches for the server.
 */
struct WchsCommand : public FourLetterCommand
{
    explicit WchsCommand(const Context & global_context_) : FourLetterCommand(global_context_) { }
    String name() override;
    void run(String & res) override;
    ~WchsCommand() override;
};

/**
 * Lists detailed information on watches for the server, by session.
 * This outputs a list of sessions(connections) with associated watches (paths).
 * Note, depending on the number of watches this operation may be expensive (ie impact server performance), use it carefully.
 */
struct WchcCommand : public FourLetterCommand
{
    explicit WchcCommand(const Context & global_context_) : FourLetterCommand(global_context_) { }
    String name() override;
    void run(String & res) override;
    ~WchcCommand() override;
};

struct FourLetterCommands
{
public:
    static volatile bool initialized;
    static std::unordered_map<Int32, FourLetterCommandPtr> commands;

    static bool isKnown(Int32 code);

    static FourLetterCommandPtr getCommand(Int32 code);

    /// There is no need to make it thread safe, because registration is no initialization and get is after startup.
    static void registerCommand(FourLetterCommandPtr & command);

    static void registerCommands(const Context & global_context);
};

}
