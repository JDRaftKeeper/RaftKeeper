#include <signal.h>
#include <setjmp.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/mman.h>
#endif

#include <new>
#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <utility>
#include <Common/StringUtils/StringUtils.h>
#include <common/phdr_cache.h>
#include <ext/scope_guard.h>


int mainEntryRaftKeeperServer(int argc, char ** argv);
int mainEntryRaftKeeperConverter(int argc, char ** argv);


#define ARRAY_SIZE(a) (sizeof(a)/sizeof((a)[0]))

namespace
{

using MainFunc = int (*)(int, char**);


/// Add an item here to register new application
std::pair<const char *, MainFunc> raftkeeper_applications[] =
{
    {"server", mainEntryRaftKeeperServer},
    {"converter", mainEntryRaftKeeperConverter},
};


int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : raftkeeper_applications)
        std::cerr << "raftkeeper " << application.first << " [args] " << std::endl;
    return -1;
}


bool isRaftKeeperApp(const std::string & app_suffix, std::vector<char *> & argv)
{
    /// Use app if the first arg 'app' is passed (the arg should be quietly removed)
    if (argv.size() >= 2)
    {
        auto first_arg = argv.begin() + 1;

        /// 'raftkeeper --client ...' and 'raftkeeper client ...' are Ok
        if (*first_arg == "--" + app_suffix || *first_arg == app_suffix)
        {
            argv.erase(first_arg);
            return true;
        }
    }

    /// Use app if raftkeeper binary is run through symbolic link with name raftkeeper-app
    std::string app_name = "raftkeeper-" + app_suffix;
    return !argv.empty() && (app_name == argv[0] || endsWith(argv[0], "/" + app_name));
}


enum class InstructionFail
{
    NONE = 0,
    SSE3 = 1,
    SSSE3 = 2,
    SSE4_1 = 3,
    SSE4_2 = 4,
    POPCNT = 5,
    AVX = 6,
    AVX2 = 7,
    AVX512 = 8
};

std::pair<const char *, size_t> instructionFailToString(InstructionFail fail)
{
    switch (fail)
    {
#define ret(x) return std::make_pair(x, ARRAY_SIZE(x) - 1)
        case InstructionFail::NONE:
            ret("NONE");
        case InstructionFail::SSE3:
            ret("SSE3");
        case InstructionFail::SSSE3:
            ret("SSSE3");
        case InstructionFail::SSE4_1:
            ret("SSE4.1");
        case InstructionFail::SSE4_2:
            ret("SSE4.2");
        case InstructionFail::POPCNT:
            ret("POPCNT");
        case InstructionFail::AVX:
            ret("AVX");
        case InstructionFail::AVX2:
            ret("AVX2");
        case InstructionFail::AVX512:
            ret("AVX512");
    }
    __builtin_unreachable();
}


sigjmp_buf jmpbuf;

[[noreturn]] void sigIllCheckHandler(int, siginfo_t *, void *)
{
    siglongjmp(jmpbuf, 1);
}

/// Check if necessary SSE extensions are available by trying to execute some sse instructions.
/// If instruction is unavailable, SIGILL will be sent by kernel.
void checkRequiredInstructionsImpl(volatile InstructionFail & fail)
{
#if defined(__SSE3__)
    fail = InstructionFail::SSE3;
    __asm__ volatile ("addsubpd %%xmm0, %%xmm0" : : : "xmm0");
#endif

#if defined(__SSSE3__)
    fail = InstructionFail::SSSE3;
    __asm__ volatile ("pabsw %%xmm0, %%xmm0" : : : "xmm0");

#endif

#if defined(__SSE4_1__)
    fail = InstructionFail::SSE4_1;
    __asm__ volatile ("pmaxud %%xmm0, %%xmm0" : : : "xmm0");
#endif

#if defined(__SSE4_2__)
    fail = InstructionFail::SSE4_2;
    __asm__ volatile ("pcmpgtq %%xmm0, %%xmm0" : : : "xmm0");
#endif

    /// Defined by -msse4.2
#if defined(__POPCNT__)
    fail = InstructionFail::POPCNT;
    {
        uint64_t a = 0;
        uint64_t b = 0;
        __asm__ volatile ("popcnt %1, %0" : "=r"(a) :"r"(b) :);
    }
#endif

#if defined(__AVX__)
    fail = InstructionFail::AVX;
    __asm__ volatile ("vaddpd %%ymm0, %%ymm0, %%ymm0" : : : "ymm0");
#endif

#if defined(__AVX2__)
    fail = InstructionFail::AVX2;
    __asm__ volatile ("vpabsw %%ymm0, %%ymm0" : : : "ymm0");
#endif

#if defined(__AVX512__)
    fail = InstructionFail::AVX512;
    __asm__ volatile ("vpabsw %%zmm0, %%zmm0" : : : "zmm0");
#endif

    fail = InstructionFail::NONE;
}

/// This function is safe to use in static initializers.
void writeErrorLen(const char * data, size_t size)
{
    while (size != 0)
    {
        ssize_t res = ::write(STDERR_FILENO, data, size);

        if ((-1 == res || 0 == res) && errno != EINTR)
            _Exit(1);

        if (res > 0)
        {
            data += res;
            size -= res;
        }
    }
}
/// Macros to avoid using strlen(), since it may fail if SSE is not supported.
#define writeError(data) do \
    { \
        static_assert(__builtin_constant_p(data)); \
        writeErrorLen(data, ARRAY_SIZE(data) - 1); \
    } while (false)

/// Check SSE and others instructions availability. Calls exit on fail.
/// This function must be called as early as possible, even before main, because static initializers may use unavailable instructions.
void checkRequiredInstructions()
{
    struct sigaction sa{};
    struct sigaction sa_old{};
    sa.sa_sigaction = sigIllCheckHandler;
    sa.sa_flags = SA_SIGINFO;
    auto signal = SIGILL;
    if (sigemptyset(&sa.sa_mask) != 0
        || sigaddset(&sa.sa_mask, signal) != 0
        || sigaction(signal, &sa, &sa_old) != 0)
    {
        /// You may wonder about strlen.
        /// Typical implementation of strlen is using SSE4.2 or AVX2.
        /// But this is not the case because it's compiler builtin and is executed at compile time.

        writeError("Can not set signal handler\n");
        _Exit(1);
    }

    volatile InstructionFail fail = InstructionFail::NONE;

    if (sigsetjmp(jmpbuf, 1))
    {
        writeError("Instruction check fail. The CPU does not support ");
        std::apply(writeErrorLen, instructionFailToString(fail));
        writeError(" instruction set.\n");
        _Exit(1);
    }

    checkRequiredInstructionsImpl(fail);

    if (sigaction(signal, &sa_old, nullptr))
    {
        writeError("Can not set signal handler\n");
        _Exit(1);
    }
}

struct Checker
{
    Checker()
    {
        checkRequiredInstructions();
    }
} checker;

}


/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
bool inside_main = false;


int main(int argc_, char ** argv_)
{
    inside_main = true;
    SCOPE_EXIT({ inside_main = false; });

    /// Reset new handler to default (that throws std::bad_alloc)
    /// It is needed because LLVM library clobbers it.
    std::set_new_handler(nullptr);

    /// PHDR cache is required for query profiler to work reliably
    /// It also speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    ///  will work only after additional call of this function.
    updatePHDRCache();

    std::vector<char *> argv(argv_, argv_ + argc_);

    /// Print a basic help if nothing was matched
    MainFunc main_func = printHelp;

    for (auto & application : raftkeeper_applications)
    {
        if (isRaftKeeperApp(application.first, argv))
        {
            main_func = application.second;
            break;
        }
    }

    return main_func(static_cast<int>(argv.size()), argv.data());
}
