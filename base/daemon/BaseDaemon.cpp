#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>

#include <Common/setThreadName.h>
#include <daemon/BaseDaemon.h>
#if defined(__linux__)
#    include <sys/prctl.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <signal.h>
#include <string.h>
#include <typeinfo>
#include <unistd.h>

#include <Poco/Condition.h>
#include <Poco/ErrorHandler.h>
#include <Poco/Exception.h>
#include <Poco/File.h>
#include <Poco/Message.h>
#include <Poco/Path.h>
#include <Poco/Util/Application.h>

#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Common/IO/ReadBufferFromFileDescriptor.h>
#include <Common/IO/ReadHelpers.h>
#include <Common/IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <Common/IO/WriteHelpers.h>
#include <Common/PipeFDs.h>
#include <Common/StackTrace.h>
#include <common/ErrorHandlers.h>
#include <common/argsToConfig.h>
#include <common/coverage.h>
#include <common/sleep.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

#if defined(OS_DARWIN)
#    pragma GCC diagnostic ignored "-Wunused-macros"
#    define _XOPEN_SOURCE 700 // ucontext is not available without _XOPEN_SOURCE
#endif
#include <ucontext.h>

#include <Common/IO/WriteBufferFromFile.h>
#include <Common/SymbolIndex.h>
#include <Common/getExecutablePath.h>
#include <Common/getHashOfLoadedBinary.h>


RK::PipeFDs signal_pipe;


/** Reset signal handler to the default and send signal to itself.
  * It's called from user signal handler to write core dump.
  */
static void call_default_signal_handler(int sig)
{
    signal(sig, SIG_DFL);
    raise(sig);
}

static const size_t signal_pipe_buf_size = sizeof(int) + sizeof(siginfo_t) + sizeof(ucontext_t) + sizeof(StackTrace) + sizeof(UInt32)
    + sizeof(void *);


using signal_function = void(int, siginfo_t *, void *);

static void writeSignalIDtoSignalPipe(int sig)
{
    auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    RK::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);
    RK::writeBinary(sig, out);
    out.next();

    errno = saved_errno;
}

/** Signal handler for HUP / USR1 */
static void closeLogsSignalHandler(int sig, siginfo_t *, void *)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    writeSignalIDtoSignalPipe(sig);
}

static void terminateRequestedSignalHandler(int sig, siginfo_t *, void *)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    writeSignalIDtoSignalPipe(sig);
}


/** Handler for "fault" or diagnostic signals. Send data about fault to separate thread to write into log.
  */
static void signalHandler(int sig, siginfo_t * info, void * context)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    RK::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const ucontext_t signal_context = *reinterpret_cast<ucontext_t *>(context);
    const StackTrace stack_trace(signal_context);

    RK::writeBinary(sig, out);
    RK::writePODBinary(*info, out);
    RK::writePODBinary(signal_context, out);
    RK::writePODBinary(stack_trace, out);
    RK::writeBinary(UInt32(getThreadId()), out);

    out.next();

    if (sig != SIGTSTP) /// This signal is used for debugging.
    {
        /// The time that is usually enough for separate thread to print info into log.
        sleepForSeconds(20); /// FIXME: use some feedback from threads that process stacktrace
        call_default_signal_handler(sig);
    }

    errno = saved_errno;
}


/** The thread that read info about signal or std::terminate from pipe.
  * On HUP / USR1, close log files (for new files to be opened later).
  * On information about std::terminate, write it to log.
  * On other signals, write info to log.
  */
class SignalListener : public Poco::Runnable
{
public:
    enum Signals : int
    {
        StdTerminate = -1,
        StopThread = -2,
        SanitizerTrap = -3,
    };

    explicit SignalListener(BaseDaemon & daemon_) : log(&Poco::Logger::get("BaseDaemon")), daemon(daemon_) { }

    void run() override
    {
        char buf[signal_pipe_buf_size];
        RK::ReadBufferFromFileDescriptor in(signal_pipe.fds_rw[0], signal_pipe_buf_size, buf);

        while (!in.eof())
        {
            int sig = 0;
            RK::readBinary(sig, in);
            // We may log some specific signals afterwards, with different log
            // levels and more info, but for completeness we log all signals
            // here at trace level.
            // Don't use strsignal here, because it's not thread-safe.
            LOG_TRACE(log, "Received signal {}", sig);

            if (sig == Signals::StopThread)
            {
                LOG_INFO(log, "Stop SignalListener thread");
                break;
            }
            else if (sig == SIGHUP || sig == SIGUSR1)
            {
                LOG_DEBUG(log, "Received signal to close logs.");
                BaseDaemon::instance().closeLogs(BaseDaemon::instance().logger());
                LOG_INFO(log, "Opened new log file after received signal.");
            }
            else if (sig == Signals::StdTerminate)
            {
                UInt32 thread_num;
                std::string message;

                RK::readBinary(thread_num, in);
                RK::readBinary(message, in);

                onTerminate(message, thread_num);
            }
            else if (sig == SIGINT || sig == SIGQUIT || sig == SIGTERM)
            {
                daemon.handleSignal(sig);
            }
            else
            {
                siginfo_t info{};
                ucontext_t context{};
                StackTrace stack_trace(NoCapture{});
                UInt32 thread_num{};

                if (sig != SanitizerTrap)
                {
                    RK::readPODBinary(info, in);
                    RK::readPODBinary(context, in);
                }

                RK::readPODBinary(stack_trace, in);
                RK::readBinary(thread_num, in);

                /// This allows to receive more signals if failure happens inside onFault function.
                /// Example: segfault while symbolizing stack trace.
                std::thread([=, this] { onFault(sig, info, context, stack_trace, thread_num); }).detach();
            }
        }
    }

private:
    Poco::Logger * log;
    BaseDaemon & daemon;

    void onTerminate(const std::string & message, UInt32 thread_num) const
    {
        LOG_FATAL(log, "(version {}, {}) (from thread {}) {}", VERSION_STRING, daemon.build_id_info, thread_num, message);
    }

    void onFault(int sig, const siginfo_t & info, const ucontext_t & context, const StackTrace & stack_trace, UInt32 thread_num) const
    {
        LOG_FATAL(log, "########################################");

        LOG_FATAL(
            log,
            "(version {}, {}) (from thread {}) Received signal {} ({})",
            VERSION_STRING,
            daemon.build_id_info,
            thread_num,
            strsignal(sig),
            sig);

        String error_message;

        if (sig != SanitizerTrap)
            error_message = signalToErrorMessage(sig, info, context);
        else
            error_message = "Sanitizer trap.";

        LOG_FATAL(log, error_message);

        if (stack_trace.getSize())
        {
            /// Write bare stack trace (addresses) just in case if we will fail to print symbolized stack trace.
            /// NOTE: This still require memory allocations and mutex lock inside logger.
            ///       BTW we can also print it to stderr using write syscalls.

            std::stringstream bare_stacktrace;
            bare_stacktrace << "Stack trace:";
            for (size_t i = stack_trace.getOffset(); i < stack_trace.getSize(); ++i)
                bare_stacktrace << ' ' << stack_trace.getFramePointers()[i];

            LOG_FATAL(log, bare_stacktrace.str());
        }

        /// Write symbolized stack trace line by line for better grep-ability.
        stack_trace.toStringEveryLine([&](const std::string & s) { LOG_FATAL(log, s); });

#if defined(OS_LINUX)
        /// Write information about binary checksum. It can be difficult to calculate, so do it only after printing stack trace.
        String calculated_binary_hash = getHashOfLoadedBinaryHex();
        if (daemon.stored_binary_hash.empty())
        {
            LOG_FATAL(
                log,
                "Calculated checksum of the binary: {}."
                " There is no information about the reference checksum.",
                calculated_binary_hash);
        }
        else if (calculated_binary_hash == daemon.stored_binary_hash)
        {
            LOG_FATAL(log, "Checksum of the binary: {}, integrity check passed.", calculated_binary_hash);
        }
        else
        {
            LOG_FATAL(
                log,
                "Calculated checksum of the ClickHouse binary ({0}) does not correspond"
                " to the reference checksum stored in the binary ({1})."
                " It may indicate one of the following:"
                " - the file was changed just after startup;"
                " - the file is damaged on disk due to faulty hardware;"
                " - the loaded executable is damaged in memory due to faulty hardware;"
                " - the file was intentionally modified;"
                " - logical error in code.",
                calculated_binary_hash,
                daemon.stored_binary_hash);
        }
#endif
    }
};


#if defined(SANITIZER)
extern "C" void __sanitizer_set_death_callback(void (*)());

static void sanitizerDeathCallback()
{
    /// Also need to send data via pipe. Otherwise it may lead to deadlocks or failures in printing diagnostic info.

    char buf[signal_pipe_buf_size];
    RK::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const StackTrace stack_trace;

    int sig = SignalListener::SanitizerTrap;
    RK::writeBinary(sig, out);
    RK::writePODBinary(stack_trace, out);
    RK::writeBinary(UInt32(getThreadId()), out);

    out.next();

    /// The time that is usually enough for separate thread to print info into log.
    sleepForSeconds(10);
}
#endif


/** To use with std::set_terminate.
  * Collects slightly more info than __gnu_cxx::__verbose_terminate_handler,
  *  and send it to pipe. Other thread will read this info from pipe and asynchronously write it to log.
  * Look at libstdc++-v3/libsupc++/vterminate.cc for example.
  */
[[noreturn]] static void terminate_handler()
{
    static thread_local bool terminating = false;
    if (terminating)
        abort();

    terminating = true;

    std::string log_message;

    if (std::current_exception())
        log_message = "Terminate called for uncaught exception:\n" + RK::getCurrentExceptionMessage(true);
    else
        log_message = "Terminate called without an active exception";

    /// POSIX.1 says that write(2)s of less than PIPE_BUF bytes must be atomic - man 7 pipe
    /// And the buffer should not be too small because our exception messages can be large.
    static constexpr size_t buf_size = PIPE_BUF;

    if (log_message.size() > buf_size - 16)
        log_message.resize(buf_size - 16);

    char buf[buf_size];
    RK::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], buf_size, buf);

    RK::writeBinary(static_cast<int>(SignalListener::StdTerminate), out);
    RK::writeBinary(UInt32(getThreadId()), out);
    RK::writeBinary(log_message, out);
    out.next();

    abort();
}


static std::string createDirectory(const std::string & file)
{
    auto path = Poco::Path(file).makeParent();
    if (path.toString().empty())
        return "";
    Poco::File(path).createDirectories();
    return path.toString();
};


static bool tryCreateDirectories(Poco::Logger * logger, const std::string & path)
{
    try
    {
        Poco::File(path).createDirectories();
        return true;
    }
    catch (...)
    {
        LOG_WARNING(logger, "{}: when creating {}, {}", __PRETTY_FUNCTION__, path, RK::getCurrentExceptionMessage(true));
    }
    return false;
}


void BaseDaemon::reloadConfiguration()
{
    /** If the program is not run in daemon mode and 'config-file' is not specified,
      *  then we use config from 'config.xml' file in current directory,
      *  but will log to console (or use parameters --log-file, --errorlog-file from command line)
      *  instead of using files specified in config.xml.
      * (It's convenient to log in console when you start server without any command line parameters.)
      */
    config_path = config().getString("config-file", "config.xml");
    RK::ConfigProcessor config_processor(config_path, false, true);
    config_processor.setConfigPath(Poco::Path(config_path).makeParent().toString());
    loaded_config = config_processor.loadConfig(/* allow_zk_includes = */ true);

    if (last_configuration != nullptr)
        config().removeConfiguration(last_configuration);
    last_configuration = loaded_config.configuration.duplicate();
    config().add(last_configuration, PRIO_DEFAULT, false);
}


BaseDaemon::BaseDaemon() = default;


BaseDaemon::~BaseDaemon()
{
    writeSignalIDtoSignalPipe(SignalListener::StopThread);
    signal_listener_thread.join();
    /// Reset signals to SIG_DFL to avoid trying to write to the signal_pipe that will be closed after.
    for (int sig : handled_signals)
    {
        signal(sig, SIG_DFL);
    }
    signal_pipe.close();
}


void BaseDaemon::terminate()
{
    if (::raise(SIGTERM) != 0)
        throw Poco::SystemException("cannot terminate process");
}

void BaseDaemon::kill()
{
    dumpCoverageReportIfPossible();
    pid_file.reset();
    /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
    /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
    _exit(128 + SIGKILL);
}

std::string BaseDaemon::getDefaultCorePath() const
{
    return "/opt/cores/";
}

void BaseDaemon::closeFDs()
{
#if defined(OS_FREEBSD) || defined(OS_DARWIN)
    Poco::File proc_path{"/dev/fd"};
#else
    Poco::File proc_path{"/proc/self/fd"};
#endif
    if (proc_path.isDirectory()) /// Hooray, proc exists
    {
        std::vector<std::string> fds;
        /// in /proc/self/fd directory filenames are numeric file descriptors
        proc_path.list(fds);
        for (const auto & fd_str : fds)
        {
            int fd = RK::parse<int>(fd_str);
            if (fd > 2 && fd != signal_pipe.fds_rw[0] && fd != signal_pipe.fds_rw[1])
                ::close(fd);
        }
    }
    else
    {
        int max_fd = -1;
#if defined(_SC_OPEN_MAX)
        max_fd = sysconf(_SC_OPEN_MAX);
        if (max_fd == -1)
#endif
            max_fd = 256; /// bad fallback
        for (int fd = 3; fd < max_fd; ++fd)
            if (fd != signal_pipe.fds_rw[0] && fd != signal_pipe.fds_rw[1])
                ::close(fd);
    }
}

namespace
{
/// In debug version on Linux, increase oom score so that clickhouse is killed
/// first, instead of some service. Use a carefully chosen random score of 555:
/// the maximum is 1000, and chromium uses 300 for its tab processes. Ignore
/// whatever errors that occur, because it's just a debugging aid and we don't
/// care if it breaks.
#if defined(OS_LINUX) && !defined(NDEBUG)
void debugIncreaseOOMScore()
{
    const std::string new_score = "555";
    try
    {
        RK::WriteBufferFromFile buf("/proc/self/oom_score_adj");
        buf.write(new_score.c_str(), new_score.size());
        buf.close();
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(&Poco::Logger::root(), "Failed to adjust OOM score: '{}'.", e.displayText());
        return;
    }
    LOG_INFO(&Poco::Logger::root(), "Set OOM score adjustment to {}", new_score);
}
#else
void debugIncreaseOOMScore()
{
}
#endif
}

void BaseDaemon::initialize(Application & self)
{
    closeFDs();

    ServerApplication::initialize(self);

    /// now highest priority (lowest value) is PRIO_APPLICATION = -100, we want higher!
    argsToConfig(argv(), config(), PRIO_APPLICATION - 100);

    bool is_daemon = config().getBool("application.runAsDaemon", false);
    if (is_daemon)
    {
        /** When creating pid file and looking for config, will search for paths relative to the working path of the program when started.
          */
        std::string path = Poco::Path(config().getString("application.path")).setFileName("").toString();
        if (0 != chdir(path.c_str()))
            throw Poco::Exception("Cannot change directory to " + path);
    }

    reloadConfiguration();

    /// This must be done before creation of any files (including logs).
    mode_t umask_num = 0027;
    if (config().has("umask"))
    {
        std::string umask_str = config().getString("umask");
        std::stringstream stream;
        stream << umask_str;
        stream >> std::oct >> umask_num;
    }
    umask(umask_num);

    RK::ConfigProcessor(config_path).savePreprocessedConfig(loaded_config, "");

    /// Write core dump on crash.
    {
        struct rlimit rlim;
        if (getrlimit(RLIMIT_CORE, &rlim))
            throw Poco::Exception("Cannot getrlimit");
        /// 1 GiB by default. If more - it writes to disk too long.
        rlim.rlim_cur = config().getUInt64("core_dump.size_limit", 1024 * 1024 * 1024);

        if (rlim.rlim_cur && setrlimit(RLIMIT_CORE, &rlim))
        {
            /// It doesn't work under address/thread sanitizer. http://lists.llvm.org/pipermail/llvm-bugs/2013-April/027880.html
            std::cerr << "Cannot set max size of core file to " + std::to_string(rlim.rlim_cur) << std::endl;
        }
    }

    /// This must be done before any usage of DateLUT. In particular, before any logging.
    if (config().has("timezone"))
    {
        const std::string config_timezone = config().getString("timezone");
        if (0 != setenv("TZ", config_timezone.data(), 1))
            throw Poco::Exception("Cannot setenv TZ variable");

        tzset();
        DateLUT::setDefaultTimezone(config_timezone);
    }

    std::string log_path = config().getString("logger.path", "");
    if (!log_path.empty())
        log_path = Poco::Path(log_path).setFileName("").toString();

    /** Redirect stdout, stderr to separate files in the log directory (or in the specified file).
      * Some libraries write to stderr in case of errors in debug mode,
      *  and this output makes sense even if the program is run in daemon mode.
      * We have to do it before buildLoggers, for errors on logger initialization will be written to these files.
      * If logger.stderr is specified then stderr will be forcibly redirected to that file.
      */
    if ((!log_path.empty() && is_daemon) || config().has("logger.stderr"))
    {
        std::string stderr_path = config().getString("logger.stderr", log_path + "/stderr.log");
        if (!freopen(stderr_path.c_str(), "a+", stderr))
            throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);

        /// Disable buffering for stderr
        setbuf(stderr, nullptr);
    }

    if ((!log_path.empty() && is_daemon) || config().has("logger.stdout"))
    {
        std::string stdout_path = config().getString("logger.stdout", log_path + "/stdout.log");
        if (!freopen(stdout_path.c_str(), "a+", stdout))
            throw Poco::OpenFileException("Cannot attach stdout to " + stdout_path);
    }

    /// Change path for logging.
    if (!log_path.empty())
    {
        std::string path = createDirectory(log_path);
        if (is_daemon && chdir(path.c_str()) != 0)
            throw Poco::Exception("Cannot change directory to " + path);
    }
    else
    {
        if (is_daemon && chdir("/tmp") != 0)
            throw Poco::Exception("Cannot change directory to /tmp");
    }

    /// sensitive data masking rules are not used here
    buildLoggers(config(), logger(), self.commandName());

    /// Create pid file.
    if (config().has("pid"))
        pid_file.emplace(config().getString("pid"), RK::StatusFile::write_pid);

    if (is_daemon)
    {
        /** Change working directory to the directory to write core dumps.
          * We have to do it after buildLoggers, because there is the case when config files was in current directory.
          */

        std::string core_path = config().getString("core_path", "");
        if (core_path.empty())
            core_path = getDefaultCorePath();

        tryCreateDirectories(&logger(), core_path);

        Poco::File cores = core_path;
        if (!(cores.exists() && cores.isDirectory()))
        {
            core_path = !log_path.empty() ? log_path : "/opt/";
            tryCreateDirectories(&logger(), core_path);
        }

        if (0 != chdir(core_path.c_str()))
            throw Poco::Exception("Cannot change directory to " + core_path);
    }

    initializeTerminationAndSignalProcessing();
    logRevision();
    debugIncreaseOOMScore();
}


static void addSignalHandler(const std::vector<int> & signals, signal_function handler, std::vector<int> * out_handled_signals)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO;

#if defined(OS_DARWIN)
    sigemptyset(&sa.sa_mask);
    for (auto signal : signals)
        sigaddset(&sa.sa_mask, signal);
#else
    if (sigemptyset(&sa.sa_mask))
        throw Poco::Exception("Cannot set signal handler.");

    for (auto signal : signals)
        if (sigaddset(&sa.sa_mask, signal))
            throw Poco::Exception("Cannot set signal handler.");
#endif

    for (auto signal : signals)
        if (sigaction(signal, &sa, nullptr))
            throw Poco::Exception("Cannot set signal handler.");

    if (out_handled_signals)
        std::copy(signals.begin(), signals.end(), std::back_inserter(*out_handled_signals));
};


static void blockSignals(const std::vector<int> & signals)
{
    sigset_t sig_set;

#if defined(OS_DARWIN)
    sigemptyset(&sig_set);
    for (auto signal : signals)
        sigaddset(&sig_set, signal);
#else
    if (sigemptyset(&sig_set))
        throw Poco::Exception("Cannot block signal.");

    for (auto signal : signals)
        if (sigaddset(&sig_set, signal))
            throw Poco::Exception("Cannot block signal.");
#endif

    if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        throw Poco::Exception("Cannot block signal.");
};


void BaseDaemon::initializeTerminationAndSignalProcessing()
{
    std::set_terminate(terminate_handler);

    /// We want to avoid SIGPIPE when working with sockets and pipes, and just handle return value/errno instead.
    blockSignals({SIGPIPE});

    /// Setup signal handlers.
    /// SIGTSTP is added for debugging purposes. To output a stack trace of any running thread at anytime.

    addSignalHandler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE, SIGTSTP, SIGTRAP}, signalHandler, &handled_signals);
    addSignalHandler({SIGHUP, SIGUSR1}, closeLogsSignalHandler, &handled_signals);
    addSignalHandler({SIGINT, SIGQUIT, SIGTERM}, terminateRequestedSignalHandler, &handled_signals);

#if defined(SANITIZER)
    __sanitizer_set_death_callback(sanitizerDeathCallback);
#endif

    /// Set up Poco ErrorHandler for Poco Threads.
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    signal_pipe.setNonBlockingWrite();
    signal_pipe.tryIncreaseSize(1 << 20);

    signal_listener = std::make_unique<SignalListener>(*this);
    signal_listener_thread.start(*signal_listener);

#if defined(__ELF__) && !defined(__FreeBSD__)
    String build_id_hex = RK::SymbolIndex::instance()->getBuildIDHex();
    if (build_id_hex.empty())
        build_id_info = "no build id";
    else
        build_id_info = "build id: " + build_id_hex;
#else
    build_id_info = "no build id";
#endif

#if defined(__linux__)
    std::string executable_path = getExecutablePath();

    if (!executable_path.empty())
        stored_binary_hash = RK::Elf(executable_path).getBinaryHash();
#endif
}

void BaseDaemon::logRevision() const
{
    Poco::Logger::root().information(
        "Starting " + std::string{VERSION_FULL}
        + ", git commit hash: " + std::string{GIT_COMMIT_HASH}
        + ", binary built on: " + std::string{BUILD_TIME}
        + ", binary built id: " + build_id_info
        + ", PID: " + std::to_string(getpid()));
}

void BaseDaemon::defineOptions(Poco::Util::OptionSet & new_options)
{
    new_options.addOption(Poco::Util::Option("config-file", "C", "load configuration from a given file")
                              .required(false)
                              .repeatable(false)
                              .argument("<file>")
                              .binding("config-file"));

    new_options.addOption(Poco::Util::Option("log-file", "L", "use given log file")
                              .required(false)
                              .repeatable(false)
                              .argument("<file>")
                              .binding("logger.path"));

    new_options.addOption(Poco::Util::Option("errorlog-file", "E", "use given log file for errors only")
                              .required(false)
                              .repeatable(false)
                              .argument("<file>")
                              .binding("logger.err_log_path"));

    new_options.addOption(
        Poco::Util::Option("pid-file", "P", "use given pidfile").required(false).repeatable(false).argument("<file>").binding("pid"));

    Poco::Util::ServerApplication::defineOptions(new_options);
}

void BaseDaemon::handleSignal(int signal_id)
{
    if (signal_id == SIGINT || signal_id == SIGQUIT || signal_id == SIGTERM)
    {
        std::unique_lock<std::mutex> lock(signal_handler_mutex);
        {
            ++terminate_signals_counter;
            sigint_signals_counter += signal_id == SIGINT;
            signal_event.notify_all();
        }

        onInterruptSignals(signal_id);
    }
    else
        throw RK::Exception(std::string("Unsupported signal: ") + strsignal(signal_id), 0);
}

void BaseDaemon::onInterruptSignals(int signal_id)
{
    is_cancelled = true;
    LOG_INFO(&logger(), "Received termination signal ({})", strsignal(signal_id));

    if (sigint_signals_counter >= 2)
    {
        LOG_INFO(&logger(), "Received second signal Interrupt. Immediately terminate.");
        call_default_signal_handler(signal_id);
        /// If the above did not help.
        _exit(128 + signal_id);
    }
}


void BaseDaemon::waitForTerminationRequest()
{
    /// NOTE: as we already process signals via pipe, we don't have to block them with sigprocmask in threads
    std::unique_lock<std::mutex> lock(signal_handler_mutex);
    signal_event.wait(lock, [this]() { return terminate_signals_counter > 0; });
}
