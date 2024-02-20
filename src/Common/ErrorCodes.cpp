#include <Common/ErrorCodes.h>

/** Previously, these constants were located in one enum.
  * But in this case there is a problem: when you add a new constant, you need to recompile
  * all translation units that use at least one constant (almost the whole project).
  * Therefore it is made so that definitions of constants are located here, in one file,
  * and their declaration are in different files, at the place of use.
  *
  * Later it was converted to the lookup table, to provide:
  * - errorCodeToName()
  * - system.errors table
  */

#define APPLY_FOR_ERROR_CODES(M) \
    M(0, OK) \
    M(1, LOGICAL_ERROR) \
    M(3, CANNOT_ALLOCATE_MEMORY) \
    M(4, CANNOT_MREMAP) \
    M(5, BAD_ARGUMENTS) \
    M(6, EPOLL_ERROR) \
    M(7, MEMORY_LIMIT_EXCEEDED) \
    M(8, PTHREAD_ERROR)          \
    M(9, CANNOT_PARSE_INPUT_ASSERTION_FAILED)          \
    M(10, CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER)          \
    M(11, CANNOT_WRITE_AFTER_END_OF_BUFFER)          \
    M(12, CANNOT_WRITE_TO_FILE_DESCRIPTOR)          \
    M(13, CANNOT_READ_FROM_FILE_DESCRIPTOR)          \
    M(14, CHILD_WAS_NOT_EXITED_NORMALLY)          \
    M(15, NOT_IMPLEMENTED)          \
    M(16, UNEXPECTED_PACKET_FROM_CLIENT)          \
    M(17, CANNOT_CREATE_CHILD_PROCESS)          \
    M(18, ALL_CONNECTION_TRIES_FAILED)          \
    M(19, UNKNOWN_SETTING)          \
    M(20, ARGUMENT_OUT_OF_BOUND)          \
    M(21, INVALID_CONFIG_PARAMETER)          \
    M(22, SYSTEM_ERROR)          \
    M(23, NETWORK_ERROR)          \
    M(24, CANNOT_GETTIMEOFDAY)          \
    M(25, CANNOT_PARSE_ESCAPE_SEQUENCE)          \
    M(26, SEEK_POSITION_OUT_OF_BOUND)          \
    M(27, CANNOT_PARSE_QUOTED_STRING)          \
    M(28, CANNOT_PARSE_DATETIME)          \
    M(30, CANNOT_RESTORE_FROM_FIELD_DUMP)          \
    M(31, BAD_GET)          \
    M(33, ILLEGAL_TYPE_OF_ARGUMENT)          \
    M(34, ATTEMPT_TO_READ_AFTER_EOF)          \
    M(35, TIMEOUT_EXCEEDED)          \
    M(36, READONLY)          \
    M(37, RAFT_ERROR)          \
    M(38, NO_ZOOKEEPER)          \
    M(39, CANNOT_MANIPULATE_SIGSET)          \
    M(40, CANNOT_SET_SIGNAL_HANDLER)          \
    M(41, CANNOT_CREATE_TIMER)          \
    M(42, CANNOT_SEEK_THROUGH_FILE)          \
    M(43, CANNOT_READ_FROM_SOCKET)          \
    M(44, UNKNOWN_FORMAT_VERSION)          \
    M(45, CANNOT_WRITE_TO_SOCKET)          \
    M(46, CANNOT_CREATE_DIRECTORY)          \
    M(47, CHECKSUM_DOESNT_MATCH)          \
    M(48, CANNOT_SCHEDULE_TASK)          \
    M(49, CANNOT_TRUNCATE_FILE)          \
    M(50, CANNOT_READ_ALL_DATA)          \
    M(51, CANNOT_CLOCK_GETTIME)          \
    M(52, CANNOT_PARSE_NUMBER)          \
    M(53, TOO_DEEP_RECURSION)          \
    M(54, CANNOT_PARSE_UUID)          \
    M(55, FILE_DOESNT_EXIST)          \
    M(56, CANNOT_PARSE_DATE)          \
    M(57, CANNOT_OPEN_FILE)          \
    M(58, CANNOT_CLOSE_FILE)          \
    M(59, CANNOT_MPROTECT)          \
    M(60, SOCKET_TIMEOUT)          \
    M(61, LIMIT_EXCEEDED)          \
    M(62, CANNOT_WAITPID)          \
    M(63, CANNOT_STATVFS)          \
    M(64, CANNOT_SELECT)          \
    M(65, CANNOT_MUNMAP)          \
    M(66, CANNOT_FSYNC)          \
    M(67, CANNOT_FSTAT)          \
    M(68, CANNOT_FCNTL)          \
    M(69, CANNOT_DLSYM)          \
    M(70, CANNOT_PIPE)          \
    M(71, CANNOT_FORK)          \
    M(72, CORRUPTED_DATA)          \
    M(73, CANNOT_PARSE_DWARF)          \
    M(74, CANNOT_PARSE_ELF)          \
    M(75, CANNOT_STAT)          \
    M(76, NETLINK_ERROR)          \
    M(78, EPOLL_CTL)          \
    M(79, EPOLL_CREATE)          \
    M(80, EPOLL_WAIT)          \
                                 \
    M(102, KEEPER_EXCEPTION) \
    M(103, POCO_EXCEPTION) \
    M(104, STD_EXCEPTION) \
    M(105, UNKNOWN_EXCEPTION) \
    M(106, INVALID_LOG_LEVEL)    \
    M(107, UNEXPECTED_ZOOKEEPER_ERROR) \
    M(108, UNEXPECTED_NODE_IN_ZOOKEEPER) \
    M(109, RAFT_FORWARD_ERROR) \
    M(110, CANNOT_PTHREAD_ATTR)  \
    M(111, UNEXPECTED_FORWARD_PACKET) \
    M(112, RAFT_IS_LEADER) \
    M(113, RAFT_NO_LEADER) \
    M(114, RAFT_FWD_NO_CONN) \
    M(115, FORWARD_NOT_CONNECTED) \
    M(116, ILLEGAL_SETTING_VALUE) \
    M(117, CORRUPTED_LOG) \
    M(118, CORRUPTED_SNAPSHOT) \
    /* See END */

namespace RK
{
namespace ErrorCodes
{
#define M(VALUE, NAME) extern const Value NAME = VALUE;
    APPLY_FOR_ERROR_CODES(M)
#undef M

    constexpr Value END = 1000;
    std::atomic<Value> values[END + 1]{};

    struct ErrorCodesNames
    {
        std::string_view names[END + 1];
        ErrorCodesNames()
        {
#define M(VALUE, NAME) names[VALUE] = std::string_view(#NAME);
            APPLY_FOR_ERROR_CODES(M)
#undef M
        }
    } error_codes_names;

    [[maybe_unused]] std::string_view getName(ErrorCode error_code)
    {
        if (error_code >= END)
            return std::string_view();
        return error_codes_names.names[error_code];
    }

    ErrorCode end() { return END + 1; }
}

}
