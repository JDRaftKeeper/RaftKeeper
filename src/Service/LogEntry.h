#pragma once

#include <libnuraft/nuraft.hxx>
#include <common/types.h>


namespace RK
{
using nuraft::buffer;
using nuraft::log_entry;
using nuraft::ptr;
using nuraft::ulong;

struct LogEntryHeader
{
    /// log term
    UInt64 term;
    UInt64 index;

    /// The length of the batch data (uncompressed)
    UInt32 data_length;

    /// The CRC32C of the batch data.
    /// If compression is enabled, this is the checksum of the compressed data.
    UInt32 data_crc;

    void reset()
    {
        term = 0;
        index = 0;
        data_length = 0;
        data_crc = 0;
    }
    static constexpr size_t HEADER_SIZE = 24;
};

class LogEntry
{
public:
    /// serialize log to nuraft buffer
    static char * serializeEntry(ptr<log_entry> & entry, ptr<buffer> & entry_buf, size_t & buf_size);
    /// parse log from buffer
    static ptr<log_entry> parseEntry(const char * entry_str, const UInt64 & term, size_t buf_size);
};

}
