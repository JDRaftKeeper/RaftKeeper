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

    /// The CRC32C of the log.
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

class LogEntryBody
{
public:
    static ptr<buffer> serialize(ptr<log_entry> & entry);
    static ptr<log_entry> deserialize(ptr<buffer> serialized_entry);
};

}
