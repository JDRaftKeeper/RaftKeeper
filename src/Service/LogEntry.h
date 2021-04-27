#pragma once

#include <libnuraft/nuraft.hxx>
#include <common/types.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#    pragma clang diagnostic ignored "-Wheader-hygiene"
#endif
#include <Service/proto/Log.pb.h>
#ifdef __clang__
#    pragma clang diagnostic pop
#endif


namespace DB
{
using nuraft::buffer;
using nuraft::log_entry;
using nuraft::ptr;
using nuraft::ulong;


struct LogEntryHeader
{
    UInt64 term;
    UInt64 index;
    // The length of the batch data (uncompressed)
    UInt32 data_length;
    // The CRC32C of the batch data.
    // If compression is enabled, this is the checksum of the compressed data.
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
    //return entry count
    static ptr<log_entry> setTermAndIndex(ptr<log_entry> & entry, ulong term, ulong index);

    static char * serializeEntry(ptr<log_entry> & entry, ptr<buffer> & entry_buf, size_t & buf_size);
    static ptr<log_entry> parseEntry(const char * entry_str, const UInt64 & term, size_t buf_size);

    //serialize protobuf to nuraft buffer
    static ptr<buffer> serializePB(ptr<LogEntryPB> msg_pb);
    static ptr<buffer> serializePB(LogEntryPB & msg_pb);

    //parse nuraft buffer to protobuf
    static ptr<LogEntryPB> parsePB(ptr<buffer> msg_buf);
    static ptr<LogEntryPB> parsePB(buffer & msg_buf);
};

}
