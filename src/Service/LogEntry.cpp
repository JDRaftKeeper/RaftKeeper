#include <Service/LogEntry.h>
#include <Service/proto/Log.pb.h>
#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>

namespace DB
{
using nuraft::byte;
using nuraft::cs_new;

ptr<log_entry> LogEntry::setTermAndIndex(ptr<log_entry> & entry, ulong term, ulong index)
{
    std::string pb_str(reinterpret_cast<const char *>(entry->get_buf().data()));
    ptr<LogEntryPB> entry_pb = cs_new<LogEntryPB>();
    entry_pb->ParseFromString(pb_str);
    entry_pb->mutable_log_index()->set_term(term);
    entry_pb->mutable_log_index()->set_index(index);
    std::string new_pb;
    entry_pb->SerializeToString(&new_pb);
    ptr<buffer> new_buf = buffer::alloc(sizeof(uint32_t) + new_pb.size());
    new_buf->put(new_pb);
    ptr<log_entry> new_entry = cs_new<log_entry>(entry->get_term(), new_buf, entry->get_val_type());
    return new_entry;
}

char * LogEntry::serializeEntry(ptr<log_entry> & entry, ptr<buffer> & entry_buf, size_t & buf_size)
{
    //entry_buf = entry->serialize();
    //buf_size = entry_buf->size();
    //return reinterpret_cast<char *>(entry_buf->get_raw(buf_size));
    ptr<buffer> data_buf = entry->get_buf_ptr();
    data_buf->pos(0);
    entry_buf = buffer::alloc(sizeof(char) + data_buf->size());
    entry_buf->put((static_cast<byte>(entry->get_val_type())));
    entry_buf->put(*data_buf);
    entry_buf->pos(0);
    buf_size = entry_buf->size();
    return reinterpret_cast<char *>(entry_buf->data_begin());
}

ptr<log_entry> LogEntry::parseEntry(const char * entry_str, const UInt64 & term, size_t buf_size)
{
    // auto entry_buf = buffer::alloc(buf_size);
    // entry_buf->put_raw(reinterpret_cast<const byte *>(entry_str), buf_size);
    // entry_buf->pos(0);
    // auto entry = log_entry::deserialize(*(entry_buf.get()));
    // return entry;
    auto entry_buf = buffer::alloc(buf_size);
    entry_buf->put_raw(reinterpret_cast<const byte *>(entry_str), buf_size);
    entry_buf->pos(0);
    nuraft::log_val_type tp = static_cast<nuraft::log_val_type>(entry_buf->get_byte());
    ptr<buffer> data = buffer::copy(*entry_buf);
    return cs_new<log_entry>(term, data, tp);
}

ptr<buffer> LogEntry::serializePB(ptr<LogEntryPB> msg_pb)
{
    LogEntryPB * entry = msg_pb.get();
    return serializePB(*entry);
}

ptr<buffer> LogEntry::serializePB(LogEntryPB & msg_pb)
{
    std::string msg_str;
    msg_pb.SerializeToString(&msg_str);
    ptr<buffer> msg_buf = buffer::alloc(sizeof(uint32_t) + msg_str.size());
    msg_buf->put(msg_str);
    msg_buf->pos(0);
    return msg_buf;
}

ptr<LogEntryPB> LogEntry::parsePB(ptr<buffer> msg_buf)
{
    return parsePB(*(msg_buf.get()));
}

ptr<LogEntryPB> LogEntry::parsePB(buffer & msg_buf)
{
    msg_buf.pos(0);
    ptr<LogEntryPB> entry_pb = cs_new<LogEntryPB>();
    std::string msg_str(msg_buf.get_str());
    entry_pb->ParseFromString(msg_str);
    return entry_pb;
}

}
