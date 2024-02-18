#include <Service/LogEntry.h>
#include <libnuraft/nuraft.hxx>

namespace RK
{
using nuraft::byte;
using nuraft::cs_new;


char * LogEntry::serializeEntry(ptr<log_entry> & entry, ptr<buffer> & entry_buf, size_t & buf_size)
{
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
    auto entry_buf = buffer::alloc(buf_size);
    entry_buf->put_raw(reinterpret_cast<const byte *>(entry_str), buf_size);

    entry_buf->pos(0);
    nuraft::log_val_type tp = static_cast<nuraft::log_val_type>(entry_buf->get_byte());

    ptr<buffer> data = buffer::copy(*entry_buf);
    return cs_new<log_entry>(term, data, tp);
}

}
