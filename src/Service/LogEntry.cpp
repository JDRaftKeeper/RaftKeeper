#include <Service/LogEntry.h>
#include <libnuraft/nuraft.hxx>

namespace RK
{
using nuraft::byte;
using nuraft::cs_new;

/// Add log entry type to head
ptr<buffer> LogEntryBody::serialize(const ptr<log_entry> & entry)
{
    auto data = entry->get_buf_ptr();
    data->pos(0);

    auto entry_buf = buffer::alloc(sizeof(char) + data->size());
    entry_buf->put((static_cast<byte>(entry->get_val_type())));

    entry_buf->put(*data);
    entry_buf->pos(0);

    return entry_buf;
}

ptr<log_entry> LogEntryBody::deserialize(const ptr<buffer> & serialized_entry)
{
    auto type = static_cast<nuraft::log_val_type>(*serialized_entry->data_begin());
    auto data = buffer::alloc(serialized_entry->size() - 1);

    data->put_raw(serialized_entry->data_begin() + 1, serialized_entry->size() - 1);
    data->pos(0);

    return cs_new<log_entry>(0, data, type); /// TODO term is set latter, it is not an intuitive way
}

}
