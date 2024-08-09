#include <Service/LogEntry.h>
#include <libnuraft/nuraft.hxx>

namespace RK
{
using nuraft::byte;
using nuraft::cs_new;

/// Add entry type to the log entry
ptr<buffer> LogEntryBody::serialize(ptr<log_entry> & entry)
{
    ptr<buffer> entry_buf;
    ptr<buffer> data = entry->get_buf_ptr();
    data->pos(0);

    entry_buf = buffer::alloc(sizeof(char) + data->size());
    entry_buf->put((static_cast<byte>(entry->get_val_type())));

    entry_buf->put(*data);
    entry_buf->pos(0);

    return entry_buf;
}

ptr<log_entry> LogEntryBody::deserialize(ptr<buffer> serialized_entry)
{
    nuraft::log_val_type type = static_cast<nuraft::log_val_type>(*serialized_entry->data_begin());
    auto data = buffer::alloc(serialized_entry->size() - 1);
    data->put_raw(serialized_entry->data_begin() + 1, serialized_entry->size() - 1);
    data->pos(0);
    return cs_new<log_entry>(0, data, type); /// term is set latter
}

}
