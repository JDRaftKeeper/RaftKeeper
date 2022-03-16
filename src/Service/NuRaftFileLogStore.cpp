#include <cassert>
#include <memory>
#include <unistd.h>
#include <Service/LogEntry.h>
#include <Service/NuRaftFileLogStore.h>

namespace DB
{
using namespace nuraft;

ptr<log_entry> makeClone(const ptr<log_entry> & entry)
{
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
    return clone;
}

ptr<log_entry> LogEntryQueue::getEntry(const UInt64 & index)
{
    //LOG_DEBUG(log,"get entry {}, index {}, batch {}", index, index & (MAX_VECTOR_SIZE - 1), index >> BIT_SIZE);
    std::shared_lock read_lock(queue_mutex);
    if (index > max_index || max_index - index >= MAX_VECTOR_SIZE)
        return nullptr;

    if (index >> BIT_SIZE == batch_index || index >> BIT_SIZE == batch_index - 1)
    {
        return entry_vec[index & (MAX_VECTOR_SIZE - 1)];
    }
    else
    {
        return nullptr;
    }
}

void LogEntryQueue::putEntry(UInt64 & index, ptr<log_entry> & entry)
{
    std::lock_guard write_lock(queue_mutex);
    entry_vec[index & (MAX_VECTOR_SIZE - 1)] = entry;
    batch_index = std::max(batch_index, index >> BIT_SIZE);
    max_index = std::max(max_index, index);
    //LOG_DEBUG(log,"put entry {}, index {}, batch {}", index, index & (MAX_VECTOR_SIZE - 1), batch_index);
}

void LogEntryQueue::putEntryOrClear(UInt64 & index, ptr<log_entry> & entry)
{
    std::lock_guard write_lock(queue_mutex);
    if (index >> BIT_SIZE == batch_index || index >> BIT_SIZE == batch_index - 1)
    {
        entry_vec[index & (MAX_VECTOR_SIZE - 1)] = entry;
        max_index = index;
    }
    else
    {
        LOG_DEBUG(log, "clear log queue.");
        batch_index = 0;
        max_index = 0;
        for (size_t i = 0; i < MAX_VECTOR_SIZE; ++i)
            entry_vec[i] = nullptr;
    }
}

void LogEntryQueue::clear()
{
    LOG_DEBUG(log, "clear log queue.");
    std::lock_guard write_lock(queue_mutex);
    batch_index = 0;
    max_index = 0;
    for (size_t i = 0; i < MAX_VECTOR_SIZE; ++i)
        entry_vec[i] = nullptr;
}

NuRaftFileLogStore::NuRaftFileLogStore(const std::string & log_dir, bool force_new, bool force_sync_): force_sync(force_sync_)
{
    log = &(Poco::Logger::get("FileLogStore"));

    segment_store = LogSegmentStore::getInstance(log_dir, force_new);

    if (segment_store->init() >= 0)
    {
        LOG_INFO(log, "Init file log store, last log index {}, log dir {}", segment_store->lastLogIndex(), log_dir);
    }
    else
    {
        LOG_WARNING(log, "Init file log store failed, log dir {}", log_dir);
        return;
    }

    if (segment_store->lastLogIndex() < 1)
    {
        /// no log entry exists, return a dummy constant entry with value set to null and term set to zero
        last_log_entry = cs_new<log_entry>(0, nuraft::buffer::alloc(0));
    }
    else
    {
        last_log_entry = segment_store->getEntry(segment_store->lastLogIndex());
    }
}

NuRaftFileLogStore::NuRaftFileLogStore(
    const std::string & log_dir, bool force_new, UInt32 max_log_size_, UInt32 max_segment_count_, bool force_sync_)
    : force_sync(force_sync_)
{
    log = &(Poco::Logger::get("FileLogStore"));

    segment_store = LogSegmentStore::getInstance(log_dir, force_new);

    segment_store->init(max_log_size_, max_segment_count_);

    if (segment_store->lastLogIndex() < 1)
    {
        /// no log entry exists, return a dummy constant entry with value set to null and term set to zero
        last_log_entry = cs_new<log_entry>(0, nuraft::buffer::alloc(0));
    }
    else
    {
        last_log_entry = segment_store->getEntry(segment_store->lastLogIndex());
    }
}

NuRaftFileLogStore::~NuRaftFileLogStore()
{
}

ptr<log_entry> NuRaftFileLogStore::make_clone(const ptr<log_entry> & entry)
{
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
    return clone;
}

ulong NuRaftFileLogStore::next_slot() const
{
#ifdef _TEST_MEMORY_
    return last_log_index_ + 1;
#else
    return segment_store->lastLogIndex() + 1;
#endif
}

ulong NuRaftFileLogStore::start_index() const
{
#ifdef _TEST_MEMORY_
    return 1;
#else
    return segment_store->firstLogIndex();
#endif
}

ptr<log_entry> NuRaftFileLogStore::last_entry() const
{
    //    std::lock_guard<std::recursive_mutex> lock(log_lock);
    if (last_log_entry)
        return make_clone(last_log_entry);
    else
        return nullptr;
}

ulong NuRaftFileLogStore::append(ptr<log_entry> & entry)
{
    ptr<log_entry> clone = makeClone(entry);
#ifdef _TEST_MEMORY_
    UInt64 log_index = last_log_index_.load(std::memory_order_acquire) + 1;
    log_queue.putEntry(log_index, clone);
    {
        std::lock_guard<std::recursive_mutex> lock(log_lock);
        mem_logs_[log_index] = clone;
    }
    last_log_index_.fetch_add(1, std::memory_order_acquire);
#else
    //std::lock_guard write_lock(index_mutex);
    UInt64 log_index = segment_store->appendEntry(entry);
    log_queue.putEntry(log_index, clone);
#endif

    last_log_entry = clone;
    return log_index;
}

void NuRaftFileLogStore::write_at(ulong index, ptr<log_entry> & entry)
{
    //std::lock_guard<std::recursive_mutex> lock(log_lock);
    //ptr<LogEntry> ch_entry = std::static_pointer_cast<LogEntry>(entry);
    //logs_count += ch_entry->setIndex(index);
    //ptr<log_entry> new_entry = LogEntry::setTermAndIndex(entry, entry->get_term(), index);
    if (segment_store->writeAt(index, entry) == index)
    {
        log_queue.clear();
    }

    //last_log_entry = std::dynamic_pointer_cast<log_entry>(ch_entry);
    last_log_entry = entry;
    LOG_DEBUG(log, "write entry at {}", index);
}

void NuRaftFileLogStore::end_of_append_batch(ulong start, ulong cnt)
{
    LOG_TRACE(log, "fsync log store, start log idx {}, log count {}", start, cnt);
    to_flush_count++;
    if (force_sync && to_flush_count % 1000 == 0)
    {
        to_flush_count = 0;
        flush();
    }
}

ptr<std::vector<ptr<log_entry>>> NuRaftFileLogStore::log_entries(ulong start, ulong end)
{
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();
    //segment_store->getEntries(start, end, ret);
    for (auto i = start; i < end; i++)
    {
        ret->push_back(entry_at(i));
        //ptr<nuraft::log_entry> src = nullptr;
    }
    LOG_DEBUG(log, "log entries, start {} end {}", start, end);
    return ret;
}

ptr<std::vector<ptr<log_entry>>> NuRaftFileLogStore::log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes)
{
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();
    //segment_store->getEntriesExt(start, end, batch_size_hint_in_bytes, ret);
    int64 get_size = 0;
    int64 entry_size = 0;
    for (auto i = start; i < end; i++)
    {
        auto entry_ptr = entry_at(i);
        entry_size = entry_ptr->get_buf().size() + sizeof(ulong) + sizeof(char);
        if (batch_size_hint_in_bytes > 0 && get_size + entry_size > batch_size_hint_in_bytes)
        {
            break;
        }
        ret->push_back(entry_ptr);
        get_size += entry_size;
    }
    LOG_DEBUG(log, "log entries ext, start {} end {}, real size {}, max size {}", start, end, get_size, batch_size_hint_in_bytes);
    return ret;
}

ptr<log_entry> NuRaftFileLogStore::entry_at(ulong index)
{
    ptr<nuraft::log_entry> src = nullptr;
#ifdef _TEST_MEMORY_
    auto entry = mem_logs_.find(index);
    if (entry != mem_logs_.end())
    {
        src = entry->second;
        return makeClone(src);
    }
    else
    {
        return nullptr;
    }
#else
    {
        //std::lock_guard write_lock(index_mutex);
        src = log_queue.getEntry(index);
        if (src == nullptr)
        {
            src = segment_store->getEntry(index);
            LOG_TRACE(log, "get entry {} from disk", index);
            //2^16, 65536
            if (index << 48 == 0)
            {
                //LOG_DEBUG(log, "get entry {} from disk", index);
            }
        }
        else
        {
            LOG_TRACE(log, "get entry {} from queue", index);
        }
    }
    if (src)
        return make_clone(src);
    else
        return nullptr;
#endif
    //    return src;
}

ulong NuRaftFileLogStore::term_at(ulong index)
{
    /// TODO zx
    if (entry_at(index))
        return entry_at(index)->get_term();
    else
        return 0;
}

ptr<buffer> NuRaftFileLogStore::pack(ulong index, int32 cnt)
{
    ptr<std::vector<ptr<log_entry>>> entries = log_entries(index, index + cnt);

    std::vector<ptr<buffer>> logs;
    size_t size_total = 0;
    for (auto it = entries->begin(); it != entries->end(); it++)
    {
        ptr<log_entry> le = *it;
        ptr<buffer> buf = le->serialize();
        size_total += buf->size();
        logs.push_back(buf);
    }

    ptr<buffer> buf_out = buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
    buf_out->pos(0);
    buf_out->put(cnt);

    for (auto & entry : logs)
    {
        ptr<buffer> & bb = entry;
        buf_out->put(static_cast<int32>(bb->size()));
        buf_out->put(*bb);
    }

    LOG_DEBUG(log, "pack log start {}, count {}", index, cnt);

    return buf_out;
}

void NuRaftFileLogStore::apply_pack(ulong index, buffer & pack)
{
    pack.pos(0);
    int32 num_logs = pack.get_int();

    for (int32 ii = 0; ii < num_logs; ++ii)
    {
        ulong cur_idx = index + ii;
        int32 buf_size = pack.get_int();

        ptr<buffer> buf_local = buffer::alloc(buf_size);
        pack.get(buf_local);

        if (cur_idx - segment_store->lastLogIndex() != 1)
            LOG_WARNING(log, "cur_idx {}, segment_store last_log_index {}, difference is not 1", cur_idx, segment_store->lastLogIndex());
        else
            LOG_DEBUG(log, "cur_idx {}, segment_store last_log_index {}", cur_idx, segment_store->lastLogIndex());

        ptr<log_entry> le = log_entry::deserialize(*buf_local);
        //if (cur_idx - segment_store->lastLogIndex() == 1)
        //  segment_store->appendEntry(le);
        {
            segment_store->writeAt(cur_idx, le);
        }
    }
    LOG_DEBUG(log, "apply pack {}", index);
}

//last_log_index : last removed log index
bool NuRaftFileLogStore::compact(ulong last_log_index)
{
    //std::lock_guard<std::recursive_mutex> lock(log_lock);
    segment_store->removeSegment(last_log_index + 1);
    log_queue.clear();
    //start_idx = last_log_index + 1;
    LOG_DEBUG(log, "compact last_log_index {}", last_log_index);
    return true;
}

/*
void FileLogStore::close()
{
    segment_store->close();
}
*/

bool NuRaftFileLogStore::flush()
{
    return segment_store->flush() == 0;
}
}
