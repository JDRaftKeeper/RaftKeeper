#include <atomic>
#include <cassert>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <Service/NuRaftStateMachine.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <Service/proto/Log.pb.h>
#include <Poco/File.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>


#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif


using namespace nuraft;

namespace DB
{
nuraft::ptr<nuraft::buffer> writeResponses(SvsKeeperStorage::ResponsesForSessions & responses)
{
    WriteBufferFromNuraftBuffer buffer;
    for (const auto & response_and_session : responses)
    {
        writeIntBinary(response_and_session.session_id, buffer);
        response_and_session.response->write(buffer);
    }
    return buffer.getBuffer();
}


SvsKeeperStorage::RequestForSession NuRaftStateMachine::parseRequest(nuraft::buffer & data)
{
    //auto log = &(Poco::Logger::get("KeeperStateMachine"));

    ReadBufferFromNuraftBuffer buffer(data);
    SvsKeeperStorage::RequestForSession request_for_session;
    readIntBinary(request_for_session.session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    request_for_session.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session.request->xid = xid;
    request_for_session.request->readImpl(buffer);

    //LOG_DEBUG(log, "Parse session id {}, length {}, xid {}, opnum {}", request_for_session.session_id, length, xid, opnum);

    return request_for_session;
}

ptr<buffer> NuRaftStateMachine::serializeRequest(SvsKeeperStorage::RequestForSession & session_request)
{
    WriteBufferFromNuraftBuffer out;
    Coordination::write(session_request.session_id, out);
    session_request.request->write(out);
    //auto log = &(Poco::Logger::get("KeeperStateMachine"));
    //LOG_INFO(log, "Serialize size {}", out.getBuffer()->size());
    return out.getBuffer();
}

/*
bool KeeperStateMachine::updateParentPath(std::string curr_path)
{
    size_t pos = curr_path.rfind("/");
    std::string parent_path = curr_path.substr(0, pos);
    if (node_map.find(parent_path) != node_map.end())
    {
        ptr<DataNode> parent_node = node_map[parent_path];
        parent_node->addChild(curr_path);
    }
    return true;
}

bool KeeperStateMachine::removePathRecursion(const std::string & curr_path)
{
    auto it = node_map.find(curr_path);
    if (it != node_map.end())
    {
        ptr<DataNode> node = it->second;
        const StringVec children = node->getChildren();
        for (auto child = children.begin(); child != children.end(); child++)
        {
            removePathRecursion(*child);
        }
        node_map.erase(curr_path);
    }
    return true;
}*/

/*
bool KeeperStateMachine::serializeNodes(const NodeMap & nodes, ptr<buffer> & buff)
{
    return true;
}

bool KeeperStateMachine::deserializeNodes(const ptr<buffer> & buff, NodeMap & nodes)
{

    return true;
}
*/

ptr<buffer> NuRaftStateMachine::pre_commit(const ulong log_idx, buffer & data)
{
    // Nothing to do with pre-commit in this example.
    LOG_INFO(log, "pre commit, log indx {}, data size {}", log_idx, data.size());
    return nullptr;
}

void NuRaftStateMachine::rollback(const ulong log_idx, buffer & data)
{
    // Nothing to do with rollback,
    // as this example doesn't do anything on pre-commit.
    LOG_INFO(log, "pre commit, log indx {}, data size {}", log_idx, data.size());
}


//ptr<buffer> KeeperStateMachine::commit(const ulong log_idx, buffer & data)
//{
//    last_committed_idx = log_idx;
//    ptr<LogEntryPB> entry_pb = LogEntry::parsePB(data);
//    if (entry_pb->entry_type() == ENTRY_TYPE_DATA)
//    {
//        std::lock_guard<std::mutex> lock(value_lock);
//        for (int idx = 0; idx < entry_pb->data_size(); idx++)
//        {
//            LogDataPB data_pb = entry_pb->data(idx);
//            std::string key = data_pb.key();
//            switch (data_pb.op_type())
//            {
//                case OP_TYPE_CREATE:
//                case OP_TYPE_SET: {
//                    auto it = node_map.find(key);
//                    if (it != node_map.end())
//                    {
//                        ptr<DataNode> node = it->second;
//                        node->setData(data);
//                    }
//                    else
//                    {
//                        //LOG_ERROR(logger, "Cant find node {} in state machine", key);
//                        ptr<DataNode> node = cs_new<DataNode>();
//                        node->setData(data);
//                        node_map[key] = node;
//                        updateParentPath(key);
//                    }
//                    break;
//                }
//                case OP_TYPE_REMOVE: {
//                    if (node_map.find(key) != node_map.end())
//                    {
//                        removePathRecursion(key);
//                    }
//                    else
//                    {
//                        LOG_ERROR(log, "Node {} that does not exist cannot be deleted in state machine", key);
//                    }
//                    break;
//                }
//                default:
//                    break;
//            }
//        }
//    }
//    ptr<buffer> ret = buffer::alloc(sizeof(log_idx));
//    buffer_serializer bs(ret);
//    bs.put_u64(log_idx);
//    return ret;
//}

nuraft::ptr<nuraft::buffer> NuRaftStateMachine::commit(const ulong log_idx, nuraft::buffer & data)
{
    //LOG_INFO(log, "Begin commit index {}", log_idx);
    if (data.size() == sizeof(int64_t))
    {
        nuraft::buffer_serializer timeout_data(data);
        int64_t session_timeout_ms = timeout_data.get_i64();
        auto response = nuraft::buffer::alloc(sizeof(int64_t));
        int64_t session_id;
        nuraft::buffer_serializer bs(response);
        {
            session_id = storage.getSessionID(session_timeout_ms);
            bs.put_i64(session_id);
        }
        LOG_DEBUG(log, "Session ID response {} with timeout {}", session_id, session_timeout_ms);
        last_committed_idx = log_idx;
        return response;
    }
    else
    {
        auto request_for_session = parseRequest(data);
        SvsKeeperStorage::ResponsesForSessions responses_for_sessions;
        {
            LOG_DEBUG(
                log,
                "Commit log index {}, SessionID/XID #{}#{}",
                log_idx,
                request_for_session.session_id,
                request_for_session.request->xid);
            /// TODO atomic process request and add response to queue
            responses_for_sessions = storage.processRequest(request_for_session.request, request_for_session.session_id);
            for (auto & response_for_session : responses_for_sessions)
                responses_queue.push(response_for_session);
        }

        last_committed_idx = log_idx;
        return nullptr;
    }
}


void NuRaftStateMachine::processReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session)
{
    SvsKeeperStorage::ResponsesForSessions responses;
    {
        responses = storage.processRequest(request_for_session.request, request_for_session.session_id);
    }
    for (const auto & response : responses)
        responses_queue.push(response);
}

std::unordered_set<int64_t> NuRaftStateMachine::getDeadSessions()
{
    return storage.getDeadSessions();
}

void NuRaftStateMachine::shutdownStorage()
{
    storage.finalize();
}

bool compareTime(const std::string & s1, const std::string & s2)
{
    return (s1 > s2);
}

void getDateFromFile(const std::string file_name, std::string & date)
{
    std::size_t p1 = file_name.find("_");
    std::size_t p2 = file_name.rfind("_");
    date = file_name.substr(p1 + 1, p2);
}

bool NuRaftStateMachine::chk_create_snapshot()
{
    return chk_create_snapshot(0L);
}

bool NuRaftStateMachine::chk_create_snapshot(time_t curr_time)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    std::vector<std::string> file_vec;
    Poco::File file_dir(snapshot_dir);
    std::string last_snap_name;
    //BackendTimer::getInitTime(last_snap_name);
    std::vector<std::string> time_vec;
    if (file_dir.exists())
    {
        file_dir.list(file_vec);
        for (auto it = file_vec.begin(); it != file_vec.end(); it++)
        {
            std::string time;
            KeeperSnapshotStore::getFileTime((*it), time);
            time_vec.push_back(time);
        }
        if (time_vec.size() > 0)
        {
            std::sort(time_vec.begin(), time_vec.end(), compareTime);
            last_snap_name = *(time_vec.begin());
        }
    }
    LOG_DEBUG(log, "check create snapshot, last_snap_name {}, curr_time {}", last_snap_name, curr_time);
    return timer.isActionTime(last_snap_name, curr_time);
}


void NuRaftStateMachine::create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done)
{
    create_snapshot(s);
    ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
}

void NuRaftStateMachine::create_snapshot(snapshot & s)
{
    LOG_INFO(log, "Create snapshot last_log_term {}, last_log_idx {}", s.get_last_log_term(), s.get_last_log_idx());
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        snap_mgr->createSnapshot(s, storage);
    }
}

void NuRaftStateMachine::save_snapshot_data(snapshot & s, const ulong offset, buffer & data)
{
    LOG_INFO(
        log,
        "Save snapshot data, snapshot last term {}, last index {}, offset {}, data size {}",
        s.get_last_log_term(),
        s.get_last_log_idx(),
        offset,
        data.size());
}

int NuRaftStateMachine::read_snapshot_data(snapshot & s, const ulong offset, buffer & data)
{
    LOG_INFO(
        log,
        "read snapshot data, snapshot last term {}, last index {}, offset {}, data size {}",
        s.get_last_log_term(),
        s.get_last_log_idx(),
        offset,
        data.size());
    return 0;
}

int NuRaftStateMachine::read_logical_snp_obj(snapshot & s, void *& user_snp_ctx, ulong obj_id, ptr<buffer> & data_out, bool & is_last_obj)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    // Snapshot doesn't exist.
    if (!snap_mgr->existSnapshot(s))
    {
        //snap_mgr->createSnapshot(s, node_map);
        data_out = nullptr;
        is_last_obj = true;
        LOG_INFO(log, "Cant find snapshot by last_log_idx {}, object id {}", s.get_last_log_idx(), obj_id);
        return 0;
    }

    if (obj_id == 0)
    {
        // Object ID == 0: first object
        data_out = buffer::alloc(sizeof(UInt32));
        buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = false;
        LOG_INFO(log, "Read snapshot object, last_log_idx {}, object id {}, is_last {}", s.get_last_log_idx(), obj_id, false);
        return 0;
    }

    // Object ID > 0: second object, put actual value.
    snap_mgr->loadSnapshotObject(s, obj_id, data_out);

    is_last_obj = !(snap_mgr->existSnapshotObject(s, obj_id + 1));

    LOG_INFO(log, "Read snapshot object, last_log_idx {}, object id {}, is_last {}", s.get_last_log_idx(), obj_id, is_last_obj);

    user_snp_ctx = nullptr;

    return 0;
}

void NuRaftStateMachine::save_logical_snp_obj(snapshot & s, ulong & obj_id, buffer & data, bool is_first_obj, bool is_last_obj)
{
    if (obj_id == 0)
    {
        // Object ID == 0: it contains dummy value, create snapshot context.
        snap_mgr->receiveSnapshot(s);
    }
    else
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        // Object ID > 0: actual snapshot value, save to local disk
        snap_mgr->saveSnapshotObject(s, obj_id, data);
    }
    LOG_INFO(log, "Save logical snapshot , object id {}, is_first_obj {}, is_last_obj {}", obj_id, is_first_obj, is_last_obj);
    // Request next object.
    obj_id++;
}

bool NuRaftStateMachine::exist_snapshot_object(snapshot & s, ulong obj_id)
{
    return snap_mgr->existSnapshotObject(s, obj_id);
}


bool NuRaftStateMachine::apply_snapshot(snapshot & s)
{
    //TODO: double buffer load or multi thread load
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    return snap_mgr->parseSnapshot(s, storage);
}

void NuRaftStateMachine::free_user_snp_ctx(void *& user_snp_ctx)
{
    // In this example, `read_logical_snp_obj` doesn't create
    // `user_snp_ctx`. Nothing to do in this function.
    if (user_snp_ctx != nullptr)
    {
        free(user_snp_ctx);
        user_snp_ctx = nullptr;
    }
}

ptr<snapshot> NuRaftStateMachine::last_snapshot()
{
    // Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    return snap_mgr->lastSnapshot();
}

bool NuRaftStateMachine::exists(const std::string & path)
{
    return (storage.container.count(path) == 1);
}

KeeperNode & NuRaftStateMachine::getNode(const std::string & path)
{
    auto node_ptr = storage.container.get(path);
    if (node_ptr != nullptr)
    {
        return *node_ptr.get();
    }
    return default_node;
}

/*
void KeeperStateMachine::registerCallback(const std::string & path, RaftWatchCallback * watch)
{
    ptr<DataNode> node = getNode(path);
    if (node != nullptr)
    {
        node->addWatch(watch);
    }
    else
    {
        LOG_WARNING(log, "Node {} is not exists.", path);
    }
}
*/

}

#ifdef __clang__
#    pragma clang diagnostic pop
#endif
