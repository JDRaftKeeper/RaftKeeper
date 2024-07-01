#include <Common/IO/Operators.h>
#include <Common/IO/WriteBufferFromString.h>
#include <common/logger_useful.h>

#include <Service/KeeperUtils.h>
#include <Service/WatchManager.h>

namespace RK
{

void WatchManager::registerWatches(const String & path, int64_t session_id, Coordination::OpNum opnum)
{
    std::lock_guard lock(watch_mutex);
    auto watches_type = opnum == Coordination::OpNum::List
            || opnum == Coordination::OpNum::SimpleList
            || opnum == Coordination::OpNum::FilteredList ? WatchType::List : WatchType::Data;

    switch (watches_type)
    {
        case WatchType::Data:
            watches[path].emplace(session_id);
            break;
        case WatchType::List:
            list_watches[path].emplace(session_id);
            break;
    }

    sessions_and_watchers[session_id][path] |= static_cast<uint8_t>(watches_type);

    LOG_TRACE(log, "Register watch path={}, session_id={}, data={}", path, toHexString(session_id), toString(sessions_and_watchers[session_id][path]));
}

ResponsesForSessions WatchManager::processWatches(const String & path, Coordination::OpNum opnum)
{
    switch (opnum)
    {
        case Coordination::OpNum::Create:
            return processWatches(path, Coordination::Event::CREATED);
        case Coordination::OpNum::Remove:
            return processWatches(path, Coordination::Event::DELETED);
        case Coordination::OpNum::Set:
            return processWatches(path, Coordination::Event::CHANGED);
        default:
            return {};
    }
}

ResponsesForSessions WatchManager::processWatches(const String & path, Coordination::Event event_type)
{
    std::lock_guard lock(watch_mutex);

    ResponsesForSessions result;
    auto it = watches.find(path);
    if (it != watches.end())
    {
        std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        watch_response->path = path;
        watch_response->xid = Coordination::WATCH_XID;
        watch_response->zxid = -1;
        watch_response->type = event_type;
        watch_response->state = Coordination::State::CONNECTED;
        for (auto watcher_session : it->second)
        {
            result.push_back(ResponseForSession{watcher_session, watch_response});
            LOG_TRACE(log, "Unregister watch for path={}, session_id={}, data={}", path, toHexString(watcher_session), toString(sessions_and_watchers[watcher_session][path]));
            if ((sessions_and_watchers[watcher_session][path] ^= static_cast<uint8_t>(WatchType::Data)) == 0)
            {
                sessions_and_watchers[watcher_session].erase(path);
                LOG_TRACE(log, "Unregister sessions_and_watchers path={}, session_id={}", path, toHexString(watcher_session));
            }
        }
        watches.erase(it);

    }

    auto parent_path = getParentPath(path);

    Strings paths_to_check_for_list_watches;
    if (event_type == Coordination::Event::CREATED)
    {
        paths_to_check_for_list_watches.push_back(parent_path); /// Trigger list watches for parent
    }
    else if (event_type == Coordination::Event::DELETED)
    {
        paths_to_check_for_list_watches.push_back(path); /// Trigger both list watches for this path
        paths_to_check_for_list_watches.push_back(parent_path); /// And for parent path
    }
    /// CHANGED event never trigger list wathes

    for (const auto & path_to_check : paths_to_check_for_list_watches)
    {
        it = list_watches.find(path_to_check);
        if (it != list_watches.end())
        {
            std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_list_response
                = std::make_shared<Coordination::ZooKeeperWatchResponse>();
            watch_list_response->path = path_to_check;
            watch_list_response->xid = Coordination::WATCH_XID;
            watch_list_response->zxid = -1;
            if (path_to_check == parent_path)
                watch_list_response->type = Coordination::Event::CHILD;
            else
                watch_list_response->type = Coordination::Event::DELETED;

            watch_list_response->state = Coordination::State::CONNECTED;
            for (auto watcher_session : it->second)
            {
                result.push_back(ResponseForSession{watcher_session, watch_list_response});
                LOG_TRACE(log, "Unregister watch forlistwatch path={}, session_id={}, data={}", path_to_check, toHexString(watcher_session), toString(sessions_and_watchers[watcher_session][path_to_check]));
                if ((sessions_and_watchers[watcher_session][path_to_check] ^= static_cast<uint8_t>(WatchType::List)) == 0)
                {
                    sessions_and_watchers[watcher_session].erase(path_to_check);
                    LOG_TRACE(log, "Unregister sessions_and_watchers path={}, session_id={}", path_to_check, toHexString(watcher_session));
                }
            }

            list_watches.erase(it);
        }
    }
    return result;
}


ResponsesForSessions WatchManager::processRequestSetWatch(
    const RequestForSession & request_for_session, std::unordered_map<String, std::pair<int64_t, int64_t>> & watch_nodes_info)
{
    ResponsesForSessions responses;

    auto * request = dynamic_cast<Coordination::ZooKeeperSetWatchesRequest *>(request_for_session.request.get());
    auto session_id = request_for_session.session_id;

    std::lock_guard lock(watch_mutex);
    for (String & path : request->data_watches)
    {
        LOG_TRACE(log, "Register data_watches for session {}, path {}, xid", toHexString(session_id), path, request->xid);
        /// register watches
        watches[path].emplace(session_id);
        sessions_and_watchers[session_id][path] |= static_cast<uint8_t>(WatchType::Data);

        /// trigger watches
        if (!watch_nodes_info.contains(path))
        {
            LOG_TRACE(
                log, "Trigger data_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
            auto watch_responses = processWatches(path, Coordination::Event::DELETED);
            responses.insert(responses.end(), watch_responses.begin(), watch_responses.end());
        }
        else if (watch_nodes_info[path].first > request->relative_zxid)
        {
            LOG_TRACE(
                log, "Trigger data_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
            auto watch_responses = processWatches(path, Coordination::Event::CHANGED);
            responses.insert(responses.end(), watch_responses.begin(), watch_responses.end());
        }
    }

    for (String & path : request->exist_watches)
    {
        LOG_TRACE(log, "Register exist_watches for session {}, path {}, xid", toHexString(session_id), path, request->xid);
        /// register watches
        watches[path].emplace(session_id);
        sessions_and_watchers[session_id][path] |= static_cast<uint8_t>(WatchType::Data);

        /// trigger watches
        if (watch_nodes_info.contains(path))
        {
            LOG_TRACE(
                log, "Trigger exist_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
            auto watch_responses = processWatches(path, Coordination::Event::CREATED);
            responses.insert(responses.end(), watch_responses.begin(), watch_responses.end());
        }
    }

    for (String & path : request->list_watches)
    {
        LOG_TRACE(log, "Register list_watches for session {}, path {}, xid", toHexString(session_id), path, request->xid);
        /// register watches
        list_watches[path].emplace(session_id);
        sessions_and_watchers[session_id][path] |= static_cast<uint8_t>(WatchType::List);

        /// trigger watches
        if (!watch_nodes_info.contains(path))
        {
            LOG_TRACE(
                log, "Trigger list_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
            auto watch_responses = processWatches(path, Coordination::Event::DELETED);
            responses.insert(responses.end(), watch_responses.begin(), watch_responses.end());
        }
        else if (watch_nodes_info[path].second > request->relative_zxid)
        {
            LOG_TRACE(
                log, "Trigger list_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
            auto watch_responses = processWatches(path, Coordination::Event::CHILD);
            responses.insert(responses.end(), watch_responses.begin(), watch_responses.end());
        }
    }

    return responses;
}

void WatchManager::cleanDeadWatches(int64_t session_id)
{
    LOG_DEBUG(log, "Clean dead watches for session {}", toHexString(session_id));

    std::lock_guard watch_lock(watch_mutex);
    auto watches_it = sessions_and_watchers.find(session_id);

    if (watches_it != sessions_and_watchers.end())
    {
        for (const auto & [watch_path, _] : watches_it->second)
        {
            auto watch = watches.find(watch_path);
            if (watch != watches.end())
            {
                auto & watches_for_path = watch->second;
                for (auto w_it = watches_for_path.begin(); w_it != watches_for_path.end();)
                {
                    if (*w_it == session_id)
                        w_it = watches_for_path.erase(w_it);
                    else
                        ++w_it;
                }
                if (watches_for_path.empty())
                    watches.erase(watch);
            }

            auto list_watch = list_watches.find(watch_path);
            if (list_watch != list_watches.end())
            {
                auto & list_watches_for_path = list_watch->second;
                for (auto w_it = list_watches_for_path.begin(); w_it != list_watches_for_path.end();)
                {
                    if (*w_it == session_id)
                        w_it = list_watches_for_path.erase(w_it);
                    else
                        ++w_it;
                }
                if (list_watches_for_path.empty())
                    list_watches.erase(list_watch);
            }
        }
        sessions_and_watchers.erase(watches_it);
    }
}

uint64_t WatchManager::getTotalWatchesCount() const
{
    std::lock_guard lock(watch_mutex);
    uint64_t ret = 0;
    for (const auto & [path, subscribed_sessions] : watches)
        ret += subscribed_sessions.size();

    for (const auto & [path, subscribed_sessions] : list_watches)
        ret += subscribed_sessions.size();

    return ret;
}

uint64_t WatchManager::getSessionsWithWatchesCount() const
{
    std::lock_guard lock(watch_mutex);
    std::unordered_set<int64_t> counter;
    for (const auto & [path, subscribed_sessions] : watches)
        counter.insert(subscribed_sessions.begin(), subscribed_sessions.end());

    for (const auto & [path, subscribed_sessions] : list_watches)
        counter.insert(subscribed_sessions.begin(), subscribed_sessions.end());

    return counter.size();
}

void WatchManager::dumpWatches(WriteBufferFromOwnString & buf) const
{
    std::lock_guard lock(watch_mutex);
    for (const auto & [session_id, watches_paths] : sessions_and_watchers)
    {
        buf << toHexString(session_id) << "\n";
        for (const auto & [path, _] : watches_paths)
            buf << "\t" << path << "\n";
    }
}

void WatchManager::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    auto write_int_vec = [&buf](const std::unordered_set<int64_t> & session_ids)
    {
        for (int64_t session_id : session_ids)
        {
            buf << "\t" << toHexString(session_id) << "\n";
        }
    };

    std::lock_guard lock(watch_mutex);
    for (const auto & [watch_path, sessions] : watches)
    {
        buf << watch_path << "\n";
        write_int_vec(sessions);
    }

    for (const auto & [watch_path, sessions] : list_watches)
    {
        buf << watch_path << "\n";
        write_int_vec(sessions);
    }
}

void WatchManager::reset()
{
    std::lock_guard lock(watch_mutex);
    watches.clear();
    list_watches.clear();
    sessions_and_watchers.clear();
}

}
