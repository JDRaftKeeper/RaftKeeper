
#include <Service/RequestsCommitEvent.h>

namespace DB
{

void RequestsCommitEvent::addRequest(int64_t session_id, int64_t xid)
{
    std::lock_guard lock(mutex);
//    wait_commits.emplace(UInt128(session_id, xid), std::make_shared<Poco::Event>());
    if (!session_xid_events.contains(session_id))
    {
        session_xid_events[session_id] = std::unordered_map<int64_t, std::shared_ptr<Poco::Event>>();
    }
    session_xid_events[session_id].emplace(xid, std::make_shared<Poco::Event>());
}

void RequestsCommitEvent::waitForCommit(int64_t session_id, int64_t xid)
{
    std::shared_ptr<Poco::Event> event;
    {
        std::lock_guard lock(mutex);

        auto session_it = session_xid_events.find(session_id);

        if (session_it != session_xid_events.end())
        {
            auto & xid_events = session_it->second;
            auto xid_it = xid_events.find(xid);

            /// Maybe wake up early
            if (xid_it != xid_events.end())
                event = xid_it->second;
        }

//        auto it = wait_commits.find(UInt128(session_id, xid));
//
//        /// Maybe wake up early
//        if (it != wait_commits.end())
//        {
//            event = it->second;
//        }
    }
    if (event)
        event->wait();
}


void RequestsCommitEvent::waitForCommit(int64_t session_id)
{
    std::vector<std::shared_ptr<Poco::Event>> events;
    {
        std::lock_guard lock(mutex);

        auto session_it = session_xid_events.find(session_id);

        if (session_it != session_xid_events.end())
        {
            auto & xid_events = session_it->second;
            for (auto xid_it = xid_events.begin(); xid_it != xid_events.end(); ++xid_it)
            {
                events.emplace_back(xid_it->second);
            }
        }
    }
    for (auto & event : events)
    {
        event->wait();
    }
}

void RequestsCommitEvent::notifiy(int64_t session_id, int64_t xid)
{
    std::lock_guard lock(mutex);
//    auto it = wait_commits.find(UInt128(session_id, xid));
//    if (it != wait_commits.end())
//    {
//        it->second->set();
//        wait_commits.erase(it);
//    }

    auto session_it = session_xid_events.find(session_id);

    if (session_it != session_xid_events.end())
    {
        auto & xid_events = session_it->second;
        auto xid_it = xid_events.find(xid);

        if (xid_it != xid_events.end())
        {
            xid_it->second->set();
            xid_events.erase(xid_it);
        }

        if (xid_events.empty())
        {
            session_xid_events.erase(session_it);
        }
    }

}

void RequestsCommitEvent::notifiyAll()
{
    std::lock_guard lock(mutex);

//    for (auto it = wait_commits.begin(); it != wait_commits.end();)
//    {
//        it->second->set();
//        it = wait_commits.erase(it);
//    }

    for (auto session_it = session_xid_events.begin(); session_it != session_xid_events.end();)
    {
        auto & xid_events = session_it->second;
        for (auto xid_it = xid_events.begin(); xid_it != xid_events.end();)
        {
            xid_it->second->set();
            xid_it = xid_events.erase(xid_it);
        }

        session_it = session_xid_events.erase(session_it);
    }
}

bool RequestsCommitEvent::hasNotified(int64_t session_id, int64_t xid) const
{
    std::lock_guard lock(mutex);
//    auto it = wait_commits.find(UInt128(session_id, xid));
//    return it == wait_commits.end();
    auto session_it = session_xid_events.find(session_id);
    if (session_it == session_xid_events.end())
        return true;

    auto & xid_events = session_it->second;
    auto xid_it = xid_events.find(xid);

    if (xid_it == xid_events.end())
        return true;

    return false;
}


bool RequestsCommitEvent::hasNotified(int64_t session_id) const
{
    std::lock_guard lock(mutex);
    //    auto it = wait_commits.find(UInt128(session_id, xid));
    //    return it == wait_commits.end();
    auto session_it = session_xid_events.find(session_id);
    if (session_it == session_xid_events.end())
        return true;

    auto & xid_events = session_it->second;

    if (xid_events.empty())
        return true;

    return false;
}

void RequestsCommitEvent::erase(int64_t session_id, int64_t xid)
{
    std::lock_guard lock(mutex);
//    wait_commits.erase(UInt128(session_id, xid));
    auto session_it = session_xid_events.find(session_id);

    if (session_it != session_xid_events.end())
    {
        auto & xid_events = session_it->second;
        auto xid_it = xid_events.find(xid);

        if (xid_it != xid_events.end())
        {
            xid_events.erase(xid_it);
        }

        if (xid_events.empty())
        {
            session_xid_events.erase(session_it);
        }
    }
}

}
