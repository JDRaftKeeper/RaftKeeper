
#include <Service/RequestsCommitEvent.h>

namespace DB
{

void RequestsCommitEvent::addRequest(int64_t session_id, int64_t xid)
{
    std::lock_guard lock(mutex);
    wait_commits.emplace(UInt128(session_id, xid), std::make_shared<Poco::Event>());
}

void RequestsCommitEvent::waitForCommit(int64_t session_id, int64_t xid)
{
    std::shared_ptr<Poco::Event> event;
    {
        std::lock_guard lock(mutex);

        auto it = wait_commits.find(UInt128(session_id, xid));

        /// Maybe wake up early
        if (it != wait_commits.end())
        {
            event = it->second;
        }
    }
    if (event)
        event->wait();
}

void RequestsCommitEvent::notifiy(int64_t session_id, int64_t xid)
{
    std::lock_guard lock(mutex);
    auto it = wait_commits.find(UInt128(session_id, xid));
    if (it != wait_commits.end())
    {
        it->second->set();
        wait_commits.erase(it);
    }
}

void RequestsCommitEvent::notifiyAll()
{
    std::lock_guard lock(mutex);

    for (auto it = wait_commits.begin(); it != wait_commits.end();)
    {
        it->second->set();
        it = wait_commits.erase(it);
    }
}

bool RequestsCommitEvent::hasNotified(int64_t session_id, int64_t xid) const
{
    std::lock_guard lock(mutex);
    auto it = wait_commits.find(UInt128(session_id, xid));
    return it == wait_commits.end();
}

void RequestsCommitEvent::erase(int64_t session_id, int64_t xid)
{
    std::lock_guard lock(mutex);
    wait_commits.erase(UInt128(session_id, xid));
}

}
