#pragma once

#include <unordered_map>
#include <Common/UInt128.h>
#include <Poco/Event.h>

namespace DB
{

class RequestsCommitEvent
{
private:
    mutable std::mutex mutex;

    std::unordered_map<UInt128, std::shared_ptr<Poco::Event>> wait_commits;
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<Poco::Event>>> session_xid_events;

public:
    void addRequest(int64_t session_id, int64_t xid);

    void waitForCommit(int64_t session_id, int64_t xid);

    void waitForCommit(int64_t session_id);

    void notifiy(int64_t session_id, int64_t xid);

    void notifiyAll();

    bool hasNotified(int64_t session_id, int64_t xid) const;

    bool hasNotified(int64_t session_id) const;

    void erase(int64_t session_id, int64_t xid);
};

}
