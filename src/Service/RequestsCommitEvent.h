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

public:
    void addRequest(int64_t session_id, int64_t xid);

    void waitForCommit(int64_t session_id, int64_t xid);

    void notifiy(int64_t session_id, int64_t xid);

    void notifiyAll();

    bool hasNotified(int64_t session_id, int64_t xid) const;

    void erase(int64_t session_id, int64_t xid);
};

}
