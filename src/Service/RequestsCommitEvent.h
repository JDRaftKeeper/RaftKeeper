#pragma once

#include <unordered_map>
#include <Common/UInt128.h>
#include <Poco/Event.h>
#include <libnuraft/nuraft.hxx>

namespace DB
{

class RequestsCommitEvent
{
private:
    mutable std::mutex mutex;

    std::unordered_map<UInt128, std::shared_ptr<Poco::Event>> wait_commits;
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<Poco::Event>>> session_xid_events;

    mutable std::mutex errors_mutex;
    std::unordered_map<UInt128, std::pair<bool, nuraft::cmd_result_code>> errors;

public:
    void addRequest(int64_t session_id, int64_t xid);

    void waitForCommit(int64_t session_id, int64_t xid);

    void waitForCommit(int64_t session_id);

    void notifiy(int64_t session_id, int64_t xid);

    void notifiyAll();

    bool hasNotified(int64_t session_id, int64_t xid) const;

    bool hasNotified(int64_t session_id) const;

    bool exist(int64_t session_id, int64_t xid) const;

    void erase(int64_t session_id, int64_t xid);

    void addError(int64_t session_id, int64_t xid, bool accepted, nuraft::cmd_result_code error_code);

    void eraseError(int64_t session_id, int64_t xid);

    bool isError(int64_t session_id, int64_t xid);

    std::pair<bool, nuraft::cmd_result_code> getError(int64_t session_id, int64_t xid);
};

}
