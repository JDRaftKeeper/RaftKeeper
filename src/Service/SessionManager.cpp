#include <Service/SessionManager.h>

#include <common/logger_useful.h>

namespace RK
{

int64_t SessionManager::getSessionID(int64_t session_timeout_ms)
{
    std::lock_guard lock(session_mutex);
    auto new_id = session_id_counter++;
    auto it = session_and_timeout.emplace(new_id, session_timeout_ms);
    if (!it.second)
    {
        LOG_DEBUG(log, "Session {} already exist, must applying a fuzzy log.", toHexString(new_id));
    }
    LOG_DEBUG(log, "New session {} created.", toHexString(new_id));
    session_expiry_queue.addNewSessionOrUpdate(new_id, session_timeout_ms);
    return new_id;
}

bool SessionManager::updateSessionTimeout(int64_t session_id, int64_t /*session_timeout_ms*/)
{
    std::lock_guard lock(session_mutex);
    if (!session_and_timeout.contains(session_id))
    {
        LOG_WARNING(log, "Updating session timeout for {}, but it is already expired.", toHexString(session_id));
        return false;
    }
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);
    LOG_INFO(log, "Updated session timeout for {}", toHexString(session_id));
    return true;
}

void SessionManager::reset()
{
    std::lock_guard lock(session_mutex);
    session_id_counter = 1;
    session_and_timeout.clear();
    session_expiry_queue.clear();
}

}
