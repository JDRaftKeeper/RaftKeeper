/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <Service/ACLMap.h>
#include <Common/SipHash.h>

namespace RK
{

template<class Key, class Val>
bool mapEquals(const std::unordered_map<Key, Val> & l, const std::unordered_map<Key, Val> & r)
{
    if (l.size() != r.size())
        return false;

    for(const auto & it : l)
    {
        if(!r.contains(it.first))
            return false;
        if(it.second != r.at(it.first))
            return false;
    }
    return true;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

size_t ACLMap::ACLsHash::operator()(const Coordination::ACLs & acls) const
{
    SipHash hash;
    for (const auto & acl : acls)
    {
        hash.update(acl.permissions);
        hash.update(acl.scheme);
        hash.update(acl.id);
    }
    return hash.get64();
}

bool ACLMap::ACLsComparator::operator()(const Coordination::ACLs & left, const Coordination::ACLs & right) const
{
    if (left.size() != right.size())
        return false;

    for (size_t i = 0; i < left.size(); ++i)
    {
        if (left[i].permissions != right[i].permissions)
            return false;

        if (left[i].scheme != right[i].scheme)
            return false;

        if (left[i].id != right[i].id)
            return false;
    }
    return true;
}

uint64_t ACLMap::convertACLs(const Coordination::ACLs & acls)
{
    std::lock_guard lock(acl_mutex);

    if (acls.empty() || acls == Coordination::ACLs{Coordination::ACL{Coordination::ACL::All, "world", "anyone"}})
        return 0;

    if (acl_to_num.count(acls))
        return acl_to_num[acls];

    /// Start from one
    auto index = max_acl_id++;

    acl_to_num[acls] = index;
    num_to_acl[index] = acls;

    return index;
}

Coordination::ACLs ACLMap::convertNumber(uint64_t acls_id) const
{
    std::lock_guard lock(acl_mutex);

    if (acls_id == 0) /// default acl is 'world,'anyone : cdrwa
    {
        return Coordination::ACLs{Coordination::ACL{Coordination::ACL::All, "world", "anyone"}};
    }

    if (!num_to_acl.count(acls_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ACL id {}. It's a bug", acls_id);

    return num_to_acl.at(acls_id);
}

void ACLMap::addMapping(uint64_t acls_id, const Coordination::ACLs & acls)
{
    std::lock_guard lock(acl_mutex);
    num_to_acl[acls_id] = acls;
    acl_to_num[acls] = acls_id;
    max_acl_id = std::max(acls_id + 1, max_acl_id); /// max_acl_id pointer next slot
}

void ACLMap::addUsage(uint64_t acl_id, uint64_t count)
{
    std::lock_guard lock(acl_mutex);
    if (acl_id == 0)
        return;
    usage_counter[acl_id] += count;
}

void ACLMap::removeUsage(uint64_t acl_id)
{
    std::lock_guard lock(acl_mutex);
    if (usage_counter.count(acl_id) == 0)
        return;

    usage_counter[acl_id]--;

    if (usage_counter[acl_id] == 0)
    {
        auto acls = num_to_acl[acl_id];
        num_to_acl.erase(acl_id);
        acl_to_num.erase(acls);
        usage_counter.erase(acl_id);
    }
}
bool ACLMap::operator==(const ACLMap & rhs) const
{

    if (acl_to_num.size() != rhs.acl_to_num.size())
        return false;

    for(const auto & it : acl_to_num)
    {
        if(!rhs.acl_to_num.contains(it.first))
            return false;
        if(it.second != rhs.acl_to_num.at(it.first))
            return false;
    }

    return mapEquals(num_to_acl, rhs.num_to_acl) && mapEquals(usage_counter, rhs.usage_counter) && max_acl_id == rhs.max_acl_id;
}

bool ACLMap::operator!=(const ACLMap & rhs) const
{
    return !(rhs == *this);
}

}
