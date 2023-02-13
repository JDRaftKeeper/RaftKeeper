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
#pragma once

#include <common/defines.h>
#include <common/StringRef.h>
#include <Common/HashTable/StringHashSet.h>
#include <Common/Arena.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <mutex>
#include <string>
#include <unordered_map>

namespace RK
{

/// Custom TLD List
///
/// Unlike tldLookup (which uses gperf) this one uses plain StringHashSet.
class TLDList
{
public:
    using Container = StringHashSet<>;

    TLDList(size_t size);

    /// Return true if the tld_container does not contains such element.
    bool insert(const StringRef & host);
    /// Check is there such TLD
    bool has(const StringRef & host) const;
    size_t size() const { return tld_container.size(); }

private:
    Container tld_container;
    std::unique_ptr<Arena> pool;
};

class TLDListsHolder
{
public:
    using Map = std::unordered_map<std::string, TLDList>;

    static TLDListsHolder & getInstance();

    /// Parse "top_level_domains_lists" section,
    /// And add each found dictionary.
    void parseConfig(const std::string & top_level_domains_path, const Poco::Util::AbstractConfiguration & config);

    /// Parse file and add it as a Set to the list of TLDs
    /// - "//" -- comment,
    /// - empty lines will be ignored.
    ///
    /// Example: https://publicsuffix.org/list/public_suffix_list.dat
    ///
    /// Return size of the list.
    size_t parseAndAddTldList(const std::string & name, const std::string & path);
    /// Throws TLD_LIST_NOT_FOUND if list does not exist
    const TLDList & getTldList(const std::string & name);

protected:
    TLDListsHolder();

    std::mutex tld_lists_map_mutex;
    Map tld_lists_map;
};

}
