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
#include <Common/StatusInfo.h>

/// Available status. Add something here as you wish.
#define APPLY_FOR_STATUS(M) \
    M(DictionaryStatus, "Dictionary Status.", RK::getStatusEnumAllPossibleValues()) \


namespace CurrentStatusInfo
{
    #define M(NAME, DOCUMENTATION, ENUM) extern const Status NAME = __COUNTER__;
        APPLY_FOR_STATUS(M)
    #undef M
    constexpr Status END = __COUNTER__;

    std::mutex locks[END] {};
    std::unordered_map<String, Int8> values[END] {};

    const char * getName(Status event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION, ENUM) #NAME,
            APPLY_FOR_STATUS(M)
        #undef M
        };

        return strings[event];
    }

    const char * getDocumentation(Status event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION, ENUM) #DOCUMENTATION,
            APPLY_FOR_STATUS(M)
        #undef M
        };

        return strings[event];
    }

    const std::vector<std::pair<String, Int8>> & getAllPossibleValues(Status event)
    {
        static const std::vector<std::pair<String, Int8>> enum_values [] =
        {
        };
        return enum_values[event];
    }

    Status end() { return END; }
}

#undef APPLY_FOR_STATUS
