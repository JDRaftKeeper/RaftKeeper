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

#include <common/types.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace RK
{
    /// Returns true if two configurations contains the same keys and values.
    bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left,
                             const Poco::Util::AbstractConfiguration & right);

    /// Returns true if specified subviews of the two configurations contains the same keys and values.
    bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const String & left_key,
                             const Poco::Util::AbstractConfiguration & right, const String & right_key);

    inline bool operator==(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right)
    {
        return isSameConfiguration(left, right);
    }

    inline bool operator!=(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right)
    {
        return !isSameConfiguration(left, right);
    }
}
