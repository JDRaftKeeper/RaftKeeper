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

#include <vector>
#include <utility>
#include <ostream>
#include <common/types.h>

namespace RK
{
    enum class ExternalLoaderStatus
    {
        NOT_LOADED, /// Object hasn't been tried to load. This is an initial state.
        LOADED, /// Object has been loaded successfully.
        FAILED, /// Object has been failed to load.
        LOADING, /// Object is being loaded right now for the first time.
        FAILED_AND_RELOADING, /// Object was failed to load before and it's being reloaded right now.
        LOADED_AND_RELOADING, /// Object was loaded successfully before and it's being reloaded right now.
        NOT_EXIST, /// Object with this name wasn't found in the configuration.
    };

    String toString(ExternalLoaderStatus status);
    std::vector<std::pair<String, Int8>> getStatusEnumAllPossibleValues();
    std::ostream & operator<<(std::ostream & out, ExternalLoaderStatus status);
}
