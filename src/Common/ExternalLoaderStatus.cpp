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
#include <Common/ExternalLoaderStatus.h>

namespace RK
{
    String toString(ExternalLoaderStatus status)
    {
        using Status = ExternalLoaderStatus;
        switch (status)
        {
            case Status::NOT_LOADED: return "NOT_LOADED";
            case Status::LOADED: return "LOADED";
            case Status::FAILED: return "FAILED";
            case Status::LOADING: return "LOADING";
            case Status::FAILED_AND_RELOADING: return "FAILED_AND_RELOADING";
            case Status::LOADED_AND_RELOADING: return "LOADED_AND_RELOADING";
            case Status::NOT_EXIST: return "NOT_EXIST";
        }
    __builtin_unreachable();
    }

    std::vector<std::pair<String, Int8>> getStatusEnumAllPossibleValues()
    {
        using Status = ExternalLoaderStatus;
        return std::vector<std::pair<String, Int8>>{
            {toString(Status::NOT_LOADED), static_cast<Int8>(Status::NOT_LOADED)},
            {toString(Status::LOADED), static_cast<Int8>(Status::LOADED)},
            {toString(Status::FAILED), static_cast<Int8>(Status::FAILED)},
            {toString(Status::LOADING), static_cast<Int8>(Status::LOADING)},
            {toString(Status::LOADED_AND_RELOADING), static_cast<Int8>(Status::LOADED_AND_RELOADING)},
            {toString(Status::FAILED_AND_RELOADING), static_cast<Int8>(Status::FAILED_AND_RELOADING)},
            {toString(Status::NOT_EXIST), static_cast<Int8>(Status::NOT_EXIST)},
        };
    }

    std::ostream & operator<<(std::ostream & out, ExternalLoaderStatus status)
    {
        return out << toString(status);
    }
}
