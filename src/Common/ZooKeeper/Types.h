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
#include <future>
#include <memory>
#include <vector>
#include <Common/ZooKeeper/IKeeper.h>
#include <Poco/Event.h>


namespace zkutil
{

using Strings = std::vector<std::string>;


namespace CreateMode
{
    extern const int Persistent;
    extern const int Ephemeral;
    extern const int EphemeralSequential;
    extern const int PersistentSequential;
}

using EventPtr = std::shared_ptr<Poco::Event>;

/// Gets multiple asynchronous results
/// Each pair, the first is path, the second is response eg. CreateResponse, RemoveResponse
template <typename R>
using AsyncResponses = std::vector<std::pair<std::string, std::future<R>>>;

Coordination::RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode);
Coordination::RequestPtr makeRemoveRequest(const std::string & path, int version);
Coordination::RequestPtr makeSetRequest(const std::string & path, const std::string & data, int version);
Coordination::RequestPtr makeCheckRequest(const std::string & path, int version);

}
