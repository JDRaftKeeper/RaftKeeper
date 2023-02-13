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

#include <Common/ZooKeeper/Types.h>


namespace zkutil
{


using KeeperException = Coordination::Exception;


class KeeperMultiException : public KeeperException
{
public:
    Coordination::Requests requests;
    Coordination::Responses responses;
    size_t failed_op_index = 0;

    std::string getPathForFirstFailedOp() const;

    /// If it is user error throws KeeperMultiException else throws ordinary KeeperException
    /// If it is ZOK does nothing
    static void check(Coordination::Error code, const Coordination::Requests & requests, const Coordination::Responses & responses);

    KeeperMultiException(Coordination::Error code, const Coordination::Requests & requests, const Coordination::Responses & responses);

private:
    static size_t getFailedOpIndex(Coordination::Error code, const Coordination::Responses & responses);
};

}
