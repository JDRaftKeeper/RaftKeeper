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
#include "getNumberOfPhysicalCPUCores.h"

#include <thread>


unsigned getNumberOfPhysicalCPUCores()
{
    static const unsigned number = []
    {
        /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
        /// (Actually, only Aarch64 is supported).
        return std::thread::hardware_concurrency();
    }();
    return number;
}
