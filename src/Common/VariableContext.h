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

/// Used in ProfileEvents and MemoryTracker to determine their hierarchy level
/// The less value the higher level (zero level is the root)
enum class VariableContext
{
    Global = 0,
    User,           /// Group of processes
    Process,        /// For example, a query or a merge
    Thread,         /// A thread of a process
    Snapshot        /// Does not belong to anybody
};
