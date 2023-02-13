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
#include <string>


namespace RK
{

/// Returns true, if the following functions supported by the system
bool supportsRenameat2();

/// Atomically rename old_path to new_path. If new_path exists, do not overwrite it and throw exception
void renameNoReplace(const std::string & old_path, const std::string & new_path);

/// Atomically exchange oldpath and newpath. Throw exception if some of them does not exist
void renameExchange(const std::string & old_path, const std::string & new_path);

/// Returns false instead of throwing exception if renameat2 is not supported
bool renameExchangeIfSupported(const std::string & old_path, const std::string & new_path);

}
