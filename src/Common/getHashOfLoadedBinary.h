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
#include <Common/SipHash.h>

/** Calculate hash of the executable loaded segments of the first loaded object.
  * It can be used for integrity checks.
  * Does not work when ClickHouse is build as multiple shared libraries.
  * Note: we don't hash all loaded readonly segments, because some of them are modified by 'strip'
  *  and we want something that survives 'strip'.
  * Note: program behaviour can be affected not only by machine code but also by the data in these segments,
  * so the integrity check is going to be incomplete.
  */
SipHash getHashOfLoadedBinary();
std::string getHashOfLoadedBinaryHex();
