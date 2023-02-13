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
#include <common/types.h>


/** Set color in terminal based on 64-bit hash value.
  * It can be used to choose some random color deterministically based on some other value.
  * Hash value should be uniformly distributed.
  */
std::string setColor(UInt64 hash);

/** Set color for logger priority value. */
const char * setColorForLogPriority(int priority);

/** Undo changes made by the functions above. */
const char * resetColor();
