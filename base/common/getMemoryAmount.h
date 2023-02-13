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

#include <cstdint>

/** Returns the size of physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform or if it cannot determine the size of physical memory.
  */
uint64_t getMemoryAmountOrZero();

/** Throws exception if it cannot determine the size of physical memory.
  */
uint64_t getMemoryAmount();
