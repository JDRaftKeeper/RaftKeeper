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
#include <string>

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

#ifndef __cpp_char8_t
using char8_t = unsigned char;
#endif

/// This is needed for more strict aliasing. https://godbolt.org/z/xpJBSb https://stackoverflow.com/a/57453713
using UInt8 = char8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using String = std::string;

namespace RK
{

using UInt8 = ::UInt8;
using UInt16 = ::UInt16;
using UInt32 = ::UInt32;
using UInt64 = ::UInt64;

using Int8 = ::Int8;
using Int16 = ::Int16;
using Int32 = ::Int32;
using Int64 = ::Int64;

using Float32 = float;
using Float64 = double;

using String = std::string;

}
