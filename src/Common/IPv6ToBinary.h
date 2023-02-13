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
#include <array>
#include <common/types.h>

namespace Poco { namespace Net { class IPAddress; }}

namespace RK
{

/// Convert IP address to raw binary with IPv6 data (big endian). If it's an IPv4, map it to IPv6.
/// Saves result into the first 16 bytes of `res`.
void IPv6ToRawBinary(const Poco::Net::IPAddress & address, char * res);

/// Convert IP address to 16-byte array with IPv6 data (big endian). If it's an IPv4, map it to IPv6.
std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address);

/// Returns pointer to 16-byte array containing mask with first `prefix_len` bits set to `1` and `128 - prefix_len` to `0`.
/// Pointer is valid during all program execution time and doesn't require freeing.
/// Values of prefix_len greater than 128 interpreted as 128 exactly.
const uint8_t * getCIDRMaskIPv6(UInt8 prefix_len);

}
