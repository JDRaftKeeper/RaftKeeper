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
#include <common/StringRef.h>


namespace RK
{
/// Quote the string.
String quoteString(const StringRef & x);

/// Double quote the string.
String doubleQuoteString(const StringRef & x);

/// Quote the identifier with backquotes.
String backQuote(const StringRef & x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(const StringRef & x);
}
