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
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace RK
{
String quoteString(const StringRef & x)
{
    String res(x.size, '\0');
    WriteBufferFromString wb(res);
    writeQuotedString(x, wb);
    return res;
}


String doubleQuoteString(const StringRef & x)
{
    String res(x.size, '\0');
    WriteBufferFromString wb(res);
    writeDoubleQuotedString(x, wb);
    return res;
}


String backQuote(const StringRef & x)
{
    String res(x.size, '\0');
    {
        WriteBufferFromString wb(res);
        writeBackQuotedString(x, wb);
    }
    return res;
}


String backQuoteIfNeed(const StringRef & x)
{
    String res(x.size, '\0');
    {
        WriteBufferFromString wb(res);
        writeProbablyBackQuotedString(x, wb);
    }
    return res;
}
}
