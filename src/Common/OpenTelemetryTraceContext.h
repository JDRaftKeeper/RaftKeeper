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

namespace RK
{

// The runtime info we need to create new OpenTelemetry spans.
struct OpenTelemetryTraceContext
{
    __uint128_t trace_id = 0;
    UInt64 span_id = 0;
    // The incoming tracestate header and the trace flags, we just pass them
    // downstream. See https://www.w3.org/TR/trace-context/
    String tracestate;
    __uint8_t trace_flags = 0;

    // Parse/compose OpenTelemetry traceparent header.
    bool parseTraceparentHeader(const std::string & traceparent, std::string & error);
    std::string composeTraceparentHeader() const;
};

}
