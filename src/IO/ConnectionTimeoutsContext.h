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

#include <IO/ConnectionTimeouts.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

namespace RK
{

/// Timeouts for the case when we have just single attempt to connect.
inline ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithoutFailover(const Settings & settings)
{
    return ConnectionTimeouts(settings.connect_timeout, settings.send_timeout, settings.receive_timeout, settings.tcp_keep_alive_timeout);
}

/// Timeouts for the case when we will try many addresses in a loop.
inline ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithFailover(const Settings & settings)
{
    return ConnectionTimeouts(
        settings.connect_timeout_with_failover_ms,
        settings.send_timeout,
        settings.receive_timeout,
        settings.tcp_keep_alive_timeout,
        0,
        settings.connect_timeout_with_failover_secure_ms,
        settings.hedged_connection_timeout_ms,
        settings.receive_data_timeout_ms);
}

inline ConnectionTimeouts ConnectionTimeouts::getHTTPTimeouts(const Context & context)
{
    const auto & settings = context.getSettingsRef();
    const auto & config = context.getConfigRef();
    Poco::Timespan http_keep_alive_timeout{config.getUInt("keep_alive_timeout", 10), 0};
    return ConnectionTimeouts(settings.http_connection_timeout, settings.http_send_timeout, settings.http_receive_timeout, settings.tcp_keep_alive_timeout, http_keep_alive_timeout);
}

}
