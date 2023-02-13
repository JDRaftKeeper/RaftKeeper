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
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <common/types.h>
#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>


namespace RK
{

/// A singleton implementing DNS names resolving with optional DNS cache
/// The cache is being updated asynchronous in separate thread (see DNSCacheUpdater)
/// or it could be updated manually via drop() method.
class DNSResolver : private boost::noncopyable
{
public:
    typedef std::vector<Poco::Net::IPAddress> IPAddresses;

    static DNSResolver & instance();

    DNSResolver(const DNSResolver &) = delete;

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolves its IP
    Poco::Net::IPAddress resolveHost(const std::string & host);

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolves all its IPs
    IPAddresses resolveHostAll(const std::string & host);

    /// Accepts host names like 'example.com:port' or '127.0.0.1:port' or '[::1]:port' and resolves its IP and port
    Poco::Net::SocketAddress resolveAddress(const std::string & host_and_port);

    Poco::Net::SocketAddress resolveAddress(const std::string & host, UInt16 port);

    /// Accepts host IP and resolves its host name
    String reverseResolve(const Poco::Net::IPAddress & address);

    /// Get this server host name
    String getHostName();

    /// Disables caching
    void setDisableCacheFlag(bool is_disabled = true);

    /// Drops all caches
    void dropCache();

    /// Updates all known hosts in cache.
    /// Returns true if IP of any host has been changed.
    bool updateCache();

    ~DNSResolver();

private:
    template<typename UpdateF, typename ElemsT>
    bool updateCacheImpl(UpdateF && update_func, ElemsT && elems, const String & log_msg);

    DNSResolver();

    struct Impl;
    std::unique_ptr<Impl> impl;
    Poco::Logger * log;

    /// Updates cached value and returns true it has been changed.
    bool updateHost(const String & host);
    bool updateAddress(const Poco::Net::IPAddress & address);

    void addToNewHosts(const String & host);
    void addToNewAddresses(const Poco::Net::IPAddress & address);
};

}
