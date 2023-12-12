/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*/
#pragma once

#include <map>

#include <Poco/Net/Socket.h>


namespace RK
{

using Socket = Poco::Net::Socket;
class PollSetImpl;

/// A set of sockets that can be efficiently polled as a whole.
///
/// PollSet is implemented using epoll (Linux) or poll (BSD) APIs.
/// A fallback implementation using select() is also provided.
class PollSet
{
public:
    enum Mode
    {
        POLL_READ = 0x01,
        POLL_WRITE = 0x02,
        POLL_ERROR = 0x04
    };

    using SocketModeMap = std::map<Poco::Net::Socket, int>;

    PollSet();
    ~PollSet();

    /// Adds the given socket to the set, for polling with the given mode.
    void add(const Socket & socket, int mode);

    /// Removes the given socket from the set.
    void remove(const Socket & socket);

    /// Updates the mode of the given socket.
    void update(const Socket & socket, int mode);

    /// Returns true if socket is registered for polling.
    bool has(const Socket & socket) const;

    /// Returns true if no socket is registered for polling.
    bool empty() const;

    /// Removes all sockets from the PollSet.
    void clear();

    /// Waits until the state of at least one of the PollSet's sockets
    /// changes accordingly to its mode, or the timeout expires.
    /// Returns a PollMap containing the sockets that have had
    /// their state changed.
    SocketModeMap poll(const Poco::Timespan & timeout);

    /// Returns the number of sockets monitored.
    int count() const;

    /// Wakes up a waiting PollSet.
    void wakeUp();

private:
    PollSetImpl * impl;

    PollSet(const PollSet &);
    PollSet & operator=(const PollSet &);
};


}
