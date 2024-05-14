/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*/
#pragma once

#include <atomic>
#include <map>

#include <Poco/Net/Net.h>
#include <Poco/Net/Socket.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>
#include <Poco/Timespan.h>

#include <Network/Observer.h>
#include <Network/PollSet.h>
#include <Network/SocketNotification.h>
#include <Network/SocketNotifier.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>


using Poco::Net::Socket;

namespace RK
{

/// This class, which is the core of the Reactor pattern,
/// implements the "Initiation Dispatcher".
///
/// The Reactor pattern has been described in the book
/// "Pattern Languages of Program Design" by Jim Coplien
/// and Douglas C. Schmidt (Addison Wesley, 1995).
///
/// The Reactor design pattern handles service requests that
/// are delivered concurrently to an application by one or more
/// clients. Each service in an application may consist of several
/// methods and is represented by a separate event handler. The event
/// handler is responsible for servicing service-specific requests.
/// The SocketReactor dispatches the event handlers.
///
/// Once started, the SocketReactor waits for events
/// on the registered sockets, using PollSet.
/// If an event is detected, the corresponding event handler
/// is invoked. There are five event types (and corresponding
/// notification classes) defined: ReadableNotification, WritableNotification,
/// ErrorNotification, TimeoutNotification, IdleNotification and
/// ShutdownNotification.
class SocketReactor : public Poco::Runnable
{
public:
    SocketReactor();
    explicit SocketReactor(const Poco::Timespan & timeout);

    virtual ~SocketReactor() override = default;

    void run() override;
    void stop();

    /// Wake up the Reactor
    void wakeUp();

    /// Sets the timeout.
    ///
    /// If no other event occurs for the given timeout
    /// interval, a timeout event is sent to all event listeners.
    ///
    /// The default timeout is 250 milliseconds;
    /// The timeout is passed to the PollSet.
    void setTimeout(const Poco::Timespan & timeout);
    const Poco::Timespan & getTimeout() const;

    /// Deprecated for it has TSAN heap-use-after-free risk.
    /// Acceptor thread wants to add 3 events(read, error, shutdown),
    /// when the first event `read` added, the handler thread can trigger
    /// read event if the socket is not available, handler thread may destroy
    /// itself and the socket, so heap-use-after-free happens.
    void addEventHandler(const Socket & socket, const AbstractObserver & observer);

    /// Registers an event handler with the SocketReactor.
    ///
    /// Usage:
    ///     Observer<MyEventHandler, MyNotification> obs(*this, &MyEventHandler::handleMyEvent);
    ///     getWorkerReactor.addEventHandler(obs);
    void addEventHandlers(const Socket & socket, const std::vector<AbstractObserver *> & observers);

    /// Returns true if the observer is registered with SocketReactor for the given socket.
    [[maybe_unused]] bool hasEventHandler(const Socket & socket, const AbstractObserver & observer);

    /// Unregisters an event handler with the SocketReactor.
    ///
    /// Usage:
    ///     Observer<MyEventHandler, MyNotification> obs(*this, &MyEventHandler::handleMyEvent);
    ///     getWorkerReactor.removeEventHandler(obs);
    void removeEventHandler(const Socket & socket, const AbstractObserver & observer);

    /// Returns true if socket is registered with this rector.
    bool has(const Socket & socket) const;

protected:
    /// Called if the timeout expires and no readable events are available.
    virtual void onTimeout();

    /// Called if no sockets are available.
    virtual void onIdle();

    /// Called when the SocketReactor is about to terminate.
    virtual void onShutdown();

    /// Called when the SocketReactor is busy and at least one notification
    /// has been dispatched.
    virtual void onBusy();

    /// Dispatches the given notification to observers which are registered for the given socket.
    void dispatch(const Socket & socket, const Notification & notification);

    /// Dispatches the given notification to all observers.
    void dispatch(const Notification & notification);

private:
    using SocketNotifierMap = std::map<poco_socket_t, SocketNotifierPtr>;

    using MutexType = Poco::FastMutex;
    using ScopedLock = MutexType::ScopedLock;

    void sleep();

    void dispatch(SocketNotifierPtr & pNotifier, const Notification & notification);

    bool hasSocketHandlers();

    SocketNotifierPtr getNotifier(const Socket & socket, bool makeNew = false);

    enum
    {
        DEFAULT_TIMEOUT = 250000
    };

    ///
    Poco::Timespan timeout;
    std::atomic<bool> stopped;

    SocketNotifierMap notifiers;
    PollSet poll_set;

    /// Notifications which will dispatched to observers
    NotificationPtr rnf;
    NotificationPtr wnf;
    NotificationPtr enf;
    NotificationPtr tnf;
    NotificationPtr inf;
    NotificationPtr snf;

    MutexType mutex;
    Poco::Event event;

    Poco::Logger * log;
};


/// SocketReactor which run asynchronously.
class AsyncSocketReactor : public SocketReactor
{
public:
    explicit AsyncSocketReactor(const Poco::Timespan & timeout, const std::string & name);
    ~AsyncSocketReactor() override;

    void run() override;

protected:
    void onIdle() override;

private:
    void startup();

    Poco::Thread thread;
    const std::string name;
};


using SocketReactorPtr = std::shared_ptr<SocketReactor>;
using AsyncSocketReactorPtr = std::shared_ptr<AsyncSocketReactor>;

}
