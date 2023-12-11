/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#pragma once

#include <atomic>
#include <map>

#include <Poco/AutoPtr.h>
#include <Poco/Net/Net.h>
#include <Poco/Net/Socket.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>
#include <Poco/Timespan.h>

#include <Common/NIO/Observer.h>
#include <Common/NIO/PollSet.h>
#include <Common/NIO/SocketNotification.h>
#include <Common/NIO/SocketNotifier.h>


using Poco::AutoPtr;
using Poco::Net::Socket;

namespace RK
{

/// This class, which is part of the Reactor pattern,
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
/// Event handlers (any class can be an event handler - there
/// is no base class for event handlers) can be registered
/// with the addEventHandler() method and deregistered with
/// the removeEventHandler() method.
///
/// An event handler is always registered for a certain socket,
/// which is given in the call to addEventHandler(). Any method
/// of the event handler class can be registered to handle the
/// event - the only requirement is that the method takes
/// a pointer to an instance of SocketNotification (or a subclass of it)
/// as argument.
///
/// Once started, the SocketReactor waits for events
/// on the registered sockets, using PollSet.
/// If an event is detected, the corresponding event handler
/// is invoked. There are five event types (and corresponding
/// notification classes) defined: ReadableNotification, WritableNotification,
/// ErrorNotification, TimeoutNotification, IdleNotification and
/// ShutdownNotification.
///
/// The ReadableNotification will be dispatched if a socket becomes
/// readable. The WritableNotification will be dispatched if a socket
/// becomes writable. The ErrorNotification will be dispatched if
/// there is an error condition on a socket.
///
/// If the timeout expires and no event has occurred, a
/// TimeoutNotification will be dispatched to all event handlers
/// registered for it. This is done in the onTimeout() method
/// which can be overridden by subclasses to perform custom
/// timeout processing.
///
/// If there are no sockets for the SocketReactor to pass to
/// PollSet, an IdleNotification will be dispatched to
/// all event handlers registered for it. This is done in the
/// onIdle() method which can be overridden by subclasses
/// to perform custom idle processing. Since onIdle() will be
/// called repeatedly in a loop, it is recommended to do a
/// short sleep or yield in the event handler.
///
/// Finally, when the SocketReactor is about to shut down (as a result
/// of stop() being called), it dispatches a ShutdownNotification
/// to all event handlers. This is done in the onShutdown() method
/// which can be overridden by subclasses to perform custom
/// shutdown processing.
///
/// The SocketReactor is implemented so that it can
/// run in its own thread. It is also possible to run
/// multiple SocketReactors in parallel, as long as
/// they work on different sockets.
///
/// It is safe to call addEventHandler() and removeEventHandler()
/// from another thread while the SocketReactor is running. Also,
/// it is safe to call addEventHandler() and removeEventHandler()
/// from event handlers.
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
    ///     Poco::Observer<MyEventHandler, SocketNotification> obs(*this, &MyEventHandler::handleMyEvent);
    ///     reactor.addEventHandler(obs);
    void addEventHandlers(const Socket & socket, const std::vector<AbstractObserver *> & observers);

    /// Returns true if the observer is registered with SocketReactor for the given socket.
    bool hasEventHandler(const Socket & socket, const AbstractObserver & observer);

    /// Unregisters an event handler with the SocketReactor.
    ///
    /// Usage:
    ///     Poco::Observer<MyEventHandler, SocketNotification> obs(*this, &MyEventHandler::handleMyEvent);
    ///     reactor.removeEventHandler(obs);
    void removeEventHandler(const Socket & socket, const AbstractObserver & observer);

    /// Returns true if socket is registered with this rector.
    bool has(const Socket & socket) const;

protected:
    /// Called if the timeout expires and no other events are available.
    ///
    /// Can be overridden by subclasses. The default implementation
    /// dispatches the TimeoutNotification and thus should be called by overriding
    /// implementations.
    virtual void onTimeout();

    /// Called if no sockets are available to call select() on.
    ///
    /// Can be overridden by subclasses. The default implementation
    /// dispatches the IdleNotification and thus should be called by overriding
    /// implementations.
    virtual void onIdle();

    virtual void onShutdown();
    /// Called when the SocketReactor is about to terminate.
    ///
    /// Can be overridden by subclasses. The default implementation
    /// dispatches the ShutdownNotification and thus should be called by overriding
    /// implementations.

    /// Called when the SocketReactor is busy and at least one notification
    /// has been dispatched.
    ///
    /// Can be overridden by subclasses to perform additional
    /// periodic tasks. The default implementation does nothing.
    virtual void onBusy();

    /// Dispatches the given notification to all observers
    /// registered for the given socket.
    void dispatch(const Socket & socket, SocketNotification * pNotification);

    /// Dispatches the given notification to all observers.
    void dispatch(SocketNotification * pNotification);

private:
    using NotifierPtr = Poco::AutoPtr<SocketNotifier>;
    using NotificationPtr = Poco::AutoPtr<SocketNotification>;
    using EventHandlerMap = std::map<poco_socket_t, NotifierPtr>;
    using MutexType = Poco::FastMutex;
    using ScopedLock = MutexType::ScopedLock;

    bool hasSocketHandlers();

    void sleep();
    void dispatch(NotifierPtr & pNotifier, SocketNotification * pNotification);

    NotifierPtr getNotifier(const Socket & socket, bool makeNew = false);

    enum
    {
        DEFAULT_TIMEOUT = 250000
    };

    std::atomic<bool> stopped;
    Poco::Timespan timeout;
    EventHandlerMap handlers;
    PollSet poll_set;
    NotificationPtr rnf;
    NotificationPtr wnf;
    NotificationPtr enf;
    NotificationPtr tnf;
    NotificationPtr inf;
    NotificationPtr snf;

    MutexType mutex;
    Poco::Event event;

    friend class SocketNotifier;
};


}
