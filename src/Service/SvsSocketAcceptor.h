#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Environment.h>
#include <Poco/NObserver.h>
#include <Poco/SharedPtr.h>
#include <vector>

#include <Core/Context.h>
#include <Service/SvsSocketReactor.h>


using Poco::Net::Socket;
using Poco::Net::ServerSocket;
using Poco::Net::StreamSocket;
using Poco::NObserver;
using Poco::AutoPtr;


namespace DB {

    template <class ServiceHandler, class SR>
    class SvsSocketAcceptor
/// This class implements the Acceptor part of the Acceptor-Connector design pattern.
    /// Only the difference from single-threaded version is documented here, For full 
    /// description see Poco::Net::SocketAcceptor documentation.
    /// 
    /// This is a multi-threaded version of SocketAcceptor, it differs from the
    /// single-threaded version in number of reactors (defaulting to number of processors)
    /// that can be specified at construction time and is rotated in a round-robin fashion
    /// by event handler. See ParallelSocketAcceptor::onAccept and 
    /// ParallelSocketAcceptor::createServiceHandler documentation and implementation for 
    /// details.
    {
    public:
        using ParallelReactor = SvsSocketReactor<SR>;
        using Observer = Poco::Observer<SvsSocketAcceptor, ReadableNotification>;

        explicit SvsSocketAcceptor(
            const String& name, Context & keeper_context_, ServerSocket & socket, unsigned threads = Poco::Environment::processorCount())
            : name_(name), keeper_context(keeper_context_), socket_(socket), reactor_(nullptr), threads_(threads), next_(0)
        /// Creates a ParallelSocketAcceptor using the given ServerSocket,
        /// sets number of threads and populates the reactors vector.
        {
            init();
        }

        SvsSocketAcceptor(
            const String& name,
            Context & keeper_context_,
            ServerSocket & socket,
            SocketReactor & reactor,
            const Poco::Timespan & timeout,
            unsigned threads = Poco::Environment::processorCount())
            : name_(name)
            , socket_(socket)
            , reactor_(&reactor)
            , threads_(threads)
            , next_(0)
            , keeper_context(keeper_context_)
            , timeout_(timeout)
        /// Creates a ParallelSocketAcceptor using the given ServerSocket, sets the
        /// number of threads, populates the reactors vector and registers itself 
        /// with the given SocketReactor.
        {
            init();
            reactor_->addEventHandler(socket_, Observer(*this, &SvsSocketAcceptor::onAccept));
            /// It is necessary to wake up the reactor.
            reactor_->wakeUp();
        }

        virtual ~SvsSocketAcceptor()
        /// Destroys the ParallelSocketAcceptor.
        {
            try
            {
                if (reactor_)
                {
                    reactor_->removeEventHandler(socket_, Observer(*this, &SvsSocketAcceptor::onAccept));
                }
            }
            catch (...)
            {
            }
        }

        void setReactor(SocketReactor& reactor)
        /// Sets the reactor for this acceptor.
        {
            registerAcceptor(reactor);
        }

        virtual void registerAcceptor(SocketReactor& reactor)
        /// Registers the ParallelSocketAcceptor with a SocketReactor.
        ///
        /// A subclass can override this function to e.g.
        /// register an event handler for timeout event.
        /// 
        /// The overriding method must either call the base class
        /// implementation or register the accept handler on its own.
        {
            reactor_ = &reactor;
            if (!reactor_->hasEventHandler(socket_, Observer(*this, &SvsSocketAcceptor::onAccept)))
            {
                reactor_->addEventHandler(socket_, Observer(*this, &SvsSocketAcceptor::onAccept));
                reactor_->wakeUp();
            }
        }
	
        virtual void unregisterAcceptor()
        /// Unregisters the ParallelSocketAcceptor.
        ///
        /// A subclass can override this function to e.g.
        /// unregister its event handler for a timeout event.
        /// 
        /// The overriding method must either call the base class
        /// implementation or unregister the accept handler on its own.
        {
            if (reactor_)
            {
                reactor_->removeEventHandler(socket_, Observer(*this, &SvsSocketAcceptor::onAccept));
            }
        }
	
        void onAccept(ReadableNotification* pNotification)
        /// Accepts connection and creates event handler.
        /// TODO why wait a moment?  For when adding EventHandler it does not wake up register.
        /// and need register event? no
        {
            pNotification->release();
            StreamSocket sock = socket_.acceptConnection();
            createServiceHandler(sock);
        }

    protected:
        using ReactorVec = std::vector<typename ParallelReactor::Ptr>;

        virtual ServiceHandler* createServiceHandler(StreamSocket& socket)
        /// Create and initialize a new ServiceHandler instance.
        /// If socket is already registered with a reactor, the new
        /// ServiceHandler instance is given that reactor; otherwise,
        /// the next reactor is used. Reactors are rotated in round-robin
        /// fashion.
        ///
        /// Subclasses can override this method.
        {
            socket.setBlocking(false);
            SocketReactor* pReactor = reactor(socket);
            if (!pReactor)
            {
                std::size_t next = next_++;
                if (next_ == reactors_.size()) next_ = 0;
                pReactor = reactors_[next];
            }
            auto* ret = new ServiceHandler(keeper_context, socket, *pReactor);
            pReactor->wakeUp();
            return ret;
        }

        SocketReactor* reactor(const Socket& socket)
        /// Returns reactor where this socket is already registered
        /// for polling, if found; otherwise returns null pointer.
        {
            typename ReactorVec::iterator it = reactors_.begin();
            typename ReactorVec::iterator end = reactors_.end();
            for (; it != end; ++it)
            {
                if ((*it)->has(socket)) return it->get();
            }
            return nullptr;
        }

        SocketReactor* reactor()
        /// Returns a pointer to the SocketReactor where
        /// this SocketAcceptor is registered.
        ///
        /// The pointer may be null.
        {
            return reactor_;
        }

        Socket& socket()
        /// Returns a reference to the SocketAcceptor's socket.
        {
            return socket_;
        }

        void init()
        /// Populates the reactors vector.
        {
            poco_assert (threads_ > 0);

            for (unsigned i = 0; i < threads_; ++i)
                reactors_.push_back(new ParallelReactor(timeout_, name_ + "#" + std::to_string(i)));
        }

        ReactorVec& reactors()
        /// Returns reference to vector of reactors.
        {
            return reactors_;
        }

        SocketReactor* reactor(std::size_t idx)
        /// Returns reference to the reactor at position idx.
        {
            return reactors_.at(idx).get();
        }

        std::size_t next()
        /// Returns the next reactor index.
        {
            return next_;
        }

    private:
        SvsSocketAcceptor() = delete;
        SvsSocketAcceptor(const SvsSocketAcceptor &) = delete;
        SvsSocketAcceptor & operator = (const SvsSocketAcceptor &) = delete;

        String name_;

        ServerSocket socket_;
        SocketReactor* reactor_;
        unsigned threads_;
        ReactorVec     reactors_;
        std::size_t    next_;

        Context & keeper_context;
        Poco::Timespan timeout_;
    };


}
