/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#include <Common/NIO/SocketNotification.h>
#include <Common/NIO/SocketNotifier.h>
#include <Common/NIO/SocketReactor.h>


namespace RK
{

SocketNotifier::SocketNotifier(const Socket & socket_) : socket(socket_)
{
}

bool SocketNotifier::addObserverIfNotExist(SocketReactor *, const AbstractObserver & observer)
{
    return nc.addObserverIfNotExist(observer);
}


bool SocketNotifier::removeObserverIfExist(SocketReactor *, const AbstractObserver & observer)
{
    return nc.removeObserverIfExist(observer);
}


namespace
{
    Socket nullSocket;
}


void SocketNotifier::dispatch(SocketNotification * pNotification)
{
    pNotification->setSocket(socket);
    pNotification->duplicate();
    try
    {
        nc.postNotification(pNotification);
    }
    catch (...)
    {
        pNotification->setSocket(nullSocket);
        throw;
    }
    pNotification->setSocket(nullSocket);
}


}
