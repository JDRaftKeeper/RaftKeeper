/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*
*/
#include <Common/NIO/SocketNotifier.h>
#include <Common/NIO/SocketReactor.h>
#include <Common/NIO/SocketNotification.h>


namespace RK {


SocketNotifier::SocketNotifier(const Socket& socket):
    _socket(socket)
{
}

bool SocketNotifier::addObserverIfNotExist(SocketReactor*, const Poco::AbstractObserver& observer)
{
    return _nc.addObserverIfNotExist(observer);
}


bool SocketNotifier::removeObserverIfExist(SocketReactor*, const Poco::AbstractObserver& observer)
{
    return _nc.removeObserverIfExist(observer);
}


namespace
{
    static Socket nullSocket;
}


void SocketNotifier::dispatch(SocketNotification* pNotification)
{
    pNotification->setSocket(_socket);
    pNotification->duplicate();
    try
    {
        _nc.postNotification(pNotification);
    }
    catch (...)
    {
        pNotification->setSocket(nullSocket);
        throw;
    }
    pNotification->setSocket(nullSocket);
}


}
