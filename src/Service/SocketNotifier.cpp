/**
 * Copyright 2021-2023 JD.com, Inc.
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
#include <Service/SocketNotifier.h>
#include <Service/SocketReactor.h>
#include <Service/SocketNotification.h>


namespace RK {


SocketNotifier::SocketNotifier(const Socket& socket):
	_socket(socket)
{
}


SocketNotifier::~SocketNotifier()
{
}


void SocketNotifier::addObserver(SocketReactor* pReactor, const Poco::AbstractObserver& observer)
{
	_nc.addObserver(observer);
	ScopedLock l(_mutex);
	if (observer.accepts(pReactor->_pReadableNotification))
		_events.insert(pReactor->_pReadableNotification.get());
	else if (observer.accepts(pReactor->_pWritableNotification))
		_events.insert(pReactor->_pWritableNotification.get());
	else if (observer.accepts(pReactor->_pErrorNotification))
		_events.insert(pReactor->_pErrorNotification.get());
	else if (observer.accepts(pReactor->_pTimeoutNotification))
		_events.insert(pReactor->_pTimeoutNotification.get());
}


void SocketNotifier::removeObserver(SocketReactor* pReactor, const Poco::AbstractObserver& observer)
{
	_nc.removeObserver(observer);
	ScopedLock l(_mutex);
	EventSet::iterator it = _events.end();
	if (observer.accepts(pReactor->_pReadableNotification))
		it = _events.find(pReactor->_pReadableNotification.get());
	else if (observer.accepts(pReactor->_pWritableNotification))
		it = _events.find(pReactor->_pWritableNotification.get());
	else if (observer.accepts(pReactor->_pErrorNotification))
		it = _events.find(pReactor->_pErrorNotification.get());
	else if (observer.accepts(pReactor->_pTimeoutNotification))
		it = _events.find(pReactor->_pTimeoutNotification.get());
	if (it != _events.end())
		_events.erase(it);
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
