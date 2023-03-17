#pragma once
#include <Common/IO/ReadBufferFromMemory.h>

#include <libnuraft/nuraft.hxx> // Y_IGNORE

namespace RK
{

class ReadBufferFromNuraftBuffer : public ReadBufferFromMemory
{
public:
    explicit ReadBufferFromNuraftBuffer(nuraft::ptr<nuraft::buffer> buffer)
        : ReadBufferFromMemory(buffer->data_begin(), buffer->size())
    {}
    explicit ReadBufferFromNuraftBuffer(nuraft::buffer & buffer)
        : ReadBufferFromMemory(buffer.data_begin(), buffer.size())
    {}
};

}
