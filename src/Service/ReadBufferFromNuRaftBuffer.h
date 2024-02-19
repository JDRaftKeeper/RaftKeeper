#pragma once

#include <Common/IO/ReadBufferFromMemory.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE

namespace RK
{

class ReadBufferFromNuRaftBuffer : public ReadBufferFromMemory
{
public:
    explicit ReadBufferFromNuRaftBuffer(nuraft::ptr<nuraft::buffer> buffer) : ReadBufferFromMemory(buffer->data_begin(), buffer->size()) { }
    explicit ReadBufferFromNuRaftBuffer(nuraft::buffer & buffer) : ReadBufferFromMemory(buffer.data_begin(), buffer.size()) { }
};

}
