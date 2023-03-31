#pragma once

#include <Common/IO/WriteBuffer.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE

namespace RK
{

/**
 * writer buffer for NuRaft Buffer.
 */
class WriteBufferFromNuraftBuffer : public WriteBuffer
{
private:
    nuraft::ptr<nuraft::buffer> buffer;
    bool is_finished = false;

    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;

    void nextImpl() override;

public:
    WriteBufferFromNuraftBuffer();

    void finalize() override final;
    nuraft::ptr<nuraft::buffer> getBuffer();

    ~WriteBufferFromNuraftBuffer() override;
};

}
