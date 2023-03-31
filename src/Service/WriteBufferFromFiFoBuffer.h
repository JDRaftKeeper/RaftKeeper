#pragma once

#include <Poco/FIFOBuffer.h>
#include <common/types.h>
#include <Common/IO/WriteBuffer.h>

namespace RK
{

using Poco::FIFOBuffer;

/**
 * writer buffer for Poco FiFoBuffer
 **/
class WriteBufferFromFiFoBuffer : public WriteBuffer
{
private:
    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;

    std::shared_ptr<FIFOBuffer> buffer;
    bool is_finished = false;

    void nextImpl() override;

public:
    explicit WriteBufferFromFiFoBuffer(size_t size = initial_size);

    void finalize() override final;
    std::shared_ptr<FIFOBuffer> getBuffer();

    ~WriteBufferFromFiFoBuffer() override;
};

using WriteBufferFromFiFoBufferPtr = std::shared_ptr<WriteBufferFromFiFoBuffer>;

}
