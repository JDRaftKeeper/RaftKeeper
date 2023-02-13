#pragma once

#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <Poco/FIFOBuffer.h>

namespace RK
{

using Poco::FIFOBuffer;

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
    bool isFinished() const { return is_finished; }

    ~WriteBufferFromFiFoBuffer() override;
};

using WriteBufferFromFiFoBufferPtr = std::shared_ptr<WriteBufferFromFiFoBuffer>;

}
