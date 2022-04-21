#pragma once

#include <IO/WriteBuffer.h>
#include <Poco/FIFOBuffer.h>
#include <Service/Types.h>

namespace DB
{

using Poco::FIFOBuffer;

class WriteBufferFromFiFoBuffer : public WriteBuffer
{
private:
    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;

    ptr<FIFOBuffer> buffer;
    bool is_finished = false;

    void nextImpl() override;

public:
    WriteBufferFromFiFoBuffer();

    WriteBufferFromFiFoBuffer(size_t size);

    void finalize() override final;
    ptr<FIFOBuffer> getBuffer();
    bool isFinished() const { return is_finished; }

    ~WriteBufferFromFiFoBuffer() override;
};

using WriteBufferFromFiFoBufferPtr = std::shared_ptr<WriteBufferFromFiFoBuffer>;

}
