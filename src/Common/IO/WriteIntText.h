#pragma once

#include "common/itoa.h"
#include "Core/Defines.h"
#include "WriteBuffer.h"

/// 40 digits or 39 digits and a sign
#define WRITE_HELPERS_MAX_INT_WIDTH 40U

namespace RK
{

namespace detail
{
    template <typename T>
    void NO_INLINE writeUIntTextFallback(T x, WriteBuffer & buf)
    {
        char tmp[WRITE_HELPERS_MAX_INT_WIDTH];
        int len = itoa(x, tmp) - tmp;
        buf.write(tmp, len);
    }
}

template <typename T>
void writeIntText(T x, WriteBuffer & buf)
{
    if (likely(reinterpret_cast<intptr_t>(buf.position()) + WRITE_HELPERS_MAX_INT_WIDTH < reinterpret_cast<intptr_t>(buf.buffer().end())))
        buf.position() = itoa(x, buf.position());
    else
        detail::writeUIntTextFallback(x, buf);
}

}
