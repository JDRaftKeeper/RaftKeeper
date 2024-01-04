#pragma once

#include <sstream>
#include <string>

namespace RK
{
template <class T>
String toHexString(T i)
{
    std::ostringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss << "0x" << std::hex << i;
    return ss.str();
}
}
