#pragma once

#include <sstream>
#include <string>

namespace RK
{
template <class T>
std::string toHexString(T i)
{
    std::ostringstream ss;
    ss << "0x" << std::hex << i;
    return ss.str();
};
}
