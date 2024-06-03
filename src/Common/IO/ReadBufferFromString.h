#pragma once

#include "ReadBufferFromMemory.h"


namespace RK
{

/** Allows to read from std::string-like object.
  */
class ReadBufferFromString : public ReadBufferFromMemory
{
public:
    /// std::string or something similar
    template <typename S>
    ReadBufferFromString(const S & s) : ReadBufferFromMemory(s.data(), s.size()) {}
};

class ReadBufferFromOwnString : public String, public ReadBufferFromString
{
public:
    template <typename S>
    explicit ReadBufferFromOwnString(S && s_) : String(std::forward<S>(s_)), ReadBufferFromString(*this)
    {
    }
};

}
