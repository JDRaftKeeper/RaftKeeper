#pragma once
#include <stdexcept>

/// Throw RK::Exception-like exception before its definition.
/// RK::Exception derived from Poco::Exception derived from std::exception.
/// RK::Exception generally cought as Poco::Exception. std::exception generally has other catch blocks and could lead to other outcomes.
/// RK::Exception is not defined yet. It'd better to throw Poco::Exception but we do not want to include any big header here, even <string>.
/// So we throw some std::exception instead in the hope its catch block is the same as RK::Exception one.
template <typename T>
inline void throwError(const T & err)
{
    throw std::runtime_error(err);
}
