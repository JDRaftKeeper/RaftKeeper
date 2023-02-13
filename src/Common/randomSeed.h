#pragma once

#include <cstdint>
#include <common/types.h>

/** Returns a number suitable as seed for PRNG. Use clock_gettime, pid and so on. */
RK::UInt64 randomSeed();
