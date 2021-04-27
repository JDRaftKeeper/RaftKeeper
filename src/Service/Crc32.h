#pragma once

#include <common/types.h>

namespace DB
{
UInt32 getCRC32(const char * data, size_t length);

bool verifyCRC32(const char * data, size_t len, uint32_t value);

}
