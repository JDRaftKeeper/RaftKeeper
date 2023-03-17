#pragma once

#include <common/defines.h>

/// The size of the I/O buffer by default.
#define DEFAULT_BUFFER_SIZE 1048576ULL

#define SHOW_CHARS_ON_SYNTAX_ERROR ptrdiff_t(160)

#if !__has_include(<sanitizer/asan_interface.h>) || !defined(ADDRESS_SANITIZER)
#   define ASAN_UNPOISON_MEMORY_REGION(a, b)
#   define ASAN_POISON_MEMORY_REGION(a, b)
#endif
