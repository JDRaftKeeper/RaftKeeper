#include <common/memory.h>

#include <new>

#if defined(OS_LINUX)
#   include <malloc.h>
#elif defined(OS_DARWIN)
#   include <malloc/malloc.h>
#endif

#if defined(OS_DARWIN) && defined(BUNDLED_STATIC_JEMALLOC)
extern "C"
{
extern void zone_register();
}

struct InitializeJemallocZoneAllocatorForOSX
{
    InitializeJemallocZoneAllocatorForOSX()
    {
        /// In case of OSX jemalloc register itself as a default zone allocator.
        ///
        /// But when you link statically then zone_register() will not be called,
        /// and even will be optimized out:
        ///
        /// It is ok to call it twice (i.e. in case of shared libraries)
        /// Since zone_register() is a no-op if the default zone is already replaced with something.
        ///
        /// https://github.com/jemalloc/jemalloc/issues/708
        zone_register();
    }
} initializeJemallocZoneAllocatorForOSX;
#endif

/// Replace default new/delete with memory tracking versions.
/// @sa https://en.cppreference.com/w/cpp/memory/new/operator_new
///     https://en.cppreference.com/w/cpp/memory/new/operator_delete


/// new

void * operator new(std::size_t size)
{
    return Memory::newImpl(size);
}

void * operator new[](std::size_t size)
{
    return Memory::newImpl(size);
}

void * operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    return Memory::newNoExept(size);
}

void * operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    return Memory::newNoExept(size);
}

/// delete

/// C++17 std 21.6.2.1 (11)
/// If a function without a size parameter is defined, the program should also define the corresponding function with a size parameter.
/// If a function with a size parameter is defined, the program shall also define the corresponding version without the size parameter.

/// cppreference:
/// It's unspecified whether size-aware or size-unaware version is called when deleting objects of
/// incomplete type and arrays of non-class and trivially-destructible class types.

void operator delete(void * ptr) noexcept
{
    Memory::deleteImpl(ptr);
}

void operator delete[](void * ptr) noexcept
{
    Memory::deleteImpl(ptr);
}

void operator delete(void * ptr, std::size_t size) noexcept
{
    Memory::deleteSized(ptr, size);
}

void operator delete[](void * ptr, std::size_t size) noexcept
{
    Memory::deleteSized(ptr, size);
}
