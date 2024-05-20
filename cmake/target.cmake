if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    set (OS_FREEBSD 1)
    add_definitions(-D OS_FREEBSD)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_DARWIN 1)
    add_definitions(-D OS_DARWIN)
endif ()

if (CMAKE_CROSSCOMPILING)
    if (OS_DARWIN)
        # Handle macOS specific configuration
    elseif (OS_LINUX)
        if (ARCH_AARCH64)
            # Handle Linux and Android specific configuration for AArch64
        endif ()
    elseif (OS_FREEBSD)
        # Handle FreeBSD specific configuration
    else ()
        message(FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    # Don't know why but CXX_STANDARD doesn't work for cross-compilation
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILE_TARGET}")
endif ()
