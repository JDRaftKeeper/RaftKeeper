if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    message(FATAL_ERROR "Only clang is supported")
elseif (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    message(FATAL_ERROR "Only regular clang is supported, not AppleClang")
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

if (COMPILER_CLANG)
    set (CLANG_MINIMUM_VERSION 17)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Clang version must be at least ${CLANG_MINIMUM_VERSION}.")
    endif ()
else ()
    message (WARNING "You are using an unsupported compiler.")
endif ()

STRING(REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
LIST(GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

# Example values: `lld-10`, `gold`.
option (LINKER_NAME "Linker name or full path")

if (NOT LINKER_NAME)
    find_program (LLD_PATH NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "lld-${COMPILER_VERSION_MAJOR}" "ld.lld" "lld")
    find_program (GOLD_PATH NAMES "ld.gold" "gold")
endif ()

if (OS_LINUX AND NOT LINKER_NAME)
    # We prefer LLD linker over Gold or BFD on Linux.
    if (LLD_PATH)
        # Clang driver simply allows full linker path.
        set (LINKER_NAME ${LLD_PATH})
    endif ()

    if (NOT LINKER_NAME)
        if (GOLD_PATH)
            set (LINKER_NAME ${GOLD_PATH})
        endif ()
    endif ()
endif ()

message(STATUS "COMPILER_VERSION_MAJOR ${COMPILER_VERSION_MAJOR}")

if (LINKER_NAME)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --ld-path=${LINKER_NAME}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --ld-path=${LINKER_NAME}")
    message(STATUS "Using custom linker by name: ${LINKER_NAME}")
endif ()
