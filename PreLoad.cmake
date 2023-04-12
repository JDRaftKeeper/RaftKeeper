# Default toolchain - this is needed to avoid dependency on OS files.
execute_process(COMMAND uname -s OUTPUT_VARIABLE OS)
execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCH)

if (OS MATCHES "Linux"
        AND NOT DEFINED CMAKE_TOOLCHAIN_FILE
        AND NOT DISABLE_HERMETIC_BUILD
        AND ("$ENV{CC}" MATCHES ".*clang.*" OR CMAKE_C_COMPILER MATCHES ".*clang.*"))

    if (ARCH MATCHES "amd64|x86_64")
        set (CMAKE_TOOLCHAIN_FILE "cmake/linux/toolchain-x86_64.cmake" CACHE INTERNAL "")
    elseif (ARCH MATCHES "^(aarch64.*|AARCH64.*|arm64.*|ARM64.*)")
        set (CMAKE_TOOLCHAIN_FILE "cmake/linux/toolchain-aarch64.cmake" CACHE INTERNAL "")
    else ()
        message (FATAL_ERROR "Unsupported architecture: ${ARCH}")
    endif ()

endif()

message(STATUS "CMAKE_TOOLCHAIN_FILE - ${CMAKE_TOOLCHAIN_FILE}")


