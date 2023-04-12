# During first run of cmake the toolchain file will be loaded twice,
# - /usr/share/cmake-3.23/Modules/CMakeDetermineSystem.cmake
# - /bld/CMakeFiles/3.23.2/CMakeSystem.cmake
#
# But once you already have non-empty cmake cache it will be loaded only
# once:
# - /bld/CMakeFiles/3.23.2/CMakeSystem.cmake
#
# This has no harm except for double load of toolchain will add
# --gcc-toolchain multiple times that will not allow ccache to reuse the
# cache.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-linux-gnu")

# Will be changed later, but somehow needed to be set here.
set (CMAKE_AR "ar")
set (CMAKE_RANLIB "ranlib")

