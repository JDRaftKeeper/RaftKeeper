cmake_minimum_required(VERSION 3.20)

project(RaftKeeper)

# Compatible mode: zookeeper or clickhouse.
option(COMPATIBLE_MODE_ZOOKEEPER
    "ClickHouse client from v22.10 is a little incompatible with Zookeeper, for example multi-read response.
    If it is ON, the built binary is Zookeeper compatible, else it is ClickHouse compatible." ON)

message(STATUS "COMPATIBLE_MODE_ZOOKEEPER: ${COMPATIBLE_MODE_ZOOKEEPER}")

if(COMPATIBLE_MODE_ZOOKEEPER)
    add_compile_definitions(COMPATIBLE_MODE_ZOOKEEPER)
else()
    add_compile_definitions(COMPATIBLE_MODE_CLICKHOUSE)
endif()

# Message level when we can not find some library or tool.
set(RECONFIGURE_MESSAGE_LEVEL WARNING)

include (cmake/arch.cmake)
include (cmake/target.cmake)
include (cmake/tools.cmake)
include (cmake/analysis.cmake)
include (cmake/find/ccache.cmake)
include (cmake/sanitize.cmake)

include (cmake/add_warning.cmake)
set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wall")    # -Werror and many more is also added inside cmake/warnings.cmake

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Write compile_commands.json
set(CMAKE_DEBUG_POSTFIX "d" CACHE STRING "Generate debug library name with a postfix.")

# Check that submodules are present only if source was downloaded with git
if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git" AND NOT EXISTS "${RaftKeeper_SOURCE_DIR}/contrib/boost/boost")
    message (FATAL_ERROR "Submodules are not initialized. Run\n\tgit submodule update --init --recursive")
endif ()

# Default CMAKE_BUILD_TYPE is Release
if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    set (CMAKE_BUILD_TYPE "Release")
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)

list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)

# Global libraries
# See:
# - default_libs.cmake
# - sanitize.cmake
add_library(global-libs INTERFACE)

if (CMAKE_GENERATOR STREQUAL "Ninja" AND NOT DISABLE_COLORED_BUILD)
    # Turn on colored output. https://github.com/ninja-build/ninja/wiki/FAQ
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")
endif ()

# clang: warning: argument unused during compilation: '-specs=/usr/share/dpkg/no-pie-compile.specs' [-Wunused-command-line-argument]
set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wno-unused-command-line-argument")
# generate ranges for fast "addr2line" search
if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -gdwarf-aranges")
endif ()

# If turned `ON`, assumes the user has either the system GTest library or the bundled one.
option(ENABLE_TESTS "Provide rk_unit_test target with Google.Test unit tests" ON)

if (OS_LINUX)
    # Only for Linux, x86_64.
    # Implies ${ENABLE_FASTMEMCPY}
    option(GLIBC_COMPATIBILITY "Enable compatibility with older glibc libraries." ON)
elseif(GLIBC_COMPATIBILITY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Glibc compatibility cannot be enabled in current configuration")
endif ()

# Make sure the final executable has symbols exported
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic")

if (OS_LINUX)
    find_program (OBJCOPY_PATH NAMES "llvm-objcopy" "llvm-objcopy-11" "llvm-objcopy-12" "llvm-objcopy-13" "llvm-objcopy-14" "llvm-objcopy-15" "llvm-objcopy-16" "objcopy")
    if (OBJCOPY_PATH)
        message(STATUS "Using objcopy: ${OBJCOPY_PATH}.")

        if (ARCH_AMD64)
            set(OBJCOPY_ARCH_OPTIONS -O elf64-x86-64 -B i386)
        elseif (ARCH_AARCH64)
            set(OBJCOPY_ARCH_OPTIONS -O elf64-aarch64 -B aarch64)
        endif ()
    else ()
        message(FATAL_ERROR "Cannot find objcopy.")
    endif ()
endif ()

if (OS_DARWIN)
    set(WHOLE_ARCHIVE -all_load)
    set(NO_WHOLE_ARCHIVE -noall_load)
else ()
    set(WHOLE_ARCHIVE --whole-archive)
    set(NO_WHOLE_ARCHIVE --no-whole-archive)
endif ()

# Ignored if `lld` is used
option(ADD_GDB_INDEX_FOR_GOLD "Add .gdb-index to resulting binaries for gold linker.")

if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    # Can be lld or ld-lld.
    if (LINKER_NAME MATCHES "lld$")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gdb-index")
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--gdb-index")
        message (STATUS "Adding .gdb-index via --gdb-index linker option.")
    # we use another tool for gdb-index, because gold linker removes section .debug_aranges, which used inside raftkeeper stacktraces
    # http://sourceware-org.1504.n7.nabble.com/gold-No-debug-aranges-section-when-linking-with-gdb-index-td540965.html#a556932
    elseif (LINKER_NAME MATCHES "gold$" AND ADD_GDB_INDEX_FOR_GOLD)
        find_program (GDB_ADD_INDEX_EXE NAMES "gdb-add-index" DOC "Path to gdb-add-index executable")
        if (NOT GDB_ADD_INDEX_EXE)
            set (USE_GDB_ADD_INDEX 0)
            message (WARNING "Cannot add gdb index to binaries, because gold linker is used, but gdb-add-index executable not found.")
        else()
            set (USE_GDB_ADD_INDEX 1)
            message (STATUS "gdb-add-index found: ${GDB_ADD_INDEX_EXE}")
        endif()
    endif ()
endif()

# Create BuildID when using lld. For other linkers it is created by default.
if (LINKER_NAME MATCHES "lld$")
    # SHA1 is not cryptographically secure but it is the best what lld is offering.
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--build-id=sha1")
endif ()

cmake_host_system_information(RESULT AVAILABLE_PHYSICAL_MEMORY QUERY AVAILABLE_PHYSICAL_MEMORY) # Not available under freebsd


if(NOT AVAILABLE_PHYSICAL_MEMORY OR AVAILABLE_PHYSICAL_MEMORY GREATER 8000)
    # Less `/tmp` usage, more RAM usage.
    option(COMPILER_PIPE "-pipe compiler option" ON)
endif()

if(COMPILER_PIPE)
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -pipe")
else()
    message(STATUS "Disabling compiler -pipe option (have only ${AVAILABLE_PHYSICAL_MEMORY} mb of memory)")
endif()

option(ARCH_NATIVE "Add -march=native compiler flag")

if (ARCH_NATIVE)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=native")
endif ()

set (CMAKE_CXX_STANDARD 23)
set (CMAKE_CXX_EXTENSIONS OFF)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_C_STANDARD 11)
set (CMAKE_C_EXTENSIONS ON)
set (CMAKE_C_STANDARD_REQUIRED ON)

# Enable C++14 sized global deallocation functions. It should be enabled by setting -std=c++14 but I'm not sure.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")

# Compiler-specific coverage flags e.g. -fcoverage-mapping for gcc
option(WITH_COVERAGE "Profile the resulting binary/binaries" OFF)

if (WITH_COVERAGE)
    if (SANITIZE)
        message (FATAL_ERROR "Cannot enable coverage with sanitizers")
    endif ()
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    # If we want to disable coverage for specific translation units
    set(WITHOUT_COVERAGE "-fno-profile-instr-generate -fno-coverage-mapping")
endif ()

set(COMPILER_FLAGS "${COMPILER_FLAGS}")

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} ${PLATFORM_EXTRA_CXX_FLAG} ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} ${COMMON_WARNING_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_C_FLAGS_ADD}")

if (OS_DARWIN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-U,_inside_main")
endif()

# Display absolute paths in error messages. Otherwise KDevelop fails to navigate to correct file and opens a new file instead.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-absolute-paths")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-absolute-paths")

if (NOT ENABLE_TESTS AND NOT SANITIZE)
    # https://clang.llvm.org/docs/ThinLTO.html
    option(ENABLE_THINLTO "Clang-specific link time optimization" ON)
endif()

# We cannot afford to use LTO when compiling unit tests, and it's not enough
# to only supply -fno-lto at the final linking stage. So we disable it
# completely.
if (ENABLE_THINLTO AND NOT ENABLE_TESTS AND NOT SANITIZE)
    # Link time optimization
    set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -flto=thin")
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -flto=thin")
    set (CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -flto=thin")
elseif (ENABLE_THINLTO)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot enable ThinLTO")
endif ()

# Always prefer llvm tools when using clang. For instance, we cannot use GNU ar when llvm LTO is enabled
find_program (LLVM_AR_PATH NAMES "llvm-ar" "llvm-ar-13" "llvm-ar-14" "llvm-ar-15" "llvm-ar-16" "llvm-ar-17")

if (LLVM_AR_PATH)
    message(STATUS "Using llvm-ar: ${LLVM_AR_PATH}.")
    set (CMAKE_AR ${LLVM_AR_PATH})
else ()
    message(WARNING "Cannot find llvm-ar. System ar will be used instead. It does not work with ThinLTO.")
endif ()

find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib" "llvm-ranlib-13" "llvm-ranlib-14"  "llvm-ranlib-15"  "llvm-ranlib-16" "llvm-ranlib-17")

if (LLVM_RANLIB_PATH)
    message(STATUS "Using llvm-ranlib: ${LLVM_RANLIB_PATH}.")
    set (CMAKE_RANLIB ${LLVM_RANLIB_PATH})
else ()
    message(WARNING "Cannot find llvm-ranlib. System ranlib will be used instead. It does not work with ThinLTO.")
endif ()


option(WERROR "Enable -Werror compiler option" ON)
if (WERROR)
    add_warning(error)
endif()

# Make this extra-checks for correct library dependencies.
if (OS_LINUX AND NOT SANITIZE)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-undefined")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
endif ()

include(cmake/glob_sources.cmake)

if (OS_LINUX)
    include(cmake/linux/default_libs.cmake)
elseif (OS_DARWIN)
    include(cmake/darwin/default_libs.cmake)
elseif (OS_FREEBSD)
    include(cmake/freebsd/default_libs.cmake)
endif ()

######################################
### Add targets below this comment ###
######################################

set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

set (CMAKE_POSITION_INDEPENDENT_CODE OFF)
if (OS_LINUX AND NOT ARCH_ARM)
    # Slightly more efficient code can be generated
    # It's disabled for ARM because otherwise ClickHouse cannot run on Android.
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-pie")
    set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fno-pie")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-no-pie")
endif ()

# https://github.com/include-what-you-use/include-what-you-use
option (USE_INCLUDE_WHAT_YOU_USE "Automatically reduce unneeded includes in source code (external tool)" OFF)

if (USE_INCLUDE_WHAT_YOU_USE)
    find_program(IWYU_PATH NAMES include-what-you-use iwyu)
    if (NOT IWYU_PATH)
        message(FATAL_ERROR "Could not find the program include-what-you-use")
    endif()
    if (${CMAKE_VERSION} VERSION_LESS "3.3.0")
        message(FATAL_ERROR "include-what-you-use requires CMake version at least 3.3.")
    endif()
endif ()

if (ENABLE_TESTS)
    message (STATUS "Unit tests are enabled")
else()
    message(STATUS "Unit tests are disabled")
endif ()

enable_testing() # Enable for tests without binary

# when installing to /usr - place configs to /etc but for /usr/local place to /usr/local/etc
if (CMAKE_INSTALL_PREFIX STREQUAL "/usr")
    set (RAFTKEEPER_ETC_DIR "/etc")
else ()
    set (RAFTKEEPER_ETC_DIR "${CMAKE_INSTALL_PREFIX}/etc")
endif ()

message (STATUS
    "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ;
    SPLIT_SHARED=${SPLIT_SHARED_LIBRARIES}
    CCACHE=${CCACHE_FOUND} ${CCACHE_VERSION}")

include (GNUInstallDirs)
include (cmake/contrib_finder.cmake)

find_contrib_lib(double-conversion) # Must be before parquet
include (cmake/find/poco.cmake)
include (cmake/find/nuraft.cmake)

set (USE_INTERNAL_CITYHASH_LIBRARY ON CACHE INTERNAL "")
find_contrib_lib(cityhash)


if (ENABLE_TESTS)
    include (cmake/find/gtest.cmake)
endif ()

include (cmake/print_flags.cmake)

if (TARGET global-group)
    message(STATUS "Has TARGET global-group")
    install (EXPORT global DESTINATION cmake)
endif ()

add_subdirectory (contrib EXCLUDE_FROM_ALL)

if (NOT ENABLE_JEMALLOC)
    message (WARNING "Non default allocator is disabled. This is not recommended for production builds.")
endif ()

macro (add_executable target)
    # invoke built-in add_executable
    # explicitly acquire and interpose malloc symbols by rk_malloc
    # if GLIBC_COMPATIBILITY is ON and ENABLE_THINLTO is on than provide memcpy symbol explicitly to neutrialize thinlto's libcall generation.
    if (GLIBC_COMPATIBILITY AND ENABLE_THINLTO)
        _add_executable (${ARGV} $<TARGET_OBJECTS:rk_malloc> $<TARGET_OBJECTS:rk_memcpy>)
    else ()
        _add_executable (${ARGV} $<TARGET_OBJECTS:rk_malloc>)
    endif ()

    get_target_property (type ${target} TYPE)
    if (${type} STREQUAL EXECUTABLE)
        # disabled for TSAN and gcc since libtsan.a provides overrides too
        if (TARGET rk_new_delete)
            # operator::new/delete for executables (MemoryTracker stuff)
            target_link_libraries (${target} PRIVATE rk_new_delete ${MALLOC_LIBRARIES})
        endif()
    endif()
endmacro()

# Add as many warnings as possible for our own code.
include (cmake/warnings.cmake)

add_subdirectory (base)
add_subdirectory (src)
add_subdirectory (programs)
add_subdirectory (tests)

include (cmake/print_include_directories.cmake)
include (cmake/sanitize_target_link_libraries.cmake)
