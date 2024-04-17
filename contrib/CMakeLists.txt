# Third-party libraries may have substandard code.

# Put all targets defined here and in added subfolders under "contrib/" folder in GUI-based IDEs by default.
# Some of third-party projects may override CMAKE_FOLDER or FOLDER property of their targets, so they will
# appear not in "contrib/" as originally planned here.
get_filename_component (_current_dir_name "${CMAKE_CURRENT_LIST_DIR}" NAME)
if (CMAKE_FOLDER)
    set (CMAKE_FOLDER "${CMAKE_FOLDER}/${_current_dir_name}")
else ()
    set (CMAKE_FOLDER "${_current_dir_name}")
endif ()
unset (_current_dir_name)

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

if (WITH_COVERAGE)
  set (WITHOUT_COVERAGE_LIST ${WITHOUT_COVERAGE})
  separate_arguments(WITHOUT_COVERAGE_LIST)
  # disable coverage for contib files and build with optimisations
  if (COMPILER_CLANG)
      add_compile_options(-O3 -DNDEBUG -finline-functions -finline-hint-functions ${WITHOUT_COVERAGE_LIST})
  else()
      add_compile_options(-O3 -DNDEBUG -finline-functions ${WITHOUT_COVERAGE_LIST})
  endif()
endif()

if (SANITIZE STREQUAL "undefined")
    # 3rd-party libraries usually not intended to work with UBSan.
    add_compile_options(-fno-sanitize=undefined)
endif()

set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

add_subdirectory (boost-cmake)
add_subdirectory (cctz-cmake)
add_subdirectory (dragonbox-cmake)
add_subdirectory (FastMemcpy)
add_subdirectory (jemalloc-cmake)
add_subdirectory (poco-cmake)
add_subdirectory (double-conversion-cmake)

set(RE2_BUILD_TESTING 0 CACHE INTERNAL "")
add_subdirectory (re2)
add_subdirectory (re2_st)

add_subdirectory (cityhash102)

set (ZLIB_ENABLE_TESTS 0 CACHE INTERNAL "")
set (SKIP_INSTALL_ALL 1 CACHE INTERNAL "")
set (ZLIB_COMPAT 1 CACHE INTERNAL "") # also enables WITH_GZFILEOP
set (WITH_NATIVE_INSTRUCTIONS ${ARCH_NATIVE} CACHE INTERNAL "")
if (OS_FREEBSD OR ARCH_I386)
    set (WITH_OPTIM 0 CACHE INTERNAL "") # Bug in assembler
endif ()
if (ARCH_AARCH64)
    set(WITH_NEON 1 CACHE INTERNAL "")
    set(WITH_ACLE 1 CACHE INTERNAL "")
endif ()

message(STATUS "INTERNAL_ZLIB_NAME = ${INTERNAL_ZLIB_NAME}")
add_subdirectory (${INTERNAL_ZLIB_NAME})
# We should use same defines when including zlib.h as used when zlib compiled
target_compile_definitions (zlib PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
if (TARGET zlibstatic)
    target_compile_definitions (zlibstatic PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
endif ()
if (ARCH_AMD64 OR ARCH_AARCH64)
    target_compile_definitions (zlib PUBLIC X86_64 UNALIGNED_OK)
    if (TARGET zlibstatic)
        target_compile_definitions (zlibstatic PUBLIC X86_64 UNALIGNED_OK)
    endif ()
endif ()

add_subdirectory (boringssl-cmake)
add_library(OpenSSL::Crypto ALIAS crypto)
add_library(OpenSSL::SSL ALIAS ssl)

set(GOOGLETEST_VERSION 1.14.0) # master
add_subdirectory(${RaftKeeper_SOURCE_DIR}/contrib/googletest/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)
target_compile_definitions (gtest INTERFACE GTEST_HAS_POSIX_RE=0)

add_subdirectory(libxml2-cmake)
add_subdirectory (fmtlib-cmake)
add_subdirectory(nuraft-cmake)
add_subdirectory(fast_float)
