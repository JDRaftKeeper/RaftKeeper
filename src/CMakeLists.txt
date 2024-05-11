if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

include(../cmake/limit_jobs.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/Common/config.h)

include (../cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} ${GIT_COMMIT_HASH}")
configure_file (Common/config.h.in ${CONFIG_COMMON})
configure_file (Common/config_version.h.in ${CONFIG_VERSION})

if (COMPILER_GCC)
    add_definitions ("-fno-tree-loop-distribute-patterns")
endif ()

add_subdirectory (Common)
add_subdirectory (NIO)
add_subdirectory (Common/IO)
add_subdirectory (Service)

add_headers_and_sources(rk_common_io Common)
add_headers_and_sources(rk_common_io Common/IO)
list (REMOVE_ITEM rk_common_io_sources Common/malloc.cpp Common/new_delete.cpp)

list (APPEND rk_common_io_sources ${CONFIG_BUILD})
list (APPEND rk_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

add_library(rk_common_io ${rk_common_io_headers} ${rk_common_io_sources})
target_link_libraries(rk_common_io PUBLIC ${NURAFT_LIBRARY})

add_library (rk_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

if (((SANITIZE STREQUAL "thread") OR (SANITIZE STREQUAL "address")) AND COMPILER_GCC)
    message(WARNING "Memory tracking is disabled, due to gcc sanitizers")
else()
    add_library (rk_new_delete STATIC Common/new_delete.cpp)
    target_link_libraries (rk_new_delete PRIVATE rk_common_io jemalloc)
endif()

add_subdirectory(ZooKeeper)
add_subdirectory(Common/Config)

set (RK_COMMON_LIBRARIES)
# libgcc_s does not provide an implementation of an atomics library. Instead,
# GCC’s libatomic library can be used to supply these when using libgcc_s.
if ((NOT USE_LIBCXX) AND COMPILER_CLANG AND OS_LINUX)
    list (APPEND RK_COMMON_LIBRARIES atomic)
endif()

set(rk_headers)
set(rk_sources)

add_headers_and_sources(rk Service)
add_headers_and_sources(rk NIO)

add_library (rk STATIC ${rk_headers} ${rk_sources})
target_link_libraries (rk PRIVATE jemalloc ${RK_COMMON_LIBRARIES})

target_include_directories (rk PUBLIC ${RaftKeeper_SOURCE_DIR}/src ${RaftKeeper_BINARY_DIR}/src)
target_include_directories (rk_common_io PUBLIC ${RaftKeeper_SOURCE_DIR}/src ${RaftKeeper_BINARY_DIR}/src)

# Otherwise it will slow down stack traces printing too much.
set_source_files_properties(
        Common/Elf.cpp
        Common/Dwarf.cpp
        Common/SymbolIndex.cpp
        PROPERTIES COMPILE_FLAGS "-O3 ${WITHOUT_COVERAGE}")

target_link_libraries (rk_common_io
        PRIVATE
            widechar_width
            ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
            common
            ${DOUBLE_CONVERSION_LIBRARIES}
            dragonbox_to_chars
)

target_link_libraries(rk_common_io
        PUBLIC
            boost::program_options
            boost::system
            ${CITYHASH_LIBRARIES}
            pcg_random
            Poco::Foundation
)

target_link_libraries(rk PUBLIC ${NURAFT_LIBRARY})


if(RE2_INCLUDE_DIR)
    target_include_directories(rk_common_io SYSTEM BEFORE PUBLIC ${RE2_INCLUDE_DIR})
endif()

target_link_libraries (rk
    PRIVATE
        boost::filesystem
        boost::program_options
        rk_config
        rk_zookeeper
    PUBLIC
        boost::system
        rk_common_io
)

if (OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries (rk PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
    target_link_libraries (rk_common_io PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif ()

target_include_directories (rk SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_include_directories (rk_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

target_link_libraries(rk PRIVATE _boost_context)

if (ENABLE_TESTS AND USE_GTEST)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all gtest sources
    grep_gtest_sources(${RaftKeeper_SOURCE_DIR}/src rk_gtest_sources)
    add_executable(rk_unit_tests ${rk_gtest_sources} ${RaftKeeper_SOURCE_DIR}/src/Service/tests/raft_test_common.cpp)

    # gtest framework has substandard code
    target_compile_options(rk_unit_tests PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-undef
        -Wno-sign-compare
        -Wno-used-but-marked-unused
        -Wno-missing-noreturn
        -Wno-gnu-zero-variadic-macro-arguments
        -Wno-unused-but-set-variable
    )

    target_link_libraries(rk_unit_tests PRIVATE ${GTEST_BOTH_LIBRARIES}
        rk
        rk_zookeeper
        loggers)
    add_check(rk_unit_tests)
endif ()
