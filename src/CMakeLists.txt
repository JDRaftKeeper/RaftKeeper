if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if(COMPILER_PIPE)
    set(MAX_COMPILER_MEMORY 2500)
else()
    set(MAX_COMPILER_MEMORY 1500)
endif()
if(MAKE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/Common/config.h)

include (../cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL}")
configure_file (Common/config.h.in ${CONFIG_COMMON})
configure_file (Common/config_version.h.in ${CONFIG_VERSION})

if (COMPILER_GCC)
    add_definitions ("-fno-tree-loop-distribute-patterns")
endif ()

add_subdirectory (Common)
add_subdirectory (Common/IO)
add_subdirectory (Service)

add_headers_and_sources(raftkeeper_common_io Common)
add_headers_and_sources(raftkeeper_common_io Common/IO)
add_headers_and_sources(raftkeeper_common_io Common/NIO)
list (REMOVE_ITEM raftkeeper_common_io_sources Common/malloc.cpp Common/new_delete.cpp)

list (APPEND raftkeeper_common_io_sources ${CONFIG_BUILD})
list (APPEND raftkeeper_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

add_library(raftkeeper_common_io ${raftkeeper_common_io_headers} ${raftkeeper_common_io_sources})
target_link_libraries(raftkeeper_common_io PUBLIC ${NURAFT_LIBRARY})

add_library (raftkeeper_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

if (((SANITIZE STREQUAL "thread") OR (SANITIZE STREQUAL "address")) AND COMPILER_GCC)
    message(WARNING "Memory tracking is disabled, due to gcc sanitizers")
else()
    add_library (raftkeeper_new_delete STATIC Common/new_delete.cpp)
    target_link_libraries (raftkeeper_new_delete PRIVATE raftkeeper_common_io jemalloc)
endif()

add_subdirectory(ZooKeeper)
add_subdirectory(Common/Config)

set (DBMS_COMMON_LIBRARIES)
# libgcc_s does not provide an implementation of an atomics library. Instead,
# GCC’s libatomic library can be used to supply these when using libgcc_s.
if ((NOT USE_LIBCXX) AND COMPILER_CLANG AND OS_LINUX)
    list (APPEND DBMS_COMMON_LIBRARIES atomic)
endif()

set(dbms_headers)
set(dbms_sources)
set (all_modules)

macro(add_object_library name common_path)
    if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
        add_headers_and_sources(dbms ${common_path})
    else ()
        list (APPEND all_modules ${name})
        add_headers_and_sources(${name} ${common_path})
        add_library(${name} SHARED ${${name}_sources} ${${name}_headers})
        target_link_libraries (${name} PRIVATE -Wl,--unresolved-symbols=ignore-all)
    endif ()
endmacro()

add_object_library(raftkeeper_service Service)

if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
    target_link_libraries (dbms PRIVATE jemalloc ${DBMS_COMMON_LIBRARIES})
    set (all_modules dbms)
endif ()

macro (dbms_target_include_directories)
    foreach (module ${all_modules})
        target_include_directories (${module} ${ARGN})
    endforeach ()
endmacro ()

macro (dbms_target_link_libraries)
    foreach (module ${all_modules})
        target_link_libraries (${module} ${ARGN})
    endforeach ()
endmacro ()

dbms_target_include_directories (PUBLIC ${RaftKeeper_SOURCE_DIR}/src ${RaftKeeper_BINARY_DIR}/src)
target_include_directories (raftkeeper_common_io PUBLIC ${RaftKeeper_SOURCE_DIR}/src ${RaftKeeper_BINARY_DIR}/src)

# Otherwise it will slow down stack traces printing too much.
set_source_files_properties(
        Common/Elf.cpp
        Common/Dwarf.cpp
        Common/SymbolIndex.cpp
        PROPERTIES COMPILE_FLAGS "-O3 ${WITHOUT_COVERAGE}")

target_link_libraries (raftkeeper_common_io
        PRIVATE
            widechar_width
            ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
            common
            ${DOUBLE_CONVERSION_LIBRARIES}
            dragonbox_to_chars
)

if(RE2_LIBRARY)
    target_link_libraries(raftkeeper_common_io PUBLIC ${RE2_LIBRARY})
endif()
if(RE2_ST_LIBRARY)
    target_link_libraries(raftkeeper_common_io PUBLIC ${RE2_ST_LIBRARY})
endif()

target_link_libraries(raftkeeper_common_io
        PRIVATE
            ${EXECINFO_LIBRARIES}
        PUBLIC
            boost::program_options
            boost::system
            ${CITYHASH_LIBRARIES}
            ${ZLIB_LIBRARIES}
            pcg_random
            Poco::Foundation
)

dbms_target_link_libraries(PUBLIC ${NURAFT_LIBRARY})


if(RE2_INCLUDE_DIR)
    target_include_directories(raftkeeper_common_io SYSTEM BEFORE PUBLIC ${RE2_INCLUDE_DIR})
endif()

dbms_target_link_libraries (
    PRIVATE
        boost::filesystem
        boost::program_options
        raftkeeper_common_config
        raftkeeper_common_zookeeper
    PUBLIC
        boost::system
        raftkeeper_common_io
)

if (OPENSSL_CRYPTO_LIBRARY)
    dbms_target_link_libraries (PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
    target_link_libraries (raftkeeper_common_io PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif ()

dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})

if (USE_PROTOBUF)
    dbms_target_link_libraries (PRIVATE ${Protobuf_LIBRARY})
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR})
endif ()

target_include_directories (raftkeeper_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

target_link_libraries (raftkeeper_common_io PUBLIC ${FAST_FLOAT_LIBRARY})
target_include_directories (raftkeeper_common_io SYSTEM BEFORE PUBLIC ${FAST_FLOAT_INCLUDE_DIR})

dbms_target_link_libraries(PRIVATE _boost_context)

if (ENABLE_TESTS AND USE_GTEST)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources(${RaftKeeper_SOURCE_DIR}/src dbms_gtest_sources)
    add_executable(unit_tests_dbms ${dbms_gtest_sources})

    # gtest framework has substandard code
    target_compile_options(unit_tests_dbms PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-undef
        -Wno-sign-compare
        -Wno-used-but-marked-unused
        -Wno-missing-noreturn
        -Wno-gnu-zero-variadic-macro-arguments
        -Wno-unused-but-set-variable
    )

    target_link_libraries(unit_tests_dbms PRIVATE ${GTEST_BOTH_LIBRARIES}
        dbms
        raftkeeper_common_zookeeper
        raftkeeper_service_protos
        loggers)
    add_check(unit_tests_dbms)
endif ()
