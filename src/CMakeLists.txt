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
message (STATUS "Will build ${VERSION_FULL} revision ${VERSION_REVISION} ${VERSION_OFFICIAL}")
configure_file (Common/config.h.in ${CONFIG_COMMON})
configure_file (Common/config_version.h.in ${CONFIG_VERSION})
configure_file (Core/config_core.h.in ${CMAKE_CURRENT_BINARY_DIR}/Core/include/config_core.h)

if (COMPILER_GCC)
    # If we leave this optimization enabled, gcc-7 replaces a pair of SSE intrinsics (16 byte load, store) with a call to memcpy.
    # It leads to slow code. This is compiler bug. It looks like this:
    #
    # (gdb) bt
    #0  memcpy (destination=0x7faa6e9f1638, source=0x7faa81d9e9a8, size=16) at ../libs/libmemcpy/memcpy.h:11
    #1  0x0000000005341c5f in _mm_storeu_si128 (__B=..., __P=<optimized out>) at /usr/lib/gcc/x86_64-linux-gnu/7/include/emmintrin.h:720
    #2  memcpySmallAllowReadWriteOverflow15Impl (n=<optimized out>, src=<optimized out>, dst=<optimized out>) at ../src/Common/memcpySmall.h:37

    add_definitions ("-fno-tree-loop-distribute-patterns")
endif ()

add_subdirectory (Common)
add_subdirectory (Core)
add_subdirectory (IO)
add_subdirectory (Service)

set(dbms_headers)
set(dbms_sources Core/Defines.h Core/Context.cpp Core/Context.h Common/Names.h)

add_headers_and_sources(clickhouse_common_io Common)
add_headers_and_sources(clickhouse_common_io IO)
list (REMOVE_ITEM clickhouse_common_io_sources Common/malloc.cpp Common/new_delete.cpp)

list (APPEND clickhouse_common_io_sources ${CONFIG_BUILD})
list (APPEND clickhouse_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

if (NOT ENABLE_SSL)
    list (REMOVE_ITEM clickhouse_common_io_sources Common/OpenSSLHelpers.cpp)
    list (REMOVE_ITEM clickhouse_common_io_headers Common/OpenSSLHelpers.h)
endif ()

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

if (SPLIT_SHARED_LIBRARIES)
    target_compile_definitions(clickhouse_common_io PRIVATE SPLIT_SHARED_LIBRARIES)
endif ()

add_library (clickhouse_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

if (((SANITIZE STREQUAL "thread") OR (SANITIZE STREQUAL "address")) AND COMPILER_GCC)
    message(WARNING "Memory tracking is disabled, due to gcc sanitizers")
else()
    add_library (clickhouse_new_delete STATIC Common/new_delete.cpp)
    target_link_libraries (clickhouse_new_delete PRIVATE clickhouse_common_io jemalloc)
endif()

add_subdirectory(Common/ZooKeeper)
add_subdirectory(Common/Config)

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

add_object_library(clickhouse_core Core)
add_object_library(clickhouse_service Service)

set (DBMS_COMMON_LIBRARIES)
# libgcc_s does not provide an implementation of an atomics library. Instead,
# GCC’s libatomic library can be used to supply these when using libgcc_s.
if ((NOT USE_LIBCXX) AND COMPILER_CLANG AND OS_LINUX)
    list (APPEND DBMS_COMMON_LIBRARIES atomic)
endif()

if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    message(STATUS "MAKE_STATIC_LIBRARIES = true, dbms_headers : ${dbms_headers} , dbms_sources : ${dbms_sources}")
    add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
    message(STATUS "DBMS_COMMON_LIBRARIES = ${DBMS_COMMON_LIBRARIES}")
    target_link_libraries (dbms PRIVATE jemalloc libdivide ${DBMS_COMMON_LIBRARIES})
    set (all_modules dbms)
    message(STATUS "all_modules ${all_modules}")
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

dbms_target_include_directories (PUBLIC ${ClickHouse_SOURCE_DIR}/src ${ClickHouse_BINARY_DIR}/src)
target_include_directories (clickhouse_common_io PUBLIC ${ClickHouse_SOURCE_DIR}/src ${ClickHouse_BINARY_DIR}/src)

if (USE_EMBEDDED_COMPILER)
    dbms_target_link_libraries (PRIVATE ${REQUIRED_LLVM_LIBRARIES})
    dbms_target_include_directories (SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()

#if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
#    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
#    set_source_files_properties(
#        Dictionaries/FlatDictionary.cpp
#        Dictionaries/HashedDictionary.cpp
#        Dictionaries/CacheDictionary.cpp
#        Dictionaries/IPAddressDictionary.cpp
#        Dictionaries/RangeHashedDictionary.cpp
#        Dictionaries/ComplexKeyHashedDictionary.cpp
#        Dictionaries/ComplexKeyCacheDictionary.cpp
#        Dictionaries/ComplexKeyCacheDictionary_generate1.cpp
#        Dictionaries/ComplexKeyCacheDictionary_generate2.cpp
#        Dictionaries/ComplexKeyCacheDictionary_generate3.cpp
#        Dictionaries/ODBCBlockInputStream.cpp
#        Dictionaries/HTTPDictionarySource.cpp
#        Dictionaries/LibraryDictionarySource.cpp
#        Dictionaries/ExecutableDictionarySource.cpp
#        Dictionaries/ClickHouseDictionarySource.cpp
#        PROPERTIES COMPILE_FLAGS -g0)
#endif ()

# Otherwise it will slow down stack traces printing too much.
set_source_files_properties(
        Common/Elf.cpp
        Common/Dwarf.cpp
        Common/SymbolIndex.cpp
        PROPERTIES COMPILE_FLAGS "-O3 ${WITHOUT_COVERAGE}")

target_link_libraries (clickhouse_common_io
        PRIVATE
            string_utils
            widechar_width
            ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
            common
            ${DOUBLE_CONVERSION_LIBRARIES}
            dragonbox_to_chars
)

if(RE2_LIBRARY)
    target_link_libraries(clickhouse_common_io PUBLIC ${RE2_LIBRARY})
endif()
if(RE2_ST_LIBRARY)
    target_link_libraries(clickhouse_common_io PUBLIC ${RE2_ST_LIBRARY})
endif()

target_link_libraries(clickhouse_common_io
        PRIVATE
            ${EXECINFO_LIBRARIES}
            cpuid
        PUBLIC
            boost::program_options
            boost::system
            ${CITYHASH_LIBRARIES}
            ${ZLIB_LIBRARIES}
            pcg_random
            Poco::Foundation
            roaring
)



#if (USE_RDKAFKA)
#    dbms_target_link_libraries(PRIVATE ${CPPKAFKA_LIBRARY} ${RDKAFKA_LIBRARY})
#    if(NOT USE_INTERNAL_RDKAFKA_LIBRARY)
#        dbms_target_include_directories(SYSTEM BEFORE PRIVATE ${RDKAFKA_INCLUDE_DIR})
#    endif()
#endif()

if (USE_AMQPCPP)
    dbms_target_link_libraries(PUBLIC amqp-cpp)
endif()

if (USE_CYRUS_SASL)
    dbms_target_link_libraries(PRIVATE ${CYRUS_SASL_LIBRARY})
endif()

if (USE_KRB5)
    dbms_target_link_libraries(PRIVATE ${KRB5_LIBRARY})
endif()

message(STATUS "NURAFT_LIBRARY ${NURAFT_LIBRARY} ${NURAFT_INCLUDE_DIR}")
#include_directories(/data1/home/wujianchao/project/jd/clickhouse/contrib/NuRaft/include)
dbms_target_link_libraries(PUBLIC ${NURAFT_LIBRARY})
#target_link_libraries (dbms PUBLIC ${NURAFT_LIBRARY})
#dbms_target_include_directories(PUBLIC ${NURAFT_INCLUDE_DIR})

get_target_property(DBMS_INCLUDE_DIRECTORIES dbms INCLUDE_DIRECTORIES)
get_target_property(DBMS_LINK_LIBRARIES dbms LINK_LIBRARIES)
message(STATUS "DBMS_INCLUDE_DIRECTORIES : ${DBMS_INCLUDE_DIRECTORIES}")
message(STATUS "DBMS_LINK_LIBRARIES : ${DBMS_LINK_LIBRARIES}")

get_target_property(DBMS_INTERFACE_INCLUDE_DIRECTORIES dbms INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(DBMS_INTERFACE_LINK_LIBRARIES dbms INTERFACE_LINK_LIBRARIES)
message(STATUS "DBMS_INTERFACE_INCLUDE_DIRECTORIES : ${DBMS_INTERFACE_INCLUDE_DIRECTORIES}")
message(STATUS "DBMS_INTERFACE_LINK_LIBRARIES : ${DBMS_INTERFACE_LINK_LIBRARIES}")

get_target_property(RAFT_INCLUDE_DIRECTORIES ${NURAFT_LIBRARY} INCLUDE_DIRECTORIES)
get_target_property(RAFT_LINK_LIBRARIES ${NURAFT_LIBRARY} LINK_LIBRARIES)
message(STATUS "RAFT_INCLUDE_DIRECTORIES : ${RAFT_INCLUDE_DIRECTORIES}")
message(STATUS "RAFT_LINK_LIBRARIES : ${RAFT_LINK_LIBRARIES}")

get_target_property(RAFT_INTERFACE_INCLUDE_DIRECTORIES ${NURAFT_LIBRARY} INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(RAFT_INTERFACE_LINK_LIBRARIES ${NURAFT_LIBRARY} INTERFACE_LINK_LIBRARIES)
message(STATUS "RAFT_INTERFACE_INCLUDE_DIRECTORIES : ${RAFT_INTERFACE_INCLUDE_DIRECTORIES}")
message(STATUS "RAFT_INTERFACE_LINK_LIBRARIES : ${RAFT_INTERFACE_LINK_LIBRARIES}")

if (USE_CONCURRENTQUEUE)
    dbms_target_link_libraries(PUBLIC ${CONCURRENTQUEUE_LIBRARY})
endif()


if(RE2_INCLUDE_DIR)
    target_include_directories(clickhouse_common_io SYSTEM BEFORE PUBLIC ${RE2_INCLUDE_DIR})
endif()

dbms_target_link_libraries (
    PRIVATE
        boost::filesystem
        boost::program_options
        clickhouse_common_config
        clickhouse_common_zookeeper
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        lz4
        Poco::JSON
        Poco::MongoDB
        string_utils
    PUBLIC
        ${MYSQLXX_LIBRARY}
        boost::system
        clickhouse_common_io
)

target_include_directories(clickhouse_common_io PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/Core/include) # uses some includes from core
dbms_target_include_directories(PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/Core/include)

dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${PDQSORT_INCLUDE_DIR})
dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${MINISELECT_INCLUDE_DIR})

if (ZSTD_LIBRARY)
    dbms_target_link_libraries(PRIVATE ${ZSTD_LIBRARY})
    target_link_libraries (clickhouse_common_io PUBLIC ${ZSTD_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${ZSTD_INCLUDE_DIR})
    if (NOT USE_INTERNAL_ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
        dbms_target_include_directories(SYSTEM BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
    endif ()
endif()

set (LZMA_LIBRARY liblzma)
set (LZMA_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/xz/src/liblzma/api)
if (LZMA_LIBRARY)
    target_link_libraries (clickhouse_common_io PUBLIC ${LZMA_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${LZMA_INCLUDE_DIR})
endif()

if (USE_ICU)
    dbms_target_link_libraries (PRIVATE ${ICU_LIBRARIES})
    dbms_target_include_directories (SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_CAPNP)
    dbms_target_link_libraries (PRIVATE ${CAPNP_LIBRARIES})
endif ()

if (USE_PARQUET)
    dbms_target_link_libraries(PRIVATE ${PARQUET_LIBRARY})
    if (NOT USE_INTERNAL_PARQUET_LIBRARY)
        dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})
        if (USE_STATIC_LIBRARIES)
            dbms_target_link_libraries(PRIVATE ${ARROW_LIBRARY})
        endif()
    endif ()
endif ()

if (USE_AVRO)
    dbms_target_link_libraries(PRIVATE ${AVROCPP_LIBRARY})
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${AVROCPP_INCLUDE_DIR})
endif ()

if (OPENSSL_CRYPTO_LIBRARY)
    dbms_target_link_libraries (PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
    target_link_libraries (clickhouse_common_io PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif ()

if (USE_LDAP)
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${OPENLDAP_INCLUDE_DIRS})
    dbms_target_link_libraries (PRIVATE ${OPENLDAP_LIBRARIES})
endif ()
dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})

if (USE_PROTOBUF)
    dbms_target_link_libraries (PRIVATE ${Protobuf_LIBRARY})
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR})
endif ()

if (USE_GRPC)
    dbms_target_link_libraries (PUBLIC clickhouse_grpc_protos)
endif()

if (USE_BROTLI)
    target_link_libraries (clickhouse_common_io PRIVATE ${BROTLI_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PRIVATE ${BROTLI_INCLUDE_DIR})
endif()

target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

if (USE_MSGPACK)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${MSGPACK_INCLUDE_DIR})
endif()

target_link_libraries (clickhouse_common_io PUBLIC ${FAST_FLOAT_LIBRARY})
target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${FAST_FLOAT_INCLUDE_DIR})

if (USE_LIBPQXX)
    dbms_target_link_libraries(PUBLIC ${LIBPQXX_LIBRARY})
    dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${LIBPQXX_INCLUDE_DIR})
endif()

dbms_target_link_libraries(PRIVATE _boost_context)

if (ENABLE_TESTS AND USE_GTEST)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources(${ClickHouse_SOURCE_DIR}/src dbms_gtest_sources)
    add_executable(unit_tests_dbms ${dbms_gtest_sources})

    # gtest framework has substandard code
    target_compile_options(unit_tests_dbms PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-undef
        -Wno-sign-compare
        -Wno-used-but-marked-unused
        -Wno-missing-noreturn
        -Wno-gnu-zero-variadic-macro-arguments
    )

    target_link_libraries(unit_tests_dbms PRIVATE ${GTEST_BOTH_LIBRARIES}
        dbms
        clickhouse_common_zookeeper
        string_utils
        clickhouse_service_protos
        loggers)
    add_check(unit_tests_dbms)
endif ()
