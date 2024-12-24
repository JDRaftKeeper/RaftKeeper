set (SRCS
    argsToConfig.cpp
    coverage.cpp
    DateLUT.cpp
    DateLUTImpl.cpp
    demangle.cpp
        getPageSize.cpp
    getThreadId.cpp
        mremap.cpp
    phdr_cache.cpp
        shift10.cpp
    sleep.cpp
    terminalColors.cpp
    errnoToString.cpp
    getResource.cpp
    StringRef.cpp
)

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-include ${RaftKeeper_SOURCE_DIR}/base/common/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

add_library (common ${SRCS})

if (WITH_COVERAGE)
    target_compile_definitions(common PUBLIC WITH_COVERAGE=1)
else ()
    target_compile_definitions(common PUBLIC WITH_COVERAGE=0)
endif ()

target_compile_options(common PUBLIC -Wno-implicit-const-int-float-conversion -Wno-reserved-identifier -Wno-switch-default)

if (USE_INTERNAL_CCTZ)
    set_source_files_properties(DateLUTImpl.cpp PROPERTIES COMPILE_DEFINITIONS USE_INTERNAL_CCTZ)
endif()

target_include_directories(common PUBLIC .. ${CMAKE_CURRENT_BINARY_DIR}/..)

target_link_libraries (common
    PUBLIC
        ${CITYHASH_LIBRARIES}
        boost::headers_only
        boost::system
        FastMemcpy
        Poco::Net
        Poco::Util
        Poco::Foundation
        cctz
        fmt
)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
