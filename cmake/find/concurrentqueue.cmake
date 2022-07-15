option(ENABLE_CONCURRENTQUEUE "Enable concurrentqueue" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CONCURRENTQUEUE)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/concurrentqueue/CMakeLists.txt")
    message (WARNING "submodule contrib/concurrentqueue is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal concurrentqueue library")
    set (USE_CONCURRENTQUEUE 0)
    return()
endif ()

set (CONCURRENTQUEUE_LIBRARY concurrentqueue)
set (CONCURRENTQUEUE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/concurrentqueue")

set (USE_CONCURRENTQUEUE 1)
message (STATUS "Using concurrentqueue=${USE_CONCURRENTQUEUE}: ${CONCURRENTQUEUE_INCLUDE_DIR} : ${CONCURRENTQUEUE_LIBRARY}")
