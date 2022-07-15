set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/concurrentqueue)

set(SRCS
    ${LIBRARY_DIR}/c_api/blockingconcurrentqueue.cpp
    ${LIBRARY_DIR}/c_api/concurrentqueue.cpp
)


add_library(concurrentqueue ${SRCS})

target_include_directories (concurrentqueue SYSTEM PUBLIC ${LIBRARY_DIR})
