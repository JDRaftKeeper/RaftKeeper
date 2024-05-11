include(${RaftKeeper_SOURCE_DIR}/cmake/glob_sources.cmake)

add_headers_and_sources(rk_zookeeper .)

add_library(rk_zookeeper ${rk_zookeeper_headers} ${rk_zookeeper_sources})
target_include_directories(rk_zookeeper SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (rk_zookeeper PUBLIC rk_common_io common)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
