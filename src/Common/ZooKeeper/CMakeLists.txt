include(${RaftKeeper_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(raftkeeper_common_zookeeper .)

add_library(raftkeeper_common_zookeeper ${raftkeeper_common_zookeeper_headers} ${raftkeeper_common_zookeeper_sources})
#add_library(raftkeeper_common_zookeeper ${raftkeeper_common_zookeeper_headers} ${raftkeeper_common_zookeeper_sources} NuRaftKeeper.cpp NuRaftKeeper.h NuRaftCommon.cpp NuRaftCommon.h NuRaftKeeperConstants.cpp NuRaftKeeperConstants.h)
target_include_directories(raftkeeper_common_zookeeper SYSTEM PUBLIC ${gRPC_INCLUDE_DIRS} ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (raftkeeper_common_zookeeper PUBLIC raftkeeper_common_io common PRIVATE raftkeeper_service_protos)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
