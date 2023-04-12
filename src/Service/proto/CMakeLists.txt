PROTOBUF_GENERATE_CPP(raftkeeper_service_proto_sources raftkeeper_service_proto_headers Log.proto)

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(raftkeeper_service_protos ${raftkeeper_service_proto_headers} ${raftkeeper_service_proto_sources})
target_link_libraries(raftkeeper_service_protos ${Protobuf_LIBRARY})
target_include_directories(raftkeeper_service_protos SYSTEM PUBLIC ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
