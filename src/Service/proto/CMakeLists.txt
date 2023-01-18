PROTOBUF_GENERATE_CPP(clickhouse_service_proto_sources clickhouse_service_proto_headers Log.proto Service.proto)

#PROTOBUF_GENERATE_GRPC_CPP
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(clickhouse_service_protos ${clickhouse_service_proto_headers} ${clickhouse_service_proto_sources})
target_include_directories(clickhouse_service_protos SYSTEM PUBLIC ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
#target_include_directories(clickhouse_service_protos SYSTEM PUBLIC ${gRPC_INCLUDE_DIRS} ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (clickhouse_service_protos PUBLIC ${gRPC_LIBRARIES})