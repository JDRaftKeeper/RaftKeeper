add_executable (zookeeper-service-copier main.cpp ${SRCS})
target_link_libraries(zookeeper-service-copier PRIVATE clickhouse_common_zookeeper clickhouse_common_io boost::program_options)
