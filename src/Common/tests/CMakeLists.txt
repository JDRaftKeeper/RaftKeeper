add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test PRIVATE raftkeeper_common_io ${CITYHASH_LIBRARIES})
if(OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries (hashes_test PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif()

add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf PRIVATE raftkeeper_common_io)

add_executable (auto_array auto_array.cpp)
target_link_libraries (auto_array PRIVATE raftkeeper_common_io)

add_executable (int_hashes_perf int_hashes_perf.cpp)
target_link_libraries (int_hashes_perf PRIVATE raftkeeper_common_io)

add_executable (simple_cache simple_cache.cpp)
target_link_libraries (simple_cache PRIVATE common)

add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array PRIVATE raftkeeper_common_io)

add_executable (pod_array pod_array.cpp)
target_link_libraries (pod_array PRIVATE raftkeeper_common_io)

add_executable (lru_hash_map_perf lru_hash_map_perf.cpp)
target_link_libraries (lru_hash_map_perf PRIVATE raftkeeper_common_io)

add_executable (thread_creation_latency thread_creation_latency.cpp)
target_link_libraries (thread_creation_latency PRIVATE raftkeeper_common_io)

add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache PRIVATE raftkeeper_common_io)

add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving PRIVATE raftkeeper_common_io)

add_executable (stopwatch stopwatch.cpp)
target_link_libraries (stopwatch PRIVATE raftkeeper_common_io)

add_executable (symbol_index symbol_index.cpp)
target_link_libraries (symbol_index PRIVATE raftkeeper_common_io)

add_executable (chaos_sanitizer chaos_sanitizer.cpp)
target_link_libraries (chaos_sanitizer PRIVATE raftkeeper_common_io)

if (OS_LINUX)
    add_executable (memory_statistics_os_perf memory_statistics_os_perf.cpp)
    target_link_libraries (memory_statistics_os_perf PRIVATE raftkeeper_common_io)
endif()

add_executable (procfs_metrics_provider_perf procfs_metrics_provider_perf.cpp)
target_link_libraries (procfs_metrics_provider_perf PRIVATE raftkeeper_common_io)

add_executable (average average.cpp)
target_link_libraries (average PRIVATE raftkeeper_common_io)

add_executable (shell_command_inout shell_command_inout.cpp)
target_link_libraries (shell_command_inout PRIVATE raftkeeper_common_io)
