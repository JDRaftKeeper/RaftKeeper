add_executable (read_buffer read_buffer.cpp)
target_link_libraries (read_buffer PRIVATE raftkeeper_common_io)

add_executable (read_buffer_perf read_buffer_perf.cpp)
target_link_libraries (read_buffer_perf PRIVATE raftkeeper_common_io)

add_executable (write_buffer write_buffer.cpp)
target_link_libraries (write_buffer PRIVATE raftkeeper_common_io)

add_executable (write_buffer_perf write_buffer_perf.cpp)
target_link_libraries (write_buffer_perf PRIVATE raftkeeper_common_io)

add_executable (valid_utf8_perf valid_utf8_perf.cpp)
target_link_libraries (valid_utf8_perf PRIVATE raftkeeper_common_io)

add_executable (valid_utf8 valid_utf8.cpp)
target_link_libraries (valid_utf8 PRIVATE raftkeeper_common_io)

add_executable (var_uint var_uint.cpp)
target_link_libraries (var_uint PRIVATE raftkeeper_common_io)

add_executable (read_escaped_string read_escaped_string.cpp)
target_link_libraries (read_escaped_string PRIVATE raftkeeper_common_io)

add_executable (parse_int_perf parse_int_perf.cpp)
target_link_libraries (parse_int_perf PRIVATE raftkeeper_common_io)

add_executable (parse_int_perf2 parse_int_perf2.cpp)
target_link_libraries (parse_int_perf2 PRIVATE raftkeeper_common_io)

add_executable (read_write_int read_write_int.cpp)
target_link_libraries (read_write_int PRIVATE raftkeeper_common_io)

add_executable (o_direct_and_dirty_pages o_direct_and_dirty_pages.cpp)
target_link_libraries (o_direct_and_dirty_pages PRIVATE raftkeeper_common_io)

add_executable (hashing_write_buffer hashing_write_buffer.cpp)
target_link_libraries (hashing_write_buffer PRIVATE raftkeeper_common_io)
add_check(hashing_write_buffer)

add_executable (hashing_read_buffer hashing_read_buffer.cpp)
target_link_libraries (hashing_read_buffer PRIVATE raftkeeper_common_io)
add_check (hashing_read_buffer)

add_executable (io_operators io_operators.cpp)
target_link_libraries (io_operators PRIVATE raftkeeper_common_io)

add_executable (write_int write_int.cpp)
target_link_libraries (write_int PRIVATE raftkeeper_common_io)

if (OS_LINUX OR OS_FREEBSD)
    add_executable(read_buffer_aio read_buffer_aio.cpp)
    target_link_libraries (read_buffer_aio PRIVATE raftkeeper_common_io)
endif ()

add_executable (limit_read_buffer limit_read_buffer.cpp)
target_link_libraries (limit_read_buffer PRIVATE raftkeeper_common_io)

add_executable (limit_read_buffer2 limit_read_buffer2.cpp)
target_link_libraries (limit_read_buffer2 PRIVATE raftkeeper_common_io)

add_executable (parse_date_time_best_effort parse_date_time_best_effort.cpp)
target_link_libraries (parse_date_time_best_effort PRIVATE raftkeeper_common_io)

add_executable (zlib_ng_bug zlib_ng_bug.cpp)
target_link_libraries (zlib_ng_bug PRIVATE ${ZLIB_LIBRARIES})

add_executable (dragonbox_test dragonbox_test.cpp)
target_link_libraries (dragonbox_test PRIVATE dragonbox_to_chars)
