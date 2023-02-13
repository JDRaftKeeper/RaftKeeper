add_library (daemon
    BaseDaemon.cpp
)

target_include_directories (daemon PUBLIC ..)
target_link_libraries (daemon PUBLIC loggers PRIVATE raftkeeper_common_io raftkeeper_common_config common ${EXECINFO_LIBRARIES})

if (USE_SENTRY)
    target_link_libraries (daemon PRIVATE ${SENTRY_LIBRARY})
endif ()
