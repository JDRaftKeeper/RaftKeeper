add_library (daemon
    BaseDaemon.cpp
)

target_include_directories (daemon PUBLIC ..)
target_link_libraries (daemon PUBLIC loggers PRIVATE rk_common_io rk_config common)
