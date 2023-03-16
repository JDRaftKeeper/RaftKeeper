set (SRCS
    AbstractConfigurationComparison.cpp
    ConfigProcessor.cpp
    configReadClient.cpp
    ConfigReloader.cpp
)

add_library(raftkeeper_common_config ${SRCS})

target_link_libraries(raftkeeper_common_config
    PUBLIC
        raftkeeper_common_zookeeper
        common
        Poco::XML
)
