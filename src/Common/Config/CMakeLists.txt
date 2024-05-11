set (SRCS
    AbstractConfigurationComparison.cpp
    ConfigProcessor.cpp
    configReadClient.cpp
    ConfigReloader.cpp
)

add_library(rk_config ${SRCS})

target_link_libraries(rk_config
    PUBLIC
        rk_zookeeper
        common
        Poco::XML
)
