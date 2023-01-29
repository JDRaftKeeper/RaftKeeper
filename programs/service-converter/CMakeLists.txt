set (CLICKHOUSE_SERVICE_CONVERTER_SOURCES ServiceConverter.cpp)

set (CLICKHOUSE_SERVICE_CONVERTER_LINK
    PRIVATE
        boost::program_options
        dbms
        ${Protobuf_LIBRARY}
)

clickhouse_program_add(service-converter)
