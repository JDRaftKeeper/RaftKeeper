set (CLICKHOUSE_SERVICE_SOURCES        
	${CMAKE_CURRENT_SOURCE_DIR}/Service.cpp
)
 
set (CLICKHOUSE_SERVICE_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io
        string_utils
        ${NURAFT_LIBRARY}
        ${LINK_RESOURCE_LIB}
    
    PUBLIC
        daemon
)

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_SERVICE_LINK PRIVATE readpassphrase)

message(STATUS "CLICKHOUSE_SERVICE_LINK = ${CLICKHOUSE_SERVICE_LINK}")
message(STATUS "CLICKHOUSE_SERVICE_SOURCES = ${CLICKHOUSE_SERVICE_SOURCES}")
message(STATUS "CLICKHOUSE_SERVICE_INCLUDE = ${CLICKHOUSE_SERVICE_INCLUDE}")
clickhouse_program_add(service)

install(FILES config.xml users.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-service COMPONENT clickhouse)

# TODO We actually need this on Mac, FreeBSD.
if (OS_LINUX)
    # Embed default config files as a resource into the binary.
    # This is needed for two purposes:
    # 1. Allow to run the binary without download of any other files.
    # 2. Allow to implement "sudo clickhouse install" tool.

    foreach(RESOURCE_FILE config.xml users.xml embedded.xml play.html)
        set(RESOURCE_OBJ ${RESOURCE_FILE}.o)
        set(RESOURCE_OBJS ${RESOURCE_OBJS} ${RESOURCE_OBJ} IServer.h)

        # https://stackoverflow.com/questions/14776463/compile-and-add-an-object-file-from-a-binary-with-cmake
        add_custom_command(OUTPUT ${RESOURCE_OBJ}
            COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && ${OBJCOPY_PATH} -I binary ${OBJCOPY_ARCH_OPTIONS} ${RESOURCE_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ}
            COMMAND ${OBJCOPY_PATH} --rename-section .data=.rodata,alloc,load,readonly,data,contents
                ${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ} ${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ})

        set_source_files_properties(${RESOURCE_OBJ} PROPERTIES EXTERNAL_OBJECT true GENERATED true)
    endforeach(RESOURCE_FILE)

    add_library(clickhouse_service_configs STATIC ${RESOURCE_OBJS})
    set_target_properties(clickhouse_service_configs PROPERTIES LINKER_LANGUAGE C)

    # whole-archive prevents symbols from being discarded for unknown reason
    # CMake can shuffle each of target_link_libraries arguments with other
    # libraries in linker command. To avoid this we hardcode whole-archive
    # library into single string.
    add_dependencies(clickhouse-service-lib clickhouse_service_configs)
endif ()