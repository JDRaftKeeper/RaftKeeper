set(RAFTKEEPER_SERVER_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/Server.cpp
        )

set(RAFTKEEPER_SERVER_LINK
        PRIVATE
        raftkeeper_common_config
        raftkeeper_common_io
        nuraft
        ${LINK_RESOURCE_LIB}

        PUBLIC
        daemon
        )

# Always use internal readpassphrase
list(APPEND RAFTKEEPER_SERVER_LINK PRIVATE readpassphrase)

raftkeeper_program_add(server)

install(FILES config.xml DESTINATION ${RAFTKEEPER_ETC_DIR}/raftkeeper-server COMPONENT raftkeeper)

# TODO We actually need this on Mac, FreeBSD.
if (OS_LINUX)
    # Embed default config files as a resource into the binary.
    # This is needed for two purposes:
    # 1. Allow to run the binary without download of any other files.
    # 2. Allow to implement "sudo raftkeeper install" tool.

    foreach (RESOURCE_FILE config.xml)
        set(RESOURCE_OBJ ${RESOURCE_FILE}.o)
        set(RESOURCE_OBJS ${RESOURCE_OBJS} ${RESOURCE_OBJ} IServer.h)

        # https://stackoverflow.com/questions/14776463/compile-and-add-an-object-file-from-a-binary-with-cmake
        add_custom_command(OUTPUT ${RESOURCE_OBJ}
                COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && ${OBJCOPY_PATH} -I binary ${OBJCOPY_ARCH_OPTIONS} ${RESOURCE_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ}
                COMMAND ${OBJCOPY_PATH} --rename-section .data=.rodata,alloc,load,readonly,data,contents
                ${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ} ${CMAKE_CURRENT_BINARY_DIR}/${RESOURCE_OBJ})

        set_source_files_properties(${RESOURCE_OBJ} PROPERTIES EXTERNAL_OBJECT true GENERATED true)
    endforeach (RESOURCE_FILE)

    add_library(raftkeeper_server_configs STATIC ${RESOURCE_OBJS})
    set_target_properties(raftkeeper_server_configs PROPERTIES LINKER_LANGUAGE C)

    # whole-archive prevents symbols from being discarded for unknown reason
    # CMake can shuffle each of target_link_libraries arguments with other
    # libraries in linker command. To avoid this we hardcode whole-archive
    # library into single string.
    add_dependencies(raftkeeper-server-lib raftkeeper_server_configs)
endif ()