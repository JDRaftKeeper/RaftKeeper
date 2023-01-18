if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

# The `clickhouse` binary is a multi purpose tool that contains multiple execution modes (client, server, etc.),
# each of them may be built and linked as a separate library.
# If you do not know what modes you need, turn this option OFF and enable SERVER and CLIENT only.
option (ENABLE_CLICKHOUSE_ALL "Enable all ClickHouse modes by default" ON)


if (CLICKHOUSE_SPLIT_BINARY)
    option(ENABLE_CLICKHOUSE_INSTALL "Install ClickHouse without .deb/.rpm/.tgz packages (having the binary only)" OFF)
else ()
    option(ENABLE_CLICKHOUSE_INSTALL "Install ClickHouse without .deb/.rpm/.tgz packages (having the binary only)"
        ${ENABLE_CLICKHOUSE_ALL})
endif ()

option (ENABLE_CLICKHOUSE_SERVICE "Service mode (main mode)" ${ENABLE_CLICKHOUSE_ALL})

message(STATUS "ClickHouse modes:")

if (ENABLE_CLICKHOUSE_INSTALL)
    message(STATUS "ClickHouse install: ON")
else()
    message(STATUS "ClickHouse install: OFF")
endif()

if (ENABLE_CLICKHOUSE_GIT_IMPORT)
    message(STATUS "ClickHouse git-import: ON")
else()
    message(STATUS "ClickHouse git-import: OFF")
endif()

if (NOT ENABLE_CLICKHOUSE_SERVICE)
    message(WARNING "ClickHouse service mode is not going to be built.")
else()
    message(STATUS "Service mode: ON")
endif()


if(NOT (MAKE_STATIC_LIBRARIES OR SPLIT_SHARED_LIBRARIES))
    set(CLICKHOUSE_ONE_SHARED ON)
endif()

configure_file (config_tools.h.in ${ConfigIncludePath}/config_tools.h)

macro(clickhouse_target_link_split_lib target name)
    if(NOT CLICKHOUSE_ONE_SHARED)
        target_link_libraries(${target} PRIVATE clickhouse-${name}-lib)
    else()
        target_link_libraries(${target} PRIVATE clickhouse-lib)
    endif()
endmacro()

macro(clickhouse_program_link_split_binary name)
    clickhouse_target_link_split_lib(clickhouse-${name} ${name})
endmacro()

macro(clickhouse_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(CLICKHOUSE_${name_uc}_SOURCES ${CLICKHOUSE_${name_uc}_SOURCES} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_LINK ${CLICKHOUSE_${name_uc}_LINK} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_INCLUDE ${CLICKHOUSE_${name_uc}_INCLUDE} PARENT_SCOPE)

    if(NOT CLICKHOUSE_ONE_SHARED)
        add_library(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_SOURCES})

        set(_link ${CLICKHOUSE_${name_uc}_LINK}) # can't use ${} in if()
        if(_link)
            target_link_libraries(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_LINK})
        endif()

        set(_include ${CLICKHOUSE_${name_uc}_INCLUDE}) # can't use ${} in if()
        if (_include)
            target_include_directories(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_INCLUDE})
        endif()
    endif()
endmacro()

macro(clickhouse_program_add_executable name)
    if(CLICKHOUSE_SPLIT_BINARY)
        add_executable(clickhouse-${name} clickhouse-${name}.cpp)
        clickhouse_program_link_split_binary(${name})
        install(TARGETS clickhouse-${name} ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif()
endmacro()

macro(clickhouse_program_add name)
    clickhouse_program_add_library(${name})
    clickhouse_program_add_executable(${name})
endmacro()

add_subdirectory (service)
add_subdirectory (service-converter)

if (CLICKHOUSE_ONE_SHARED)
    add_library(clickhouse-lib SHARED  ${CLICKHOUSE_SERVICE_SOURCES} ${CLICKHOUSE_SERVICE_CONVERTER_SOURCES})
    target_link_libraries(clickhouse-lib ${CLICKHOUSE_SERVICE_LINK} ${CLICKHOUSE_SERVICE_CONVERTER_LINK})
    target_include_directories(clickhouse-lib ${CLICKHOUSE_SERVICE_INCLUDE} ${CLICKHOUSE_SERVICE_CONVERTER_INCLUDE})
    set_target_properties(clickhouse-lib PROPERTIES SOVERSION ${VERSION_MAJOR}.${VERSION_MINOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse DEBUG_POSTFIX "")
    install (TARGETS clickhouse-lib LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endif()

if (CLICKHOUSE_SPLIT_BINARY)
    set (CLICKHOUSE_ALL_TARGETS clickhouse-service clickhouse-service-converter)

    if (ENABLE_CLICKHOUSE_ODBC_BRIDGE)
        list (APPEND CLICKHOUSE_ALL_TARGETS clickhouse-odbc-bridge)
    endif ()

    set_target_properties(${CLICKHOUSE_ALL_TARGETS} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

    add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_ALL_TARGETS})
    add_custom_target (clickhouse ALL DEPENDS clickhouse-bundle)

    install(PROGRAMS clickhouse-split-helper DESTINATION ${CMAKE_INSTALL_BINDIR} RENAME clickhouse COMPONENT clickhouse)
else ()
    add_executable (clickhouse main.cpp)

    # A library that prevent usage of several functions from libc.
    if (ARCH_AMD64 AND OS_LINUX AND NOT OS_ANDROID)
        set (HARMFUL_LIB harmful)
    endif ()

    target_link_libraries (clickhouse PRIVATE clickhouse_common_io string_utils ${HARMFUL_LIB})
    target_include_directories (clickhouse PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

    clickhouse_target_link_split_lib(clickhouse service)

    if (ENABLE_CLICKHOUSE_INSTALL)
        clickhouse_target_link_split_lib(clickhouse install)
    endif ()

    clickhouse_target_link_split_lib(clickhouse service-converter)

    set (CLICKHOUSE_BUNDLE)

    if (ENABLE_CLICKHOUSE_SERVICE)
        add_custom_target (clickhouse-service ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-service DEPENDS clickhouse)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-service DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-service)
        add_custom_target (clickhouse-service-converter ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-service-converter DEPENDS clickhouse)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-service-converter DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-service-converter)
    endif ()

    install (TARGETS clickhouse RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)

    add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_BUNDLE})

    if (USE_GDB_ADD_INDEX)
        add_custom_command(TARGET clickhouse POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} clickhouse COMMENT "Adding .gdb-index to clickhouse" VERBATIM)
    endif()

    if (USE_BINARY_HASH)
        add_custom_command(TARGET clickhouse POST_BUILD COMMAND ./clickhouse hash-binary > hash && ${OBJCOPY_PATH} --add-section .note.ClickHouse.hash=hash clickhouse COMMENT "Adding .note.ClickHouse.hash to clickhouse" VERBATIM)
    endif()
endif ()

if (ENABLE_TESTS AND USE_GTEST)
    set (CLICKHOUSE_UNIT_TESTS_TARGETS unit_tests_libcommon unit_tests_dbms)
    add_custom_target (clickhouse-tests ALL DEPENDS ${CLICKHOUSE_UNIT_TESTS_TARGETS})
    add_dependencies(clickhouse-bundle clickhouse-tests)
endif()
