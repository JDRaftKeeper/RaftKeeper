if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

macro(raftkeeper_target_link_split_lib target name)
    target_link_libraries(${target} PRIVATE raftkeeper-${name}-lib)
endmacro()

macro(raftkeeper_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(RAFTKEEPER_${name_uc}_SOURCES ${RAFTKEEPER_${name_uc}_SOURCES} PARENT_SCOPE)
    set(RAFTKEEPER_${name_uc}_LINK ${RAFTKEEPER_${name_uc}_LINK} PARENT_SCOPE)
    set(RAFTKEEPER_${name_uc}_INCLUDE ${RAFTKEEPER_${name_uc}_INCLUDE} PARENT_SCOPE)

    add_library(raftkeeper-${name}-lib ${RAFTKEEPER_${name_uc}_SOURCES})

    set(_link ${RAFTKEEPER_${name_uc}_LINK}) # can't use ${} in if()
    if(_link)
        target_link_libraries(raftkeeper-${name}-lib ${RAFTKEEPER_${name_uc}_LINK})
    endif()

    set(_include ${RAFTKEEPER_${name_uc}_INCLUDE}) # can't use ${} in if()
    if (_include)
        target_include_directories(raftkeeper-${name}-lib ${RAFTKEEPER_${name_uc}_INCLUDE})
    endif()
endmacro()

macro(raftkeeper_program_add name)
    raftkeeper_program_add_library(${name})
endmacro()

add_subdirectory (server)
add_subdirectory (converter)

add_executable (raftkeeper main.cpp)

# A library that prevent usage of several functions from libc.
if (ARCH_AMD64 AND OS_LINUX AND NOT OS_ANDROID)
    set (HARMFUL_LIB harmful)
endif ()

target_link_libraries (raftkeeper PRIVATE raftkeeper_common_io string_utils ${HARMFUL_LIB})
target_include_directories (raftkeeper PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

raftkeeper_target_link_split_lib(raftkeeper server)
raftkeeper_target_link_split_lib(raftkeeper converter)

set (RAFTKEEPER_BUNDLE)

add_custom_target (raftkeeper-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink raftkeeper raftkeeper-server DEPENDS raftkeeper)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/raftkeeper-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT raftkeeper)
list(APPEND RAFTKEEPER_BUNDLE raftkeeper-server)
add_custom_target (raftkeeper-converter ALL COMMAND ${CMAKE_COMMAND} -E create_symlink raftkeeper raftkeeper-converter DEPENDS raftkeeper)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/raftkeeper-converter DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT raftkeeper)
list(APPEND RAFTKEEPER_BUNDLE raftkeeper-converter)
#endif ()

install (TARGETS raftkeeper RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT raftkeeper)

if (USE_GDB_ADD_INDEX)
    add_custom_command(TARGET raftkeeper POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} raftkeeper COMMENT "Adding .gdb-index to raftkeeper" VERBATIM)
endif()

add_custom_target (raftkeeper-bundle ALL DEPENDS ${RAFTKEEPER_BUNDLE})

if (ENABLE_TESTS AND USE_GTEST)
    set (RAFTKEEPER_UNIT_TESTS_TARGETS unit_tests_libcommon unit_tests_dbms)
    add_custom_target (raftkeeper-tests ALL DEPENDS ${RAFTKEEPER_UNIT_TESTS_TARGETS})
    add_dependencies(raftkeeper-bundle raftkeeper-tests)
endif()
