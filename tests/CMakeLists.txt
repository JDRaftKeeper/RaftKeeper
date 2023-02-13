enable_testing()

# Run tests with "ninja check" or "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${RaftKeeper_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

install (PROGRAMS raftkeeper-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT raftkeeper)
install (
    DIRECTORY queries performance config
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/raftkeeper-test
    USE_SOURCE_PERMISSIONS
    COMPONENT raftkeeper
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN ".gitignore" EXCLUDE
)

if (ENABLE_TEST_INTEGRATION)
    add_subdirectory (integration)
endif ()
