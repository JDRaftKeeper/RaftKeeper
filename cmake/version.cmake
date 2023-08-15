include(${CMAKE_SOURCE_DIR}/version.txt)


set (VERSION_NAME "${PROJECT_NAME}")
set (VERSION_FULL "${VERSION_NAME} ${VERSION_STRING}")

math (EXPR VERSION_INTEGER "${VERSION_PATCH} + ${VERSION_MINOR}*1000 + ${VERSION_MAJOR}*1000000")

find_package(Git QUIET)
if (GIT_FOUND)
    execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else ()
    set(GIT_COMMIT_HASH "Unknown")
endif ()

find_program(DATE_CMD NAMES date)
if (DATE_CMD)
    execute_process(
            COMMAND ${DATE_CMD} "+%Y-%m-%d %H:%M:%S"
            OUTPUT_VARIABLE BUILD_TIME
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else ()
    set(BUILD_TIME, "Unknown")
endif ()
