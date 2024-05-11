if (NOT EXISTS "${RaftKeeper_SOURCE_DIR}/contrib/libxml2/libxml.h")
    message (WARNING "submodule contrib/libxml2 is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal libxml")
endif ()

set (LIBXML2_INCLUDE_DIR ${RaftKeeper_SOURCE_DIR}/contrib/libxml2/include ${RaftKeeper_SOURCE_DIR}/contrib/libxml2-cmake/linux_x86_64/include)
set (LIBXML2_LIBRARIES libxml2)

message (STATUS "Using libxml2: ${LIBXML2_INCLUDE_DIR} : ${LIBXML2_LIBRARIES}")
