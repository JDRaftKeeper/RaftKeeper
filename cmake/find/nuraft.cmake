if (NOT EXISTS "${RaftKeeper_SOURCE_DIR}/contrib/NuRaft/CMakeLists.txt")
    message (WARNING "submodule contrib/NuRaft is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal NuRaft library")
    set (USE_NURAFT 0)
    return()
endif ()

#set (USE_JD_NURAFT 1)
set (NURAFT_LIBRARY nuraft)
set (NURAFT_INCLUDE_DIR "${RaftKeeper_SOURCE_DIR}/contrib/NuRaft/include")

set (USE_NURAFT 1)
message (STATUS "Using NuRaft=${USE_NURAFT}: ${NURAFT_INCLUDE_DIR} : ${NURAFT_LIBRARY}")
