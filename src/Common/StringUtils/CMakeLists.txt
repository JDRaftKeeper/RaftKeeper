# These files are located in separate library, because they are used by Yandex.Metrika code
#  in places when no dependency on whole "dbms" library is possible.

include(${RaftKeeper_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(raftkeeper_common_stringutils .)

add_library(string_utils ${raftkeeper_common_stringutils_headers} ${raftkeeper_common_stringutils_sources})
