set(RAFTKEEPER_CONVERTER_SOURCES RaftKeeperConverter.cpp)

set(RAFTKEEPER_CONVERTER_LINK
        PRIVATE
        boost::program_options
        rk
        )

raftkeeper_program_add(converter)
