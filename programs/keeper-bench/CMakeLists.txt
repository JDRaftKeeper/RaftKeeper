set(RAFTKEEPER_KEEPER_BENCH_SOURCES
        Runner.cpp
)

set(RAFTKEEPER_KEEPER_BENCH_LINK
        PRIVATE
        boost::program_options
        rk
)

raftkeeper_program_add(keeper-bench)
