option (USE_UNWIND "Enable libunwind (better stacktraces)" ON)

add_subdirectory(contrib/libunwind-cmake)
set (UNWIND_LIBRARIES unwind)
set (EXCEPTION_HANDLING_LIBRARY ${UNWIND_LIBRARIES})

message (STATUS "Using libunwind: ${UNWIND_LIBRARIES}")
message (STATUS "Using exception handler: ${EXCEPTION_HANDLING_LIBRARY}")
