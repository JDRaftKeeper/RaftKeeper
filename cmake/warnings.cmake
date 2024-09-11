# Our principle is to enable as many warnings as possible and always do it with "warnings as errors" flag.
#
# But it comes with some cost:
# - we have to disable some warnings in 3rd party libraries (they are located in "contrib" directory)
# - we have to include headers of these libraries as -isystem to avoid warnings from headers
#   (this is the same behaviour as if these libraries were located in /usr/include)
# - sometimes warnings from 3rd party libraries may come from macro substitutions in our code
#   and we have to wrap them with #pragma GCC/clang diagnostic ignored

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif ()

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-I${RaftKeeper_SOURCE_DIR}/base -include ${RaftKeeper_SOURCE_DIR}/src/Core/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

# Add some warnings that are not available even with -Wall -Wextra -Wpedantic.
# Intended for exploration of new compiler warnings that may be found useful.
# Applies to clang only
option (WEVERYTHING "Enable -Weverything option with some exceptions." ON)

# Control maximum size of stack frames. It can be important if the code is run in fibers with small stack size.
# Only in release build because debug has too large stack frames.
if ((NOT CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG") AND (NOT SANITIZE))
    add_warning(frame-larger-than=65536)
endif ()

add_warning(pedantic)
no_warning(vla-extension)
no_warning(zero-length-array)
no_warning(c11-extensions)

add_warning(comma)
add_warning(conditional-uninitialized)
add_warning(covered-switch-default)
add_warning(deprecated)
add_warning(embedded-directive)
add_warning(empty-init-stmt) # linux-only
add_warning(extra-semi-stmt) # linux-only
add_warning(extra-semi)
add_warning(gnu-case-range)
add_warning(inconsistent-missing-destructor-override)
add_warning(newline-eof)
add_warning(old-style-cast)
add_warning(range-loop-analysis)
add_warning(redundant-parens)
add_warning(reserved-id-macro)
add_warning(shadow-field) # clang 8+
add_warning(shadow-uncaptured-local)
add_warning(shadow)
add_warning(string-plus-int) # clang 8+
add_warning(undef)
add_warning(unreachable-code-return)
add_warning(unreachable-code)
add_warning(unused-exception-parameter)
add_warning(unused-macros)
add_warning(unused-member-function)
no_warning(unsafe-buffer-usage)
# XXX: libstdc++ has some of these for 3way compare
if (USE_LIBCXX)
    add_warning(zero-as-null-pointer-constant)
endif()

if (WEVERYTHING)
    add_warning(everything)
    no_warning(c++98-compat-pedantic)
    no_warning(c++98-compat)
    no_warning(c99-extensions)
    no_warning(conversion)
    no_warning(ctad-maybe-unsupported) # clang 9+, linux-only
    no_warning(deprecated-dynamic-exception-spec)
    no_warning(disabled-macro-expansion)
    no_warning(documentation-unknown-command)
    no_warning(double-promotion)
    no_warning(exit-time-destructors)
    no_warning(float-equal)
    no_warning(global-constructors)
    no_warning(missing-prototypes)
    no_warning(missing-variable-declarations)
    no_warning(nested-anon-types)
    no_warning(packed)
    no_warning(padded)
    no_warning(shift-sign-overflow)
    no_warning(sign-conversion)
    no_warning(switch-enum)
    no_warning(undefined-func-template)
    no_warning(unused-template)
    no_warning(vla)
    no_warning(weak-template-vtables)
    no_warning(weak-vtables)
    no_warning(thread-safety-negative) # experimental flag, too many false positives

    # XXX: libstdc++ has some of these for 3way compare
    if (NOT USE_LIBCXX)
        no_warning(zero-as-null-pointer-constant)
    endif()

    # TODO Enable conversion, sign-conversion, double-promotion warnings.
endif ()
