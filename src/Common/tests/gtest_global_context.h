#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    RK::SharedContextHolder shared_context;
    RK::Context context;

    ContextHolder()
        : shared_context(RK::Context::createShared())
        , context(RK::Context::createGlobal(shared_context.get()))
    {
        context.makeGlobalContext();
        context.setPath("./");
    }

    ContextHolder(ContextHolder &&) = default;
};

inline const ContextHolder & getContext()
{
    static ContextHolder holder;
    return holder;
}
