#pragma once

#include <Functions/registerFunctions.h>
#include <Formats/registerFormats.h>


inline void tryRegisterFunctions()
{
    static struct Register { Register() { RK::registerFunctions(); } } registered;
}

inline void tryRegisterFormats()
{
    static struct Register { Register() { RK::registerFormats(); } } registered;
}
