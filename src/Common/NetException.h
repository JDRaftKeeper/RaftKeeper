#pragma once

#include <Common/Exception.h>


namespace RK
{

class NetException : public Exception
{
public:
    NetException(const std::string & msg, int code) : Exception(msg, code) {}

    NetException * clone() const override { return new NetException(*this); }
    void rethrow() const override { throw *this; }

private:
    const char * name() const throw() override { return "RK::NetException"; }
    const char * className() const throw() override { return "RK::NetException"; }
};

}
