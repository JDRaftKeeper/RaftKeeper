#include <string>

#include <iostream>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/HexBinaryEncoder.h>


static void parse_trash_string_as_uint_must_fail(const std::string & str)
{
    using namespace RK;

    unsigned x = 0xFF;

    try
    {
        x = parse<unsigned>(str);
    }
    catch (...)
    {
        /// Ok
        return;
    }

    std::cerr << "Parsing must fail, but finished successfully x=" << x;
    exit(-1);
}


int main(int argc, char ** argv)
{
    parse_trash_string_as_uint_must_fail("trash");
    parse_trash_string_as_uint_must_fail("-1");

    if (argc != 2)
    {
        std::cerr << "Usage: " << std::endl
            << argv[0] << " unsigned_number" << std::endl;
        return 1;
    }

    RK::UInt64 x = RK::parse<UInt64>(argv[1]);
    Poco::HexBinaryEncoder hex(std::cout);
    RK::writeVarUInt(x, hex);
    std::cout << std::endl;

    std::string s;

    {
        RK::WriteBufferFromString wb(s);
        RK::writeVarUInt(x, wb);
        wb.next();
    }

    hex << s;
    std::cout << std::endl;

    s.clear();
    s.resize(9);

    s.resize(RK::writeVarUInt(x, s.data()) - s.data());

    hex << s;
    std::cout << std::endl;

    RK::UInt64 y = 0;

    RK::ReadBufferFromString rb(s);
    RK::readVarUInt(y, rb);

    std::cerr << "x: " << x << ", y: " << y << std::endl;

    return 0;
}
