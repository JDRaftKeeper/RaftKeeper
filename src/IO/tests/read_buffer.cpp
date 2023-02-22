#include <string>

#include <iostream>

#include <common/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>


int main(int, char **)
{
    try
    {
        std::string s = "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
        RK::ReadBufferFromString in(s);

        RK::Int64 a;
        RK::Float64 b;
        RK::String c, d;

        RK::readIntText(a, in);
        in.ignore();

        RK::readFloatText(b, in);
        in.ignore();

        RK::readEscapedString(c, in);
        in.ignore();

        RK::readQuotedString(d, in);

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << in.count() << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
