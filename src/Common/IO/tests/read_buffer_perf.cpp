#include <string>

#include <iostream>

#include "Common/IO/ReadBufferFromFile.h"
#include "Common/IO/ReadHelpers.h"
#include "common/types.h"


int main(int, char **)
{
    try
    {
        RK::ReadBufferFromFile in("test");

        RK::Int64 a = 0;
        RK::Float64 b = 0;
        RK::String c, d;

        size_t i = 0;
        while (!in.eof())
        {
            RK::readIntText(a, in);
            in.ignore();

            RK::readFloatText(b, in);
            in.ignore();

            RK::readEscapedString(c, in);
            in.ignore();

            RK::readQuotedString(d, in);
            in.ignore();

            ++i;
        }

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << i << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
