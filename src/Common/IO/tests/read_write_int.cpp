#include <string>

#include <iostream>

#include "Common/IO/ReadBufferFromString.h"
#include "Common/IO/ReadHelpers.h"
#include "Common/IO/WriteBufferFromString.h"
#include "Common/IO/WriteHelpers.h"
#include <common/types.h>


int main(int, char **)
{
    try
    {
        Int64 x1 = std::numeric_limits<Int64>::min();
        Int64 x2 = 0;
        std::string s;

        std::cerr << static_cast<Int64>(x1) << std::endl;

        {
            RK::WriteBufferFromString wb(s);
            RK::writeIntText(x1, wb);
        }

        std::cerr << s << std::endl;

        {
            RK::ReadBufferFromString rb(s);
            RK::readIntText(x2, rb);
        }

        std::cerr << static_cast<Int64>(x2) << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
