#include <string>

#include <iostream>
#include <fstream>

#include <common/types.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>


int main(int, char **)
{
    try
    {
        RK::Int64 a = -123456;
        RK::Float64 b = 123.456;
        RK::String c = "вася пе\tтя";
        RK::String d = "'xyz\\";

        std::ofstream s("test");
        RK::WriteBufferFromOStream out(s);

        for (int i = 0; i < 1000000; ++i)
        {
            RK::writeIntText(a, out);
            RK::writeChar(' ', out);

            RK::writeFloatText(b, out);
            RK::writeChar(' ', out);

            RK::writeEscapedString(c, out);
            RK::writeChar('\t', out);

            RK::writeQuotedString(d, out);
            RK::writeChar('\n', out);
        }
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
