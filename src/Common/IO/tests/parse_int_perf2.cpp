#include <iostream>
#include <iomanip>

#include "Common/IO/ReadBufferFromFileDescriptor.h"
#include "Common/IO/ReadHelpers.h"

#include "Common/Stopwatch.h"


namespace test
{
    template <typename T>
    void readIntText(T & x, RK::ReadBuffer & buf)
    {
        bool negative = false;
        x = 0;

        if (unlikely(buf.eof()))
            RK::throwReadAfterEOF();

        if (is_signed_v<T> && *buf.position() == '-')
        {
            ++buf.position();
            negative = true;
        }

        if (*buf.position() == '0')
        {
            ++buf.position();
            return;
        }

        while (!buf.eof())
        {
            if ((*buf.position() & 0xF0) == 0x30)
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else
                break;
        }

        if (is_signed_v<T> && negative)
            x = -x;
    }
}


int main(int, char **)
{
    try
    {
        RK::ReadBufferFromFileDescriptor in(STDIN_FILENO);
        Int64 n = 0;
        size_t nums = 0;

        Stopwatch watch;

        while (!in.eof())
        {
            RK::readIntText(n, in);
            in.ignore();

            //std::cerr << "n: " << n << std::endl;

            ++nums;
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Read " << nums << " numbers (" << in.count() / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
            << nums / watch.elapsedSeconds() << " num/sec. (" << in.count() / watch.elapsedSeconds() / 1000000 << " MB/s.)"
            << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
