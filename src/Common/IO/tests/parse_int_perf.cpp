#include <iostream>
#include <iomanip>

#include <common/types.h>

#include "Common/IO/ReadHelpers.h"
#include "Common/IO/WriteHelpers.h"
#include "Common/IO/WriteIntText.h"

#include "Common/Stopwatch.h"


static UInt64 rdtsc()
{
#if defined(__x86_64__)
    UInt64 val;
    __asm__ __volatile__("rdtsc" : "=A" (val) :);
    return val;
#else
    // TODO: make for arm
    return 0;
#endif
}


int main(int argc, char ** argv)
{
    try
    {
        if (argc < 2)
        {
            std::cerr << "Usage: program n\n";
            return 1;
        }

        using T = UInt8;

        size_t n = std::stol(argv[1]);
        assert(n > 0);

        std::vector<T> data(n);
        std::vector<T> data2(n);

        {
            Stopwatch watch;

            for (size_t i = 0; i < n; ++i)
                data[i] = lrand48();// / lrand48();// ^ (lrand48() << 24) ^ (lrand48() << 48);

            watch.stop();
            std::cerr << std::fixed << std::setprecision(2)
                << "Generated " << n << " numbers (" << data.size() * sizeof(data[0]) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
                << data.size() * sizeof(data[0]) / watch.elapsedSeconds() / 1000000 << " MB/s."
                << std::endl;
        }

        std::vector<char> formatted;
        formatted.reserve(n * 21);

        {
            RK::WriteBufferFromVector wb(formatted);
        //    RK::CompressedWriteBuffer wb2(wb1);
            Stopwatch watch;

            UInt64 tsc = rdtsc();

            for (size_t i = 0; i < n; ++i)
            {
                //writeIntTextTable(data[i], wb);
                RK::writeIntText(data[i], wb);
                //RK::writeIntText(data[i], wb);
                RK::writeChar('\t', wb);
            }

            tsc = rdtsc() - tsc;

            watch.stop();
            std::cerr << std::fixed << std::setprecision(2)
                << "Written " << n << " numbers (" << wb.count() / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
                << n / watch.elapsedSeconds() << " num/s., "
                << wb.count() / watch.elapsedSeconds() / 1000000 << " MB/s., "
                << watch.elapsed() / n << " ns/num., "
                << tsc / n << " ticks/num., "
                << watch.elapsed() / wb.count() << " ns/byte., "
                << tsc / wb.count() << " ticks/byte."
                << std::endl;
        }

        {
            RK::ReadBuffer rb(formatted.data(), formatted.size(), 0);
        //    RK::CompressedReadBuffer rb(rb_);
            Stopwatch watch;

            for (size_t i = 0; i < n; ++i)
            {
                RK::readIntText(data2[i], rb);
                RK::assertChar('\t', rb);
            }

            watch.stop();
            std::cerr << std::fixed << std::setprecision(2)
                << "Read " << n << " numbers (" << rb.count() / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
                << rb.count() / watch.elapsedSeconds() / 1000000 << " MB/s."
                << std::endl;
        }

        std::cerr << (0 == memcmp(data.data(), data2.data(), data.size()) ? "Ok." : "Fail.") << std::endl;
    }
    catch (const RK::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
