#include <IO/Operators.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>


int main(int, char **)
{
    {
        RK::WriteBufferFromFileDescriptor buf(STDOUT_FILENO);
        buf
            << "Hello, world!" << '\n'
            << RK::escape << "Hello, world!" << '\n'
            << RK::quote << "Hello, world!" << '\n'
            << RK::double_quote << "Hello, world!" << '\n'
            << RK::binary << "Hello, world!" << '\n'
            << LocalDateTime(time(nullptr)) << '\n'
            << LocalDate(time(nullptr)) << '\n'
            << 1234567890123456789UL << '\n'
            << RK::flush;
    }

    {
        std::string hello;
        {
            RK::WriteBufferFromString buf(hello);
            buf << "Hello";
            std::cerr << hello.size() << '\n';
        }

        std::cerr << hello.size() << '\n';
        std::cerr << hello << '\n';
    }

    {
        RK::WriteBufferFromFileDescriptor buf(STDOUT_FILENO);
        size_t x = 11;
        buf << "Column " << x << ", \n";
    }

    return 0;
}
