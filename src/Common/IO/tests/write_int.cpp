#include "Common/IO/WriteBufferFromFileDescriptor.h"
#include "Common/IO/WriteHelpers.h"


/** gcc-7 generates wrong code with -O1 -finline-small-functions -ftree-vrp
  * This is compiler bug. The issue does not exist in gcc-8 or clang-8.
  */


using namespace RK;


static void NO_INLINE write(WriteBuffer & out, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        writeIntText(i, out);
        writeChar(' ', out);
    }
}


int main(int, char **)
{
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    write(out, 80);
    return 0;
}
