#include <Common/IO/ReadHelpers.h>
#include <Common/IO/WriteBufferFromString.h>
#include <Common/IO/ReadBufferFromFile.h>
#include <Common/ShellCommand.h>
#include <Common/getMaxFileDescriptorCount.h>
#include <filesystem>

int getMaxFileDescriptorCount()
{
    namespace fs = std::filesystem;
    int result = -1;
#if defined(__linux__) || defined(__APPLE__)
    using namespace RK;

    if (fs::exists("/proc/sys/fs/file-max"))
    {
        ReadBufferFromFile reader("/proc/sys/fs/file-max");
        readIntText(result, reader);
    }
    else
    {
        auto command = ShellCommand::execute("ulimit -n");
        try
        {
            readIntText(result, command->out);
            command->wait();
        }
        catch (...)
        {
        }
    }

#endif

    return result;
}
