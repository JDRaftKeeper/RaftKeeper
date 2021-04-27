#include <iostream>
#include <Service/NuRaftCommon.h>
#include <libnuraft/nuraft.hxx>
#include <sys/stat.h>
#include <sys/types.h>
#include <Poco/File.h>

using namespace nuraft;

namespace DB
{
int Directory::createDir(const std::string & dir)
{
    Poco::File(dir).createDirectories();
    return 0;
}
}
