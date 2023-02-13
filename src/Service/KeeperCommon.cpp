#include <iostream>
#include <Service/KeeperCommon.h>
#include <libnuraft/nuraft.hxx>
#include <sys/stat.h>
#include <sys/types.h>
#include <Poco/File.h>

using namespace nuraft;

namespace RK
{
int Directory::createDir(const std::string & dir)
{
    Poco::File(dir).createDirectories();
    return 0;
}
}
