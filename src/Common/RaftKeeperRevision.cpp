#include <Common/RaftKeeperRevision.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace RaftKeeperRevision
{
    unsigned getVersionInteger() { return VERSION_INTEGER; }
}
