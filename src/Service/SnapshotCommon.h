#pragma once

#include <map>
#include <string>

#include <Common/IO/WriteBufferFromFile.h>
#include <libnuraft/nuraft.hxx>

#include <Service/KeeperStore.h>
#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>
#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#    pragma clang diagnostic ignored "-Wheader-hygiene"
#endif
#include <Service/proto/Log.pb.h>
#ifdef __clang__
#    pragma clang diagnostic pop
#endif
#include <ZooKeeper/IKeeper.h>


namespace RK
{

const String MAGIC_SNAPSHOT_TAIL = "SnapTail";
const String MAGIC_SNAPSHOT_HEAD = "SnapHead";

/// 0.3KB / Node * 100M Count =  300MB
static constexpr UInt32 MAX_OBJECT_NODE_SIZE = 1000000;
/// 100M Count / 10K = 10K
static constexpr UInt32 SAVE_BATCH_SIZE = 10000;

using StringMap = std::unordered_map<String, String>;
using IntMap = std::unordered_map<String, int64_t>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// Add ACL map
    V2 = 2, /// Replace protobuf
    V3 = 3, /// Add last_log_term to file name
    None = 255,
};

String toString(SnapshotVersion version);


static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V2;

struct SnapshotBatchHeader
{
    /// The length of the batch data (uncompressed)
    UInt32 data_length;
    /// The CRC32C of the batch data.
    /// If compression is enabled, this is the checksum of the compressed data.
    UInt32 data_crc;
    void reset()
    {
        data_length = 0;
        data_crc = 0;
    }
    static const size_t HEADER_SIZE = 8;
};

enum class SnapshotBatchType : int
{
    SNAPSHOT_TYPE_DATA = 0,
    SNAPSHOT_TYPE_DATA_EPHEMERAL = 1,
    SNAPSHOT_TYPE_CONFIG = 2,
    SNAPSHOT_TYPE_SERVER = 3,
    SNAPSHOT_TYPE_SESSION = 4,
    SNAPSHOT_TYPE_STRINGMAP = 5,
    SNAPSHOT_TYPE_UINTMAP = 6,
    SNAPSHOT_TYPE_ACLMAP = 7
};

struct SnapshotBatchBody
{
    SnapshotBatchType type;
    /// element count
    int32_t count;
    std::vector<String> elements;

    void add(const String & element);
    size_t size() const;
    String & operator[](size_t n);

    static String serialize(const SnapshotBatchBody & batch_body);
    static ptr<SnapshotBatchBody> parse(const String & data);
};

int openFileForWrite(const String & obj_path);
int openFileForRead(String & obj_path);

/// snapshot object file header
bool isFileHeader(UInt64 magic);

/// snapshot object file tail
bool isFileTail(UInt64 magic);

std::shared_ptr<WriteBufferFromFile> openFileAndWriteHeader(const String & path, const SnapshotVersion version);
void writeTailAndClose(std::shared_ptr<WriteBufferFromFile> & out, UInt32 checksum);

UInt32 updateCheckSum(UInt32 checksum, UInt32 data_crc);

/// ----- For snapshot version 1 ----- // TODO delete

/// save batch data in snapshot object
std::pair<size_t, UInt32> saveBatch(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchPB> & batch);
std::pair<size_t, UInt32>
saveBatchAndUpdateCheckSum(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchPB> & batch, UInt32 checksum);

void serializeAcls(ACLMap & acls, String path, UInt32 save_batch_size, SnapshotVersion version);
[[maybe_unused]] size_t serializeEphemerals(KeeperStore::Ephemerals & ephemerals, std::mutex & mutex, String path, UInt32 save_batch_size);

/// Serialize sessions and return the next_session_id before serialize
int64_t serializeSessions(KeeperStore & store, UInt32 save_batch_size, SnapshotVersion version, String & path);

/// Save map<string, string> or map<string, uint64>
template <typename T>
void serializeMap(T & snap_map, UInt32 save_batch_size, SnapshotVersion version, String & path);


/// ----- For snapshot version 2 -----

/// save batch data in snapshot object
std::pair<size_t, UInt32> saveBatchV2(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch);
std::pair<size_t, UInt32>
saveBatchAndUpdateCheckSumV2(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch, UInt32 checksum);

void serializeAclsV2(ACLMap & acls, String path, UInt32 save_batch_size, SnapshotVersion version);
[[maybe_unused]] size_t
serializeEphemeralsV2(KeeperStore::Ephemerals & ephemerals, std::mutex & mutex, String path, UInt32 save_batch_size);

/// Serialize sessions and return the next_session_id before serialize
int64_t serializeSessionsV2(KeeperStore & store, UInt32 save_batch_size, SnapshotVersion version, String & path);

/// Save map<string, string> or map<string, uint64>
template <typename T>
void serializeMapV2(T & snap_map, UInt32 save_batch_size, SnapshotVersion version, String & path);

}
