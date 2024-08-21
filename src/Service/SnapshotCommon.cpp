#include <Poco/File.h>

#include <Common/Exception.h>
#include <Common/IO/WriteHelpers.h>

#include <Service/KeeperUtils.h>
#include <Service/ReadBufferFromNuRaftBuffer.h>
#include <Service/SnapshotCommon.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <ZooKeeper/ZooKeeperIO.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int CORRUPTED_SNAPSHOT;
}

using nuraft::cs_new;

String toString(SnapshotVersion version)
{
    switch (version)
    {
        case SnapshotVersion::V0:
            return "v0";
        case SnapshotVersion::V1:
            return "v1";
        case SnapshotVersion::V2:
            return "v2";
        case SnapshotVersion::UNKNOWN:
            return "unknown";
    }
}


bool isSnapshotFileHeader(UInt64 magic)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'H', 'e', 'a', 'd'};
    };
    return magic == magic_num;
}

bool isSnapshotFileTail(UInt64 magic)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'T', 'a', 'i', 'l'};
    };
    return magic == magic_num;
}

int openFileForWrite(const String & path)
{
    int snap_fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (snap_fd < 0)
        throwFromErrno("Opening snapshot object " + path + " failed", ErrorCodes::CORRUPTED_SNAPSHOT);
    return snap_fd;
}

ptr<WriteBufferFromFile> openFileAndWriteHeader(const String & path, SnapshotVersion version)
{
    auto out = std::make_shared<WriteBufferFromFile>(path);
    out->write(MAGIC_SNAPSHOT_HEAD.data(), MAGIC_SNAPSHOT_HEAD.size());
    writeIntBinary(static_cast<uint8_t>(version), *out);
    return out;
}

int openFileForRead(const String & path)
{
    int snap_fd = ::open(path.c_str(), O_RDWR);
    if (snap_fd < 0)
        throwFromErrno("Opening snapshot object " + path + " failed", ErrorCodes::CORRUPTED_SNAPSHOT);
    return snap_fd;
}

void writeTailAndClose(ptr<WriteBufferFromFile> & out, UInt32 checksum)
{
    out->write(MAGIC_SNAPSHOT_TAIL.data(), MAGIC_SNAPSHOT_TAIL.size());
    writeIntBinary(checksum, *out);
    out->next();
    out->close();
}

UInt32 updateCheckSum(UInt32 checksum, UInt32 data_crc)
{
    union
    {
        UInt64 data;
        UInt32 crc[2];
    };
    crc[0] = checksum;
    crc[1] = data_crc;
    return RK::getCRC32(reinterpret_cast<const char *>(&data), 8);
}

String serializeKeeperNode(const String & path, const ptr<KeeperNode> & node, SnapshotVersion version)
{
    WriteBufferFromOwnString buf;

    Coordination::write(path, buf);
    Coordination::write(node->data, buf);

    if (version == SnapshotVersion::V0)
    {
        /// Just ignore acls for snapshot V0 which is only used in JD /// TODO delete the compatibility code
        Coordination::ACLs acls;
        Coordination::write(acls, buf);
    }
    else
        Coordination::write(node->acl_id, buf);

    Coordination::write(node->is_ephemeral, buf);
    Coordination::write(node->is_sequential, buf);
    Coordination::write(node->stat, buf);

    return std::move(buf.str());
}

ptr<KeeperNodeWithPath>parseKeeperNode(const String & buf, SnapshotVersion version)
{
    ReadBufferFromMemory in(buf.data(), buf.size());

    ptr<KeeperNodeWithPath> node_with_path = cs_new<KeeperNodeWithPath>();
    auto & node = node_with_path->node;
    node = std::make_shared<KeeperNode>();

    Coordination::read(node_with_path->path, in);
    Coordination::read(node->data, in);

    if (version == SnapshotVersion::V0)
    {
        /// Just ignore acls for snapshot V0 which is only used in JD /// TODO delete the compatibility code
        Coordination::ACLs acls;
        Coordination::read(acls, in);
    }
    else
        Coordination::read(node->acl_id, in);

    Coordination::read(node->is_ephemeral, in);
    Coordination::read(node->is_sequential, in);
    Coordination::read(node->stat, in);

    node->children.reserve(node->stat.numChildren);

    return node_with_path;
}


std::pair<size_t, UInt32> saveBatchV2(ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch)
{
    if (!batch)
        batch = cs_new<SnapshotBatchBody>();

    String str_buf = SnapshotBatchBody::serialize(*batch);

    SnapshotBatchHeader header;
    header.data_length = str_buf.size();
    header.data_crc = RK::getCRC32(str_buf.c_str(), str_buf.size());

    writeIntBinary(header.data_length, *out);
    writeIntBinary(header.data_crc, *out);

    out->write(str_buf.c_str(), header.data_length);
    out->next();

    return {SnapshotBatchHeader::HEADER_SIZE + header.data_length, header.data_crc};
}

std::pair<size_t, UInt32>
saveBatchAndUpdateCheckSumV2(ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch, UInt32 checksum)
{
    auto [save_size, data_crc] = saveBatchV2(out, batch);
    /// rebuild batch
    batch = cs_new<SnapshotBatchBody>();
    return {save_size, updateCheckSum(checksum, data_crc)};
}

void serializeAclsV2(const NumToACLMap & acl_map, String path, UInt32 save_batch_size, SnapshotVersion version)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));

    LOG_INFO(log, "Begin create snapshot acl object, acl size {}, path {}", acl_map.size(), path);

    auto out = openFileAndWriteHeader(path, version);
    ptr<SnapshotBatchBody> batch;

    uint64_t index = 0;
    UInt32 checksum = 0;

    for (const auto & acl_it : acl_map)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
                checksum = new_checksum;
            }
            batch = cs_new<SnapshotBatchBody>();
            batch->type = SnapshotBatchType::SNAPSHOT_TYPE_ACLMAP;
        }

        /// append to batch
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(acl_it.first, buf);
        Coordination::write(acl_it.second, buf);

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        batch->add(String(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last acl batch
    auto [_, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
    checksum = new_checksum;

    writeTailAndClose(out, checksum);
    LOG_INFO(log, "Finish create snapshot acl object, acl size {}, path {}", acl_map.size(), path);
}

[[maybe_unused]] size_t serializeEphemeralsV2(KeeperStore::Ephemerals & ephemerals, std::mutex & mutex, String path, UInt32 save_batch_size)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "Begin create snapshot ephemeral object, node size {}, path {}", ephemerals.size(), path);

    ptr<SnapshotBatchBody> batch;

    std::lock_guard lock(mutex);

    if (ephemerals.empty())
    {
        LOG_INFO(log, "Ephemeral nodes size is 0");
        return 0;
    }

    auto out = cs_new<WriteBufferFromFile>(path);
    uint64_t index = 0;
    for (auto & ephemeral_it : ephemerals)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                saveBatchV2(out, batch);
            }
            batch = cs_new<SnapshotBatchBody>();
            batch->type = SnapshotBatchType::SNAPSHOT_TYPE_DATA_EPHEMERAL;
        }

        /// append to batch
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(ephemeral_it.first, buf);
        Coordination::write(ephemeral_it.second.size(), buf);

        for (const auto & node_path : ephemeral_it.second)
        {
            Coordination::write(node_path, buf);
        }

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        batch->add(String(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    saveBatchV2(out, batch);
    out->close();
    return 1;
}

void serializeSessionsV2(SessionAndTimeout & session_and_timeout, SessionAndAuth & session_and_auth, UInt32 save_batch_size, SnapshotVersion version, String & path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    auto out = openFileAndWriteHeader(path, version);
    LOG_INFO(log, "Begin create snapshot session object, session size {}, path {}", session_and_timeout.size(), path);

    ptr<SnapshotBatchBody> batch;

    uint64_t index = 0;
    UInt32 checksum = 0;

    for (auto && session_it : session_and_timeout)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
                checksum = new_checksum;
            }
            batch = cs_new<SnapshotBatchBody>();
            batch->type = SnapshotBatchType::SNAPSHOT_TYPE_SESSION;
        }

        /// append to batch
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(session_it.first, buf); //NewSession
        Coordination::write(session_it.second, buf); //Timeout_ms

        Coordination::AuthIDs ids;
        if (session_and_auth.count(session_it.first))
            ids = session_and_auth.at(session_it.first);
        Coordination::write(ids, buf);

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        batch->add(String(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    auto [_, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
    checksum = new_checksum;
    writeTailAndClose(out, checksum);
}

template <typename T>
void serializeMapV2(T & snap_map, UInt32 save_batch_size, SnapshotVersion version, String & path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "Begin create snapshot map object, map size {}, path {}", snap_map.size(), path);

    auto out = openFileAndWriteHeader(path, version);
    ptr<SnapshotBatchBody> batch;

    uint64_t index = 0;
    UInt32 checksum = 0;

    for (auto & it : snap_map)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
                checksum = new_checksum;
            }

            batch = cs_new<SnapshotBatchBody>();
            if constexpr (std::is_same_v<T, StringMap>)
                batch->type = SnapshotBatchType::SNAPSHOT_TYPE_STRINGMAP;
            else if constexpr (std::is_same_v<T, IntMap>)
                batch->type = SnapshotBatchType::SNAPSHOT_TYPE_UINTMAP;
            else
                throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Only support string and int map.");
        }

        /// append to batch
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(it.first, buf);
        Coordination::write(it.second, buf);

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        batch->add(String(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    auto [_, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
    checksum = new_checksum;
    writeTailAndClose(out, checksum);
}

template void serializeMapV2<StringMap>(StringMap & snap_map, UInt32 save_batch_size, SnapshotVersion version, String & path);
template void serializeMapV2<IntMap>(IntMap & snap_map, UInt32 save_batch_size, SnapshotVersion version, String & path);

void SnapshotBatchBody::add(const String & element)
{
    elements.push_back(element);
}

size_t SnapshotBatchBody::size() const
{
    return elements.size();
}

String & SnapshotBatchBody::operator[](size_t n)
{
    return elements.at(n);
}

String SnapshotBatchBody::serialize(const SnapshotBatchBody & batch_body)
{
    WriteBufferFromOwnString buf;
    writeIntBinary(static_cast<int32_t>(batch_body.type), buf);
    writeIntBinary(static_cast<int32_t>(batch_body.elements.size()), buf);
    for (const auto & element : batch_body.elements)
    {
        writeIntBinary(static_cast<int32_t>(element.size()), buf);
        writeString(element, buf);
    }
    return std::move(buf.str());
}

ptr<SnapshotBatchBody> SnapshotBatchBody::parse(const String & data)
{
    ptr<SnapshotBatchBody> batch_body = std::make_shared<SnapshotBatchBody>();
    ReadBufferFromMemory in(data.c_str(), data.size());
    int32_t type;
    readIntBinary(type, in);
    batch_body->type = static_cast<SnapshotBatchType>(type);
    int32_t element_count;
    readIntBinary(element_count, in);
    batch_body->elements.reserve(element_count);
    for (int i = 0; i < element_count; i++)
    {
        int32_t element_size;
        readIntBinary(element_size, in);
        String element;
        element.resize(element_size);
        in.readStrict(element.data(), element_size);
        batch_body->elements.emplace_back(std::move(element));
    }
    return batch_body;
}

void parseBatchDataV2(KeeperStore & store, SnapshotBatchBody & batch, BucketEdges & buckets_edges, BucketNodes & bucket_nodes, SnapshotVersion version)
{
    for (size_t i = 0; i < batch.size(); i++)
    {
        const auto & data = batch[i];

        String path;
        ptr<KeeperNode> node;

        try
        {
            auto node_with_path = parseKeeperNode(data, version);
            path = std::move(node_with_path->path);
            node = std::move(node_with_path->node);
            assert(node);
        }
        catch (...)
        {
            throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Snapshot is corrupted, can't parse the {}th node in batch", i + 1);
        }

        if (version == SnapshotVersion::V0)
            node->acl_id = 0;

        /// Some strange ACLID during deserialization from ZooKeeper
        if (node->acl_id == std::numeric_limits<uint64_t>::max())
            node->acl_id = 0;

        store.acl_map.addUsage(node->acl_id);

        auto ephemeral_owner = node->stat.ephemeralOwner;
        if (ephemeral_owner != 0)
            store.addEphemeralNode(ephemeral_owner, path);

        if (likely(path != "/"))
        {
            auto rslash_pos = path.rfind('/');

            if (unlikely(rslash_pos < 0))
                throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Can't find parent path for path {}", path);

            auto parent_path = rslash_pos == 0 ? "/" : path.substr(0, rslash_pos);

            // Storage edges in different bucket, according to the bucket index of parent node.
            // Which allow us to insert child paths for all nodes in parallel.
            buckets_edges[store.getBucketIndex(parent_path)].emplace_back(std::move(parent_path), path.substr(rslash_pos + 1));
        }

        bucket_nodes[store.getBucketIndex(path)].emplace_back(std::move(path), std::move(node));
    }
}

void parseBatchSessionV2(KeeperStore & store, SnapshotBatchBody & batch, SnapshotVersion version)
{
    for (size_t i = 0; i < batch.size(); i++)
    {
        const String & data = batch[i];
        ReadBufferFromMemory in(data.data(), data.size());

        int64_t session_id;
        int64_t timeout;

        try
        {
            Coordination::read(session_id, in);
            Coordination::read(timeout, in);

            if (version >= SnapshotVersion::V1)
            {
                Coordination::AuthIDs ids;
                Coordination::read(ids, in);
                if (!ids.empty())
                    store.addSessionAuth(session_id, ids);
            }
        }
        catch (...)
        {
            throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Snapshot is corrupted, can't parse the {}th session in batch", i + 1);
        }
        store.addSessionID(session_id, timeout);
    }
}

void parseBatchAclMapV2(KeeperStore & store, SnapshotBatchBody & batch, SnapshotVersion version)
{
    if (version >= SnapshotVersion::V1)
    {
        for (size_t i = 0; i < batch.size(); i++)
        {
            const String & data = batch[i];
            ReadBufferFromMemory in(data.data(), data.size());

            uint64_t acl_id;
            Coordination::ACLs acls;

            try
            {
                Coordination::read(acl_id, in);
                Coordination::read(acls, in);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Snapshot is corrupted, can't parse the {}th acl in batch", i + 1);
            }

            store.addACLs(acl_id, acls);
        }
    }
}

void parseBatchIntMapV2(KeeperStore & store, SnapshotBatchBody & batch, SnapshotVersion /*version*/)
{
    IntMap int_map;
    for (size_t i = 0; i < batch.size(); i++)
    {
        const String & data = batch[i];
        ReadBufferFromMemory in(data.data(), data.size());

        String key;
        int64_t value;
        try
        {
            Coordination::read(key, in);
            Coordination::read(value, in);
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::CORRUPTED_SNAPSHOT, "Snapshot is corrupted, can't parse the {}th element of int_map in batch", i + 1);
        }
        int_map[key] = value;
    }
    if (int_map.find("ZXID") != int_map.end())
    {
        store.setZxid(int_map["ZXID"]);
    }
    if (int_map.find("SESSIONID") != int_map.end())
    {
        store.setSessionIDCounter(int_map["SESSIONID"]);
    }
}

}
