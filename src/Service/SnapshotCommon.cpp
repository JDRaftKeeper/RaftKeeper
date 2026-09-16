#include <Poco/File.h>

#include <Common/Exception.h>
#include <Common/IO/WriteHelpers.h>

#include <Service/Crc32.h>
#include <Service/KeeperUtils.h>
#include <Service/ReadBufferFromNuRaftBuffer.h>
#include <Service/SnapshotCommon.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <Service/ZstdLogCodec.h>
#include <ZooKeeper/ZooKeeperIO.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int CORRUPTED_SNAPSHOT;
    extern const int UNKNOWN_FORMAT_VERSION;
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
        case SnapshotVersion::V3:
            return "v3";
        case SnapshotVersion::V4:
            return "v4";
        case SnapshotVersion::UNKNOWN:
            return "unknown";
    }
}

void SnapshotFormat::validate() const
{
    if (version > MAX_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", static_cast<uint8_t>(version));
    if (codec != SnapshotCodec::None && codec != SnapshotCodec::Zstd)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot codec {}", static_cast<uint8_t>(codec));
    if (version < SnapshotVersion::V4 && codec != SnapshotFormat(version).codec)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Snapshot codec does not match legacy version {}", toString(version));
}

SnapshotFormat readSnapshotFormat(ReadBuffer & in)
{
    uint8_t version;
    readIntBinary(version, in);
    SnapshotFormat format(static_cast<SnapshotVersion>(version));
    format.validate();
    if (format.version >= SnapshotVersion::V4)
    {
        uint8_t codec;
        uint16_t flags;
        uint32_t reserved;
        readIntBinary(codec, in);
        readIntBinary(flags, in);
        readIntBinary(reserved, in);
        format.codec = static_cast<SnapshotCodec>(codec);
        format.validate();
        if (flags != 0 || reserved != 0)
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot flags or reserved fields");
    }
    return format;
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

ptr<WriteBufferFromFile> openFileAndWriteHeader(const String & path, SnapshotFormat format)
{
    format.validate();
    auto out = std::make_shared<WriteBufferFromFile>(path);
    out->write(MAGIC_SNAPSHOT_HEAD.data(), MAGIC_SNAPSHOT_HEAD.size());
    writeIntBinary(static_cast<uint8_t>(format.version), *out);
    if (format.version >= SnapshotVersion::V4)
    {
        writeIntBinary(static_cast<uint8_t>(format.codec), *out);
        writeIntBinary(uint16_t{0}, *out); // flags
        writeIntBinary(uint32_t{0}, *out); // reserved
    }
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


std::pair<size_t, UInt32> saveBatchV2(ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch, SnapshotFormat format)
{
    if (!batch)
        batch = cs_new<SnapshotBatchBody>();

    String str_buf = SnapshotBatchBody::serialize(*batch);

    if (format.codec == SnapshotCodec::Zstd)
    {
        auto compressed = ZstdLogCodec::compress(str_buf.data(), str_buf.size());
        if (!compressed)
            throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Failed to zstd-compress snapshot batch");
        str_buf.assign(reinterpret_cast<const char *>(compressed->data_begin()), compressed->size());
    }

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
saveBatchAndUpdateCheckSumV2(ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch, UInt32 checksum, SnapshotFormat format)
{
    auto [save_size, data_crc] = saveBatchV2(out, batch, format);
    /// rebuild batch
    batch = cs_new<SnapshotBatchBody>();
    return {save_size, updateCheckSum(checksum, data_crc)};
}

namespace
{

    template <typename Map, typename WriteElement>
    UInt32 writeMetadataBatches(
        ptr<WriteBufferFromFile> & out,
        UInt32 checksum,
        SnapshotBatchType type,
        const Map & values,
        UInt32 save_batch_size,
        SnapshotFormat format,
        WriteElement write_element)
    {
        if (save_batch_size == 0)
            throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Snapshot batch size must be positive");

        auto batch = cs_new<SnapshotBatchBody>();
        batch->type = type;
        for (const auto & value : values)
        {
            WriteBufferFromOwnString buf;
            write_element(value, buf);
            batch->add(buf.str());
            if (batch->size() == save_batch_size)
            {
                checksum = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format).second;
                batch->type = type;
            }
        }

        if (batch->size() != 0 || values.empty())
            checksum = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format).second;
        return checksum;
    }

    template <typename Map>
    UInt32
    writeMapBatches(ptr<WriteBufferFromFile> & out, UInt32 checksum, const Map & values, UInt32 save_batch_size, SnapshotFormat format)
    {
        constexpr auto type
            = std::is_same_v<Map, IntMap> ? SnapshotBatchType::SNAPSHOT_TYPE_UINTMAP : SnapshotBatchType::SNAPSHOT_TYPE_STRINGMAP;
        return writeMetadataBatches(
            out,
            checksum,
            type,
            values,
            save_batch_size,
            format,
            [](const auto & value, WriteBuffer & buf)
            {
                Coordination::write(value.first, buf);
                Coordination::write(value.second, buf);
            });
    }

    UInt32 writeSessionBatches(
        ptr<WriteBufferFromFile> & out,
        UInt32 checksum,
        const SessionAndTimeout & sessions,
        const SessionAndAuth & auth,
        UInt32 save_batch_size,
        SnapshotFormat format)
    {
        return writeMetadataBatches(
            out,
            checksum,
            SnapshotBatchType::SNAPSHOT_TYPE_SESSION,
            sessions,
            save_batch_size,
            format,
            [&auth](const auto & value, WriteBuffer & buf)
            {
                Coordination::write(value.first, buf);
                Coordination::write(value.second, buf);
                auto it = auth.find(value.first);
                Coordination::write(it == auth.end() ? Coordination::AuthIDs{} : it->second, buf);
            });
    }

    UInt32 writeAclBatches(
        ptr<WriteBufferFromFile> & out, UInt32 checksum, const NumToACLMap & acls, UInt32 save_batch_size, SnapshotFormat format)
    {
        return writeMetadataBatches(
            out,
            checksum,
            SnapshotBatchType::SNAPSHOT_TYPE_ACLMAP,
            acls,
            save_batch_size,
            format,
            [](const auto & value, WriteBuffer & buf)
            {
                Coordination::write(value.first, buf);
                Coordination::write(value.second, buf);
            });
    }

}

void serializeAclsV2(const NumToACLMap & acls, String path, UInt32 save_batch_size, SnapshotFormat format)
{
    auto out = openFileAndWriteHeader(path, format);
    auto checksum = writeAclBatches(out, 0, acls, save_batch_size, format);
    writeTailAndClose(out, checksum);
}

void serializeSessionsV2(
    SessionAndTimeout & session_and_timeout,
    SessionAndAuth & session_and_auth,
    UInt32 save_batch_size,
    SnapshotFormat format,
    String & path)
{
    auto out = openFileAndWriteHeader(path, format);
    auto checksum = writeSessionBatches(out, 0, session_and_timeout, session_and_auth, save_batch_size, format);
    writeTailAndClose(out, checksum);
}

template <typename T>
void serializeMapV2(T & snap_map, UInt32 save_batch_size, SnapshotFormat format, String & path)
{
    auto out = openFileAndWriteHeader(path, format);
    auto checksum = writeMapBatches(out, 0, snap_map, save_batch_size, format);
    writeTailAndClose(out, checksum);
}

template void serializeMapV2<StringMap>(StringMap & snap_map, UInt32 save_batch_size, SnapshotFormat format, String & path);
template void serializeMapV2<IntMap>(IntMap & snap_map, UInt32 save_batch_size, SnapshotFormat format, String & path);

void serializeSnapshotMetadata(
    const IntMap & counters,
    const SessionAndTimeout & sessions,
    const SessionAndAuth & auth,
    const NumToACLMap & acls,
    UInt32 save_batch_size,
    SnapshotFormat format,
    const String & path)
{
    auto out = openFileAndWriteHeader(path, format);
    auto checksum = writeMapBatches(out, 0, counters, save_batch_size, format);
    checksum = writeSessionBatches(out, checksum, sessions, auth, save_batch_size, format);
    checksum = writeAclBatches(out, checksum, acls, save_batch_size, format);
    writeTailAndClose(out, checksum);
}

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
    if (type < 0 || type > static_cast<int32_t>(SnapshotBatchType::SNAPSHOT_TYPE_ACLMAP))
        throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Invalid snapshot batch type {}", type);
    batch_body->type = static_cast<SnapshotBatchType>(type);
    int32_t element_count;
    readIntBinary(element_count, in);
    if (element_count < 0 || static_cast<size_t>(element_count) > in.available() / sizeof(int32_t))
        throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Invalid snapshot batch element count {}", element_count);
    batch_body->elements.reserve(element_count);
    for (int i = 0; i < element_count; i++)
    {
        int32_t element_size;
        readIntBinary(element_size, in);
        if (element_size < 0 || static_cast<size_t>(element_size) > in.available())
            throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Invalid snapshot batch element size {}", element_size);
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

IntMap parseBatchIntMapV2(KeeperStore & store, std::optional<UInt32> & object_count, SnapshotBatchBody & batch, SnapshotVersion /*version*/)
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
    if (int_map.contains("ZXID"))
    {
        store.setZxid(int_map["ZXID"]);
    }
    if (int_map.contains("SESSIONID"))
    {
        store.setSessionIDCounter(int_map["SESSIONID"]);
    }
    if (int_map.contains("OBJECTCOUNT"))
    {
        object_count = int_map["OBJECTCOUNT"];
    }
    return int_map;
}

}
