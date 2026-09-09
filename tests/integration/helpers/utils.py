from kazoo.client import KazooClient, Create, GetData, GetChildren, Exists, OPEN_ACL_UNSAFE, \
    GetChildren2, Exists, TransactionRequest, Create2
from kazoo.protocol.paths import _prefix_root
from kazoo.protocol.serialization import MultiHeader, Transaction, multiheader_struct, int_struct, read_string, \
    read_buffer, stat_struct, ZnodeStat, write_string, write_buffer, Delete, SetData, CheckVersion
from kazoo.protocol.connection import ReplyHeader
from kazoo.exceptions import EXCEPTIONS, MarshallingError, NoNodeError
from kazoo.security import ACL
from kazoo.handlers.utils import capture_exceptions, wrap
from collections import namedtuple
import struct
from os.path import split

bytes_types = (bytes, bytearray)

def close_zk_clients(zk_clients):
    for zk_client in zk_clients:
        try:
            zk_client.stop()
            zk_client.close()
        except:
            pass


def close_zk_client(zk_client):
    try:
        zk_client.stop()
        zk_client.close()
    except:
        pass


# clear child nodes for path
def clear_zk_children(zk_client, path):
    try:
        nodes = zk_client.get_children(path)
        for node in [nodes]:
            zk_client.delete(path + '/' + node)
    finally:
        close_zk_client(zk_client)


class CheckIfNotExistsVersion(namedtuple("CheckVersion", "path version")):
    type = 501

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b


class CreateIfNotExists(namedtuple("CreateIfNotExists", "path data acl flags")):
    type = 502

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(len(self.acl)))
        for acl in self.acl:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.flags))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return read_string(bytes, offset)[0]

class MultiReadRequest(object):
    """A Zookeeper MultiReadRequest

    A MultiReadRequest provides a builder object that can be used to
    construct and commit a set of operations.

    """
    def __init__(self, client):
        self.client = client
        self.operations = []
        self.committed = False

    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False):
        """Add a create ZNode ops to the operations.

        .. note::

            Create ops is illegal for mulitRead,
            Will get `kazoo.exceptions.MarshallingError` after commit
            It's only used for test.

        :returns: None

        """
        if acl is None and self.client.default_acl:
            acl = self.client.default_acl

        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if acl and not isinstance(acl, (tuple, list)):
            raise TypeError("Invalid type for 'acl' (acl must be a tuple/list"
                            " of ACL's")
        if not isinstance(value, bytes_types):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(ephemeral, bool):
            raise TypeError("Invalid type for 'ephemeral' (bool expected)")
        if not isinstance(sequence, bool):
            raise TypeError("Invalid type for 'sequence' (bool expected)")

        flags = 0
        if ephemeral:
            flags |= 1
        if sequence:
            flags |= 2
        if acl is None:
            acl = OPEN_ACL_UNSAFE

        self._add(Create(_prefix_root(self.client.chroot, path), value, acl,
                         flags), None)

    def get(self, path, watcher):
        """Add a get ZNode ops to the operations.
        :returns: None
        """
        self._add(GetData(path, watcher), None)

    def get_children(self, path, watcher):
        """Add a simpleList ops to the operations.
        :returns: None
        """
        self._add(GetChildren(path, watcher), None)

    def exists(self, path, watcher):
        """Add an exists ZNode ops to the operations.
        :returns: None
        """
        self._add(Exists(path, watcher), None)

    def get_children3(self, path, list_type, watcher):
        """Add a filteredList ops to the operations.
        :returns: List of childern
        """
        self._add(GetChildren3(path, watcher, list_type), None)

    def commit_async(self):
        """Commit the operations asynchronously.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        self.committed = True
        async_object = self.client.handler.async_result()
        self.client._call(MultiRead(self.operations), async_object)
        return async_object

    def commit(self):
        """Commit the operations.

        :returns: A list of the results for each operation in the
                  transaction.

        """
        return self.commit_async().get()

    def __enter__(self):
        return self

    def _add(self, request, post_processor=None):
        self.operations.append(request)


class GetChildren3(namedtuple('GetChildren3', 'path watcher list_type'), GetChildren2):
    type = 500

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        b.extend(struct.pack('B', self.list_type))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        children = []
        for c in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return children, stat


class RemoveRecursive(namedtuple('RemoveRecursive', 'path remove_nodes_limit')):
    type = 503

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(struct.pack('!I', self.remove_nodes_limit))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return True


class TryRemove(namedtuple('TryRemove', 'path version')):
    type = 505

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(struct.pack('!i', self.version))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return True


class ListRecursive(namedtuple('ListRecursive', 'path max_entries')):
    type = 507

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(struct.pack('!I', self.max_entries))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        children = []
        for c in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        return children


class CheckStat(namedtuple('CheckStat', 'path version stat')):
    type = 504

    def serialize(self):
        # Wire: path + version + full Stat (matches ClickHouse Keeper). Any Stat field == -1 is a wildcard.
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        b.extend(stat_struct.pack(*self.stat))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return True


class FilteredListWithStatsAndData(namedtuple('FilteredListWithStatsAndData', 'path watcher list_type with_stat with_data')):
    type = 506

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        b.extend(struct.pack('B', self.list_type))
        b.extend([1 if self.with_stat else 0])
        b.extend([1 if self.with_data else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        # names
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        children = []
        for _ in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        # parent stat
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        offset += stat_struct.size
        # per-child stats
        stats_count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        stats = []
        for _ in range(stats_count):
            stats.append(ZnodeStat._make(stat_struct.unpack_from(bytes, offset)))
            offset += stat_struct.size
        # per-child data
        data_count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        data = []
        for _ in range(data_count):
            d, offset = read_buffer(bytes, offset)
            data.append(d)
        return children, stat, stats, data


class KeeperFeatureClient(KazooClient):
    """A Zookeeper Python client extends from Kazoo.KazooClient,
    Kazoo is a Python library working with Zookeeper.

    supports multi_read ops
    """
    def __init__(self, hosts, timeout):
        """Create a :class:`KeeperFeatureClient` instance (extends from KazooClient).

        :param hosts: Comma-separated list of hosts to connect to
                      (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
        :param timeout: The longest to wait for a Zookeeper connection.

        Basic Example:

        For example::

            zk = KeeperFeatureClient()
            t = zk.start()
            children = zk.get_children('/')
            zk.stop()

        """
        super(KeeperFeatureClient, self).__init__(
            hosts=hosts
            , timeout=timeout)

    def multi_read(self):
        """Create and return a :class:`TransactionRequest` object

        Creates a :class:`MultiReadRequest` object. An ops can
        consist of multiple operations which can be committed as a
        single atomic unit.
        one of operations failure does not affect other operations

        :returns: A MultiReadRequest.
        :rtype: :class:`MultiReadRequest`

        """
        return MultiReadRequest(self)

    def remove_recursive(self, path, remove_nodes_limit=0):
        """Remove a node and all its descendants atomically.

        :returns: True on success.
        :raises NoNodeError: if the node doesn't exist.
        """
        async_result = self.handler.async_result()
        self._call(RemoveRecursive(_prefix_root(self.chroot, path), remove_nodes_limit), async_result)
        return async_result.get()

    def try_remove(self, path, version=-1):
        """Remove a node if it exists. No error if the node is absent.

        :returns: True on success.
        """
        async_result = self.handler.async_result()
        self._call(TryRemove(_prefix_root(self.chroot, path), version), async_result)
        return async_result.get()

    def list_recursive(self, path, max_entries=0):
        """List all descendants of a node recursively.

        :returns: List of full paths under the given node.
        """
        async_result = self.handler.async_result()
        self._call(ListRecursive(_prefix_root(self.chroot, path), max_entries), async_result)
        return async_result.get()

    def check_stat(self, path, version=-1, czxid=-1, mzxid=-1, ctime=-1, mtime=-1,
                   stat_version=-1, cversion=-1, aversion=-1, ephemeral_owner=-1,
                   data_length=-1, num_children=-1, pzxid=-1):
        """Check a node's stat (OpNum 504). `version` is the check version; every Stat field left
        at -1 is a wildcard. Wire format matches ClickHouse Keeper (path + version + full Stat).

        :returns: True on match. Raises on mismatch/missing node.
        """
        stat = (czxid, mzxid, ctime, mtime, stat_version, cversion, aversion,
                ephemeral_owner, data_length, num_children, pzxid)
        async_result = self.handler.async_result()
        self._call(CheckStat(_prefix_root(self.chroot, path), version, stat), async_result)
        return async_result.get()

    def list_children_with_stats_and_data(self, path, list_type=0, with_stat=True, with_data=True, watch=None):
        """FilteredListWithStatsAndData (OpNum 506).

        :returns: (children, stat, per_child_stats, per_child_data)
        """
        async_result = self.handler.async_result()
        self._call(FilteredListWithStatsAndData(_prefix_root(self.chroot, path), watch, list_type, with_stat, with_data), async_result)
        return async_result.get()

    def get_filtered_children(self, path, watch=None, list_type=None, include_data=False):
        """Get a list of child nodes of a path.

        If a watch is provided it will be left on the node with the
        given path. The watch will be triggered by a successful
        operation that deletes the node of the given path or
        creates/deletes a child under the node.

        The list of children returned is not sorted and no guarantee is
        provided as to its natural or lexical order.

        :param path: Path of node to list.
        :param watch: Optional watch callback to set for future changes
                      to this path.
        :param include_data:
            Include the :class:`~kazoo.protocol.states.ZnodeStat` of
            the node in addition to the children. This option changes
            the return value to be a tuple of (children, stat).

        :param list_type: type of List,
            Options:{0: ALL, 1: PERSISTENT_ONLY, 2:EPHEMERAL_ONLY}.

        :returns: List of child node names, or tuple if `include_data`
                  is `True`.
        :rtype: list

        :raises:
            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        .. versionadded:: 0.5
            The `include_data` option.

        """
        return self.get_filtered_children_async(path, watch=watch, list_type=list_type,
                                       include_data=include_data).get()

    def get_filtered_children_async(self, path, watch=None, list_type=None, include_data=False):
        """Asynchronously get a list of child nodes of a path. Takes
        the same arguments as :meth:`get_children`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if watch and not callable(watch):
            raise TypeError("Invalid type for 'watch' (must be a callable)")
        if not isinstance(include_data, bool):
            raise TypeError("Invalid type for 'include_data' (bool expected)")


        async_result = self.handler.async_result()

        if type:
            req = GetChildren3(_prefix_root(self.chroot, path), watch, list_type)
        else:
            if include_data:
                req = GetChildren2(_prefix_root(self.chroot, path), watch)
            else:
                req = GetChildren(_prefix_root(self.chroot, path), watch)
        self._call(req, async_result)
        return async_result
    def _create_async_inner(
        self, path, value, acl, flags, trailing=False, include_data=False, if_not_exsits=False
    ):
        async_result = self.handler.async_result()
        if if_not_exsits:
            opcode = CreateIfNotExists
        elif include_data:
            opcode = Create2
        else:
            opcode = Create

        call_result = self._call(
            opcode(
                _prefix_root(self.chroot, path, trailing=trailing),
                value,
                acl,
                flags,
            ),
            async_result,
        )
        if call_result is False:
            # We hit a short-circuit exit on the _call. Because we are
            # not using the original async_result here, we bubble the
            # exception upwards to the do_create function in
            # KazooClient.create so that it gets set on the correct
            # async_result object
            raise async_result.exception
        return async_result

    def create_if_not_exists_async(
            self,
            path,
            value=b"",
            acl=None,
            ephemeral=False,
            sequence=False,
            makepath=False,
            include_data=False,
    ):
        """Asynchronously create a ZNode if not exists. Takes the same arguments as
        :meth:`create`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        .. versionadded:: 1.1
            The makepath option.
        .. versionadded:: 2.7
            The `include_data` option.
        """
        if acl is None and self.default_acl:
            acl = self.default_acl

        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if acl and (
                isinstance(acl, ACL) or not isinstance(acl, (tuple, list))
        ):
            raise TypeError(
                "Invalid type for 'acl' (acl must be a tuple/list" " of ACL's"
            )
        if value is not None and not isinstance(value, bytes):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(ephemeral, bool):
            raise TypeError("Invalid type for 'ephemeral' (bool expected)")
        if not isinstance(sequence, bool):
            raise TypeError("Invalid type for 'sequence' (bool expected)")
        if not isinstance(makepath, bool):
            raise TypeError("Invalid type for 'makepath' (bool expected)")
        if not isinstance(include_data, bool):
            raise TypeError("Invalid type for 'include_data' (bool expected)")

        flags = 0
        if ephemeral:
            flags |= 1
        if sequence:
            flags |= 2
        if acl is None:
            acl = OPEN_ACL_UNSAFE

        async_result = self.handler.async_result()

        @capture_exceptions(async_result)
        def do_create():
            result = self._create_async_inner(
                path,
                value,
                acl,
                flags,
                trailing=sequence,
                include_data=include_data,
                if_not_exsits=True,
            )
            result.rawlink(create_completion)

        @capture_exceptions(async_result)
        def retry_completion(result):
            result.get()
            do_create()

        @wrap(async_result)
        def create_completion(result):
            try:
                if include_data:
                    new_path, stat = result.get()
                    return self.unchroot(new_path), stat
                else:
                    return self.unchroot(result.get())
            except NoNodeError:
                if not makepath:
                    raise
                if sequence and path.endswith("/"):
                    parent = path.rstrip("/")
                else:
                    parent, _ = split(path)
                self.ensure_path_async(parent, acl).rawlink(retry_completion)

        do_create()
        return async_result

    def create_if_not_exists(
            self,
            path,
            value=b"",
            acl=None,
            ephemeral=False,
            sequence=False,
            makepath=False,
            include_data=False,
    ):
        """Create a node with the given value as its data. Optionally
        set an ACL on the node.

        The ephemeral and sequence arguments determine the type of the
        node.

        An ephemeral node will be automatically removed by ZooKeeper
        when the session associated with the creation of the node
        expires.

        A sequential node will be given the specified path plus a
        suffix `i` where i is the current sequential number of the
        node. The sequence number is always fixed length of 10 digits,
        0 padded. Once such a node is created, the sequential number
        will be incremented by one.

        If a node with the same actual path already exists in
        ZooKeeper, a NodeExistsError will be raised. Note that since a
        different actual path is used for each invocation of creating
        sequential nodes with the same path argument, the call will
        never raise NodeExistsError.

        If the parent node does not exist in ZooKeeper, a NoNodeError
        will be raised. Setting the optional `makepath` argument to
        `True` will create all missing parent nodes instead.

        An ephemeral node cannot have children. If the parent node of
        the given path is ephemeral, a NoChildrenForEphemeralsError
        will be raised.

        This operation, if successful, will trigger all the watches
        left on the node of the given path by :meth:`exists` and
        :meth:`get` API calls, and the watches left on the parent node
        by :meth:`get_children` API calls.

        The maximum allowable size of the node value is 1 MB. Values
        larger than this will cause a ZookeeperError to be raised.

        :param path: Path of node.
        :param value: Initial bytes value of node.
        :param acl: :class:`~kazoo.security.ACL` list.
        :param ephemeral: Boolean indicating whether node is ephemeral
                          (tied to this session).
        :param sequence: Boolean indicating whether path is suffixed
                         with a unique index.
        :param makepath: Whether the path should be created if it
                         doesn't exist.
        :param include_data:
            Include the :class:`~kazoo.protocol.states.ZnodeStat` of
            the node in addition to its real path. This option changes
            the return value to be a tuple of (path, stat).

        :returns: Real path of the new node, or tuple if `include_data`
                  is `True`
        :rtype: str

        :raises:
            :exc:`~kazoo.exceptions.NodeExistsError` if the node
            already exists.

            :exc:`~kazoo.exceptions.NoNodeError` if parent nodes are
            missing.

            :exc:`~kazoo.exceptions.NoChildrenForEphemeralsError` if
            the parent node is an ephemeral node.

            :exc:`~kazoo.exceptions.ZookeeperError` if the provided
            value is too large.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        .. versionadded:: 1.1
            The `makepath` option.
        .. versionadded:: 2.7
            The `include_data` option.
        """
        acl = acl or self.default_acl
        return self.create_if_not_exists_async(
            path,
            value,
            acl=acl,
            ephemeral=ephemeral,
            sequence=sequence,
            makepath=makepath,
            include_data=include_data,
        ).get()

    def transaction(self):
        """Create and return a :class:`TransactionRequest` object

        Creates a :class:`TransactionRequest` object. A Transaction can
        consist of multiple operations which can be committed as a
        single atomic unit. Either all of the operations will succeed
        or none of them.

        :returns: A TransactionRequest.
        :rtype: :class:`TransactionRequest`

        .. versionadded:: 0.6
            Requires Zookeeper 3.4+

        """
        return TransactionRequestExt(self)

class TransactionExt(Transaction):
    """Transaction whose deserialize also understands the ClickHouse-Keeper extension ops
    (CheckStat/RemoveRecursive/TryRemove) in successful subresponses. Stock kazoo's Transaction
    checks the header type before the error code, so an ok header with an unknown extension type
    would silently corrupt the whole result list."""

    @classmethod
    def deserialize(cls, bytes, offset):
        header = MultiHeader(None, False, None)
        results = []
        response = None
        while not header.done:
            if header.type == -1:
                err = int_struct.unpack_from(bytes, offset)[0]
                offset += int_struct.size
                response = EXCEPTIONS[err]()
            elif header.err:
                # RaftKeeper reports a failed subresponse via the header err field with the
                # original op type preserved (ZooKeeper uses type == -1 above); both must parse
                # to the exception.
                response = EXCEPTIONS[header.err]()
            elif header.type == Create.type:
                response, offset = read_string(bytes, offset)
            elif header.type == Delete.type:
                response = True
            elif header.type == SetData.type:
                response = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
                offset += stat_struct.size
            elif header.type == CheckVersion.type or header.type == CheckStat.type \
                    or header.type == RemoveRecursive.type or header.type == TryRemove.type:
                # CheckStat/RemoveRecursive/TryRemove success bodies are empty.
                response = True
            if response:
                results.append(response)
            header, offset = MultiHeader.deserialize(bytes, offset)
        return results


class TransactionRequestExt(TransactionRequest):
    """A Zookeeper Transaction Request

    A Transaction provides a builder object that can be used to
    construct and commit an atomic set of operations. The transaction
    must be committed before its sent.

    Transactions are not thread-safe and should not be accessed from
    multiple threads at once.

    .. note::

        The ``committed`` attribute only indicates whether this
        transaction has been sent to Zookeeper and is used to prevent
        duplicate commits of the same transaction. The result should be
        checked to determine if the transaction executed as desired.

    .. versionadded:: 0.6
        Requires Zookeeper 3.4+

    """

    def check_if_not_exists(self, path, version):
        """Add a Check Version to the transaction.

        This command will fail and abort a transaction if the path
        does not match the specified version.

        """
        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")
        self._add(
            CheckIfNotExistsVersion(_prefix_root(self.client.chroot, path), version)
        )

    def commit_async(self):
        """Use TransactionExt so extension subresponses (CheckStat & co.) parse correctly."""
        self.committed = True
        async_object = self.client.handler.async_result()
        self.client._call(TransactionExt(self.operations), async_object)
        return async_object

    def check_stat(self, path, version=-1, cversion=-1, aversion=-1):
        """Add a CheckStat (OpNum 504) condition to the transaction."""
        # CheckStat.stat tuple order: czxid, mzxid, ctime, mtime, version, cversion, aversion,
        # ephemeralOwner, dataLength, numChildren, pzxid. -1 = wildcard (matches the direct
        # KeeperFeatureClient.check_stat method).
        stat = (-1, -1, -1, -1, -1, cversion, aversion, -1, -1, -1, -1)
        self._add(CheckStat(_prefix_root(self.client.chroot, path), version, stat))

    def try_remove(self, path, version=-1):
        """Add a TryRemove (OpNum 505) to the transaction."""
        self._add(TryRemove(_prefix_root(self.client.chroot, path), version))

    def remove_recursive(self, path, remove_nodes_limit=0):
        """Add a RemoveRecursive (OpNum 503) to the transaction."""
        self._add(RemoveRecursive(_prefix_root(self.client.chroot, path), remove_nodes_limit))

    def create_if_not_exist(self, path, value=b"", acl=None, ephemeral=False,
                            sequence=False):
        """Add a create ZNode ops to the operations.

        .. note::

            Create ops is illegal for mulitRead,
            Will get `kazoo.exceptions.MarshallingError` after commit
            It's only used for test.

        :returns: None

        """
        if acl is None and self.client.default_acl:
            acl = self.client.default_acl

        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if acl and not isinstance(acl, (tuple, list)):
            raise TypeError("Invalid type for 'acl' (acl must be a tuple/list"
                            " of ACL's")
        if not isinstance(value, bytes_types):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(ephemeral, bool):
            raise TypeError("Invalid type for 'ephemeral' (bool expected)")
        if not isinstance(sequence, bool):
            raise TypeError("Invalid type for 'sequence' (bool expected)")

        flags = 0
        if ephemeral:
            flags |= 1
        if sequence:
            flags |= 2
        if acl is None:
            acl = OPEN_ACL_UNSAFE

        self._add(CreateIfNotExists(_prefix_root(self.client.chroot, path), value, acl,
                                    flags), None)


class MultiRead(namedtuple('MultiRead', 'operations')):
    type = 22

    def serialize(self):
        b = bytearray()
        for op in self.operations:
            b.extend(MultiHeader(op.type, False, -1).serialize() +
                     op.serialize())
        return b + multiheader_struct.pack(-1, True, -1)

    @classmethod
    def deserialize(cls, bytes, offset):
        header = MultiHeader(None, False, None)
        results = []
        response = None
        while not header.done:
            if header.type == -1:
                err = int_struct.unpack_from(bytes, offset)[0]
                offset += int_struct.size
                response = EXCEPTIONS[err]()
            elif header.err is not None and header.err != 0:
                response = EXCEPTIONS[header.err]()
            elif header.type == Create.type:
                response, offset = read_string(bytes, offset)
            elif header.type == GetData.type:
                data, offset = read_buffer(bytes, offset)
                stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
                offset += stat_struct.size
                response = (data, stat)
            elif header.type == GetChildren2.type:
                count = int_struct.unpack_from(bytes, offset)[0]
                offset += int_struct.size
                children = []

                if count == -1:  # pragma: nocover
                    print("-------11-------")
                for c in range(count):
                    child, offset = read_string(bytes, offset)
                    children.append(child)
                stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
                offset += stat_struct.size
                response = (children, stat)

            elif header.type == Exists.type:
                stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
                offset += stat_struct.size
                response = (stat)

            elif header.type == GetChildren.type:
                count = int_struct.unpack_from(bytes, offset)[0]
                offset += int_struct.size
                children = []

                if count == -1:  # pragma: nocover
                    print("-------11-------")
                for c in range(count):
                    child, offset = read_string(bytes, offset)
                    children.append(child)
                response = children
            if response is not None:
                results.append(response)
            header, offset = MultiHeader.deserialize(bytes, offset)
        return results

    @staticmethod
    def unchroot(client, response):
        resp = []
        for result in response:
            if isinstance(result, str):
                resp.append(client.unchroot(result))
            else:
                resp.append(result)
        return resp
