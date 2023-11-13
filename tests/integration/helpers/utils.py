from kazoo.client import KazooClient, Create, GetData, GetChildren, OPEN_ACL_UNSAFE, string_types, bytes_types, \
    GetChildren2, Exists
from kazoo.protocol.paths import _prefix_root
from kazoo.protocol.serialization import MultiHeader, Transaction, multiheader_struct, int_struct, read_string, \
    read_buffer, stat_struct, ZnodeStat, write_string
from kazoo.protocol.connection import ReplyHeader
from kazoo.exceptions import EXCEPTIONS, MarshallingError
from collections import namedtuple
import struct


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

        if not isinstance(path, string_types):
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


class GetChildren3(namedtuple('GetChildren3', 'path watcher list_type')):
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
        if not isinstance(path, string_types):
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
            if isinstance(result, string_types):
                resp.append(client.unchroot(result))
            else:
                resp.append(result)
        return resp
