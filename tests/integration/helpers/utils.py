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
