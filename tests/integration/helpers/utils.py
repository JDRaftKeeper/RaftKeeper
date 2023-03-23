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
