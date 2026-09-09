"""3-node cluster back-to-back comparison: RaftKeeper cluster vs a real ClickHouse Keeper cluster.

Where test_clickhouse_keeper_back_to_back is single-node and functional, this suite stands up a
3-node RaftKeeper cluster and a 3-node ClickHouse Keeper cluster and targets the gaps that a
single node cannot exercise and that ClickHouse depends on:

  - watch triggering (data + child watches), including a watch set on one node and triggered via
    another (cross-node watch propagation);
  - cross-node read-after-write consistency (write on one node, sync + read on another);
  - versioned Remove (CAS delete), Sync, and FilteredListWithStatsAndData (OpNum 506).

Like the single-node suite it needs a real ClickHouse Keeper image and must run against a
ClickHouse-compatible RaftKeeper build; it is run only by the integration-test-clickhouse-mode CI
job and self-skips if the keeper cluster can't be brought up.
"""
import os
import re
import subprocess
import threading
import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import KeeperFeatureClient, close_zk_clients

cluster = RaftKeeperCluster(__file__)

# 3-node RaftKeeper cluster; ch_keeper cluster is attached via with_clickhouse_keeper_cluster.
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper_three_nodes_1.xml'],
                             with_clickhouse_keeper_cluster=True, stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper_three_nodes_2.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper_three_nodes_3.xml'],
                             stay_alive=True)


# --- infra: get the keeper image into the docker-in-docker daemon and make compose v1 tolerate it ---

def _keeper_image_tar():
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ch_keeper_image.tar')


def _maybe_load_keeper_image():
    tar = _keeper_image_tar()
    if not os.path.exists(tar):
        return f"no image tarball at {tar} (DIND will try to pull)"
    r = subprocess.run(['docker', 'load', '-i', tar], capture_output=True, text=True)
    return f"docker load rc={r.returncode}: {(r.stdout + r.stderr).strip()[:300]}"


def _patch_compose_for_modern_docker():
    try:
        import pathlib
        import compose.service as svc
        p = pathlib.Path(svc.__file__)
        src = p.read_text()
        patched = re.sub(r"image_config\[(['\"])ContainerConfig\1\]",
                         "image_config.get('ContainerConfig', {})", src)
        if patched != src:
            p.write_text(patched)
            import importlib.util
            cache = importlib.util.cache_from_source(str(p))
            try:
                os.remove(cache)
            except OSError:
                pass
    except Exception:  # noqa: BLE001
        pass


def _keeper_cluster_compose_up():
    """Bring up (only) the reference ClickHouse Keeper cluster via compose. Returns (rc, output)."""
    env = {**os.environ}
    for i, cfg in enumerate(cluster.clickhouse_keeper_cluster_config_paths, start=1):
        env[f'CH_KEEPER_CONFIG{i}'] = cfg
    r = subprocess.run(cluster.base_clickhouse_keeper_cluster_cmd + ['up', '-d', '--force-recreate'],
                       capture_output=True, text=True, env=env)
    return r.returncode, f"$ cluster compose up rc={r.returncode}\n{r.stdout}{r.stderr}"


def _reference_keeper_works():
    """Can the reference ClickHouse Keeper cluster actually run here? Retry its bring-up and probe
    real client connections. Used to decide skip-vs-fail after cluster.start() fails: only a
    verified reference-side failure may skip; a RaftKeeper-side failure must fail the suite (this
    job is the only integration coverage for the ClickHouse-mode build)."""
    try:
        rc, out = _keeper_cluster_compose_up()
        print(out)
        if rc != 0:
            return False
        cluster.wait_clickhouse_keeper_cluster_to_start(30)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"reference keeper probe failed: {e}")
        return False


def _keeper_diagnostics():
    out = []
    for cmd in (['docker', 'images'], ['docker', 'ps', '-a']):
        r = subprocess.run(cmd, capture_output=True, text=True)
        out.append(f"$ {' '.join(cmd)}\n{r.stdout}{r.stderr}")
    try:
        for inst in ('ch_keeper1', 'ch_keeper2', 'ch_keeper3'):
            logs = subprocess.run(['docker', 'logs', cluster.get_instance_docker_id(inst)],
                                  capture_output=True, text=True)
            out.append(f"$ {inst} logs\n{logs.stdout}{logs.stderr}")
    except Exception as e:  # noqa: BLE001
        out.append(f"diag error: {e}")
    return "\n".join(out)[:6000]


def _safe_shutdown():
    try:
        cluster.shutdown()
    except Exception:  # noqa: BLE001
        pass


@pytest.fixture(scope="module")
def started_cluster():
    load_status = _maybe_load_keeper_image()
    _patch_compose_for_modern_docker()
    try:
        cluster.start()
    except Exception as ex:
        # Skip only when the failure is verifiably on the reference side (image unreachable in
        # docker-in-docker etc.). If the reference keeper cluster works, the failure came from
        # RaftKeeper startup and must fail the suite, not be silently skipped.
        diag = _keeper_diagnostics()
        if _reference_keeper_works():
            _safe_shutdown()
            raise Exception(
                f"ClickHouse Keeper cluster is available, but cluster.start() failed - this looks "
                f"like a RaftKeeper-side startup failure and must not be skipped: {ex}\n"
                f"load: {load_status}\n{diag}") from ex
        _safe_shutdown()
        pytest.skip(f"ClickHouse Keeper cluster unavailable: {ex}\nload: {load_status}\n{diag}")
    try:
        yield cluster
    finally:
        _safe_shutdown()


# --- per-cluster node connections ---

def _open(kind, n):
    """Open a client to node n (1-based) of the given cluster kind ('raft' or 'ch')."""
    if kind == 'raft':
        zk = KeeperFeatureClient(hosts=cluster.get_instance_ip(f"node{n}") + ":8101", timeout=60.0)
        zk.start()
        return zk
    return cluster.get_clickhouse_keeper_client(f"ch_keeper{n}")


def _run(scenario, kind):
    try:
        return ('ok', scenario(kind))
    except Exception as e:  # noqa: BLE001
        return ('err', type(e).__name__)


def assert_clusters_agree(started_cluster, scenario, label=""):
    """Run scenario(kind) against both clusters and assert identical outcome (value or exception)."""
    r = _run(scenario, 'raft')
    c = _run(scenario, 'ch')
    assert r == c, f"{label}: raft={r} ch={c}"


def _wait_event(evt, timeout=15):
    return evt.wait(timeout)


def test_versioned_remove(started_cluster):
    # Remove(2) with a version check: wrong version -> BadVersionError, correct version -> deleted.
    def scenario(kind):
        a = _open(kind, 1)
        try:
            a.create('/c_vr', b'x')
            version = a.exists('/c_vr').version
            try:
                a.delete('/c_vr', version=version + 1)
                wrong = 'ok'
            except Exception as e:  # noqa: BLE001
                wrong = type(e).__name__
            a.delete('/c_vr', version=version)
            return (wrong, a.exists('/c_vr') is None)
        finally:
            try:
                a.delete('/c_vr')
            except Exception:
                pass
            close_zk_clients([a])
    assert_clusters_agree(started_cluster, scenario, "versioned_remove")


def test_sync_read_after_write_cross_node(started_cluster):
    # Write on node1, sync + read on node3: the write must be visible (Sync + cross-node consistency).
    def scenario(kind):
        w = _open(kind, 1)
        r = _open(kind, 3)
        try:
            w.create('/c_sync', b'v1')
            r.sync('/c_sync')
            return r.get('/c_sync')[0]
        finally:
            try:
                w.delete('/c_sync')
            except Exception:
                pass
            close_zk_clients([w, r])
    assert_clusters_agree(started_cluster, scenario, "sync_read_after_write")


def test_filtered_list_with_stats_and_data(started_cluster):
    # FilteredListWithStatsAndData (OpNum 506): compare child names and their data (not absolute stat).
    def scenario(kind):
        a = _open(kind, 1)
        try:
            a.create('/c_fld')
            a.create('/c_fld/a', b'da')
            a.create('/c_fld/b', b'db')
            children, _stat, _stats, data = a.list_children_with_stats_and_data(
                '/c_fld', list_type=0, with_stat=True, with_data=True)
            by_name = dict(zip(children, data))
            return (sorted(children), sorted(by_name.items()))
        finally:
            try:
                a.delete('/c_fld', recursive=True)
            except Exception:
                pass
            close_zk_clients([a])
    assert_clusters_agree(started_cluster, scenario, "filtered_list_with_stats_and_data")


def test_data_watch_triggered(started_cluster):
    def scenario(kind):
        a = _open(kind, 1)
        try:
            a.create('/c_dw', b'v0')
            evt = threading.Event()
            box = {}

            def cb(event):
                box['type'] = event.type
                evt.set()

            a.get('/c_dw', watch=cb)
            a.set('/c_dw', b'v1')
            return (_wait_event(evt), box.get('type'))
        finally:
            try:
                a.delete('/c_dw')
            except Exception:
                pass
            close_zk_clients([a])
    assert_clusters_agree(started_cluster, scenario, "data_watch")


def test_child_watch_triggered(started_cluster):
    def scenario(kind):
        a = _open(kind, 1)
        try:
            a.create('/c_cw')
            evt = threading.Event()
            box = {}

            def cb(event):
                box['type'] = event.type
                evt.set()

            a.get_children('/c_cw', watch=cb)
            a.create('/c_cw/child')
            return (_wait_event(evt), box.get('type'))
        finally:
            try:
                a.delete('/c_cw', recursive=True)
            except Exception:
                pass
            close_zk_clients([a])
    assert_clusters_agree(started_cluster, scenario, "child_watch")


def test_watch_across_nodes(started_cluster):
    # Set a data watch through a connection to node2, change the node through node1: the watch must
    # still fire (watches propagate across the cluster, not just the connected node).
    def scenario(kind):
        watcher = _open(kind, 2)
        trigger = _open(kind, 1)
        try:
            trigger.create('/c_wan', b'v0')
            watcher.sync('/c_wan')
            evt = threading.Event()
            box = {}

            def cb(event):
                box['type'] = event.type
                evt.set()

            watcher.get('/c_wan', watch=cb)
            trigger.set('/c_wan', b'v1')
            return (_wait_event(evt), box.get('type'))
        finally:
            try:
                trigger.delete('/c_wan')
            except Exception:
                pass
            close_zk_clients([watcher, trigger])
    assert_clusters_agree(started_cluster, scenario, "watch_across_nodes")


def test_multi_node_read_after_write(started_cluster):
    # Write on node1, read the same value from node2 and node3 (after sync).
    def scenario(kind):
        w = _open(kind, 1)
        readers = [_open(kind, 2), _open(kind, 3)]
        try:
            w.create('/c_mn', b'hello')
            values = []
            for r in readers:
                r.sync('/c_mn')
                values.append(r.get('/c_mn')[0])
            return values
        finally:
            try:
                w.delete('/c_mn')
            except Exception:
                pass
            close_zk_clients([w] + readers)
    assert_clusters_agree(started_cluster, scenario, "multi_node_read_after_write")
