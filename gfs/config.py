"""Cluster topology and timing constants. Every node is a local process on its own port."""
import os

HOST = "127.0.0.1"
MASTERS = {"m1": 7001, "m2": 7002, "m3": 7003}
CHUNKSERVERS = {f"cs{i}": 7100 + i for i in range(1, 6)}
DASHBOARD_PORT = int(os.environ.get("GFS_DASHBOARD_PORT", 8765))

REPLICATION = 3
CHUNK_SIZE = int(os.environ.get("GFS_CHUNK_SIZE", 262144))  # bytes; real GFS uses 64 MB
DATA_DIR = os.environ.get("GFS_DATA", "data")

# Raft timing (seconds)
RAFT_HEARTBEAT = 0.08
ELECTION_MIN = 0.5
ELECTION_MAX = 1.0
SNAPSHOT_EVERY = int(os.environ.get("GFS_SNAPSHOT_EVERY", 500))  # applied entries between snapshots

# Chunk-server liveness
HEARTBEAT = 0.4          # chunk server -> every master
DEAD_AFTER = 2.5         # no heartbeat for this long => server considered dead

# Append recovery
RECONCILE_GRACE = 1.5    # an intent must be this old before the leader settles it
ABORT_AFTER = 4.0        # unreachable replica for this long => abort the intent


def addr(port):
    return f"{HOST}:{port}"


def master_addrs():
    return {m: addr(p) for m, p in MASTERS.items()}


def chunkserver_addrs():
    return {c: addr(p) for c, p in CHUNKSERVERS.items()}
