"""Smoke test: start the cluster, append records, read them back, kill the leader, keep going."""
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gfs import checker, config
from gfs.client import Client
from gfs.cluster import Cluster

work = os.path.join(os.environ.get("TEMP", "."), "gfs_smoke")
shutil.rmtree(work, ignore_errors=True)
c = Cluster(work, chunk_size=2048)
log = os.path.join(work, "c1.jsonl")
try:
    c.start_all()
    t0 = time.time()
    leader = c.wait_ready()
    print(f"ready in {time.time() - t0:.1f}s, leader={leader}")
    cl = Client("c1", log)
    t0 = time.time()
    for i in range(20):
        cl.append("/f", f"c1-{i}|{'x' * 100}".encode(), f"c1-{i}")
    print(f"20 appends in {time.time() - t0:.2f}s")
    print("killing leader", leader)
    c.kill(leader)
    t0 = time.time()
    new = c.wait_leader(exclude=leader)
    print(f"new leader {new} after {time.time() - t0:.2f}s")
    for i in range(20, 40):
        cl.append("/f", f"c1-{i}|{'x' * 100}".encode(), f"c1-{i}")
    recs = cl.read_all("/f")
    print("records readable:", len(recs))
    res = checker.check("/f", [log])
    print({k: v for k, v in res.items() if k not in ("mismatch",)})
    print("OK" if res["ok"] and res["records_visible"] == 40 else "FAILED")
finally:
    c.stop_all()
