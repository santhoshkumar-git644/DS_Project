"""Where does an append's time go? Measures each phase and the effect of parallel replica writes.

    python tests/bench.py
Phases: begin = INTENT through Raft; write = data to 3 replicas (each fsyncs data + index);
commit = replica check + DONE through Raft.
"""
import json
import os
import shutil
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gfs.client import Client
from gfs.cluster import Cluster

N = 150
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def one_client(name, parallel, n=N):
    cl = Client(name, parallel_writes=parallel)
    t0 = time.perf_counter()
    for i in range(n):
        cl.append("/bench", f"{name}-{i}|{'x' * 200}".encode(), f"{name}-{i}")
    return cl, time.perf_counter() - t0


def summarize(label, cl, elapsed, n):
    b, w, c = zip(*cl.timings)
    tot = [x + y + z for x, y, z in cl.timings]
    ms = lambda v: round(v * 1000, 1)
    return {"config": label, "appends_per_s": round(n / elapsed, 1),
            "total_p50_ms": ms(statistics.median(tot)), "total_p95_ms": ms(pct(tot, 95)),
            "begin_p50_ms": ms(statistics.median(b)), "write_p50_ms": ms(statistics.median(w)),
            "commit_p50_ms": ms(statistics.median(c))}


def main():
    work = os.path.join(OUT, "bench")
    shutil.rmtree(work, ignore_errors=True)
    cluster = Cluster(work)
    rows = []
    try:
        cluster.start_all()
        cluster.wait_ready()
        one_client("warm", True, 20)
        for label, parallel in (("1 client, sequential replica writes", False),
                                ("1 client, parallel replica writes", True)):
            cl, el = one_client(label[:3] + str(parallel), parallel)
            rows.append(summarize(label, cl, el, N))

        results = []
        def run(i):
            results.append(one_client(f"c{i}", True, 100))
        threads = [threading.Thread(target=run, args=(i,)) for i in range(3)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        el = time.perf_counter() - t0
        merged = Client("merged")
        for cl, _ in results:
            merged.timings += cl.timings
        rows.append(summarize("3 concurrent clients, parallel writes", merged, el, 300))
    finally:
        cluster.stop_all()
    cols = list(rows[0])
    w = [max(len(c), *(len(str(r[c])) for r in rows)) for c in cols]
    print("  ".join(c.ljust(x) for c, x in zip(cols, w)))
    for r in rows:
        print("  ".join(str(r[c]).ljust(x) for c, x in zip(cols, w)))
    json.dump(rows, open(os.path.join(OUT, "bench.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
