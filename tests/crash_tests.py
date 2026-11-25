"""Crash-injection campaign.

  python tests/crash_tests.py campaign --trials 3     # 5 crash points x trials, exactly-once client
  python tests/crash_tests.py baseline --trials 3     # same, client regenerates the ID on retry
  python tests/crash_tests.py repair                  # kill a chunk server, watch repair, check data
  python tests/crash_tests.py restart                 # kill all masters, restart, check durability + dedup

Every trial runs a fresh 3-master / 5-chunk-server cluster, three concurrent clients, arms one
failpoint on the leader (or on a replica for cs.mid_write), lets the process die, restarts it after
a random delay, waits for all clients to be acked, then runs the independent checker.
"""
import argparse
import json
import os
import random
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gfs import checker, config, failpoints, rpc
from gfs.client import AppendFailed, Client
from gfs.cluster import Cluster

CHUNK = 2048          # small chunks so a trial spans many chunks
FILE = "/data"


def payload(logical, rng):
    return f"{logical}|{'x' * rng.randint(30, 150)}".encode()


def run_clients(n_clients, per_client, work, dedup, rng):
    logs, clients, errors = [], [], []

    def worker(i):
        cl = Client(f"c{i}", os.path.join(work, f"c{i}.jsonl"), dedup=dedup, deadline=60)
        clients.append(cl)
        try:
            for n in range(per_client):
                cl.append(FILE, payload(f"c{i}-{n}", rng), f"c{i}-{n}")
        except AppendFailed as e:
            errors.append(str(e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, n_clients + 1)]
    for t in threads:
        t.start()
    logs = [os.path.join(work, f"c{i}.jsonl") for i in range(1, n_clients + 1)]
    return threads, logs, clients, errors


def failover_time(cluster, old, t0, timeout=15):
    """Seconds from killing `old` until the new leader has applied its first entry (its own
    leader_ready event). Falls back to the time the harness noticed."""
    new = cluster.wait_leader(timeout=timeout, exclude=old)
    seen = time.time() - t0
    st = cluster.status(new) or {}
    ready = [e["ts"] for e in st.get("events", [])
             if e["kind"] == "leader_ready" and e["ts"] >= t0]
    return round(min(ready) - t0, 2) if ready else round(seen, 2)


def wait_quiet(cluster, timeout=25):
    end = time.time() + timeout
    while time.time() < end:
        lead = cluster.leader()
        if lead and lead[1]["pending"] == 0:
            return True
        time.sleep(0.3)
    return False


def latencies(logs):
    sent, worst = {}, 0.0
    for p in logs:
        for line in open(p):
            e = json.loads(line)
            if e["ev"] == "SENT":
                sent.setdefault(e["logical"], e["ts"])
            elif e["ev"] == "ACK":
                worst = max(worst, e["ts"] - sent[e["logical"]])
    return round(worst, 2)


def run_trial(point, trial, root, dedup=True, n_clients=3, per_client=60):
    rng = random.Random(f"{point}-{trial}-{dedup}")
    work = os.path.join(root, f"{point}-{'dedup' if dedup else 'baseline'}-{trial}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    cluster = Cluster(work, chunk_size=CHUNK)
    row = {"point": point, "mode": "exactly-once" if dedup else "at-least-once", "trial": trial}
    try:
        cluster.start_all()
        cluster.wait_ready()
        threads, logs, clients, errors = run_clients(n_clients, per_client, work, dedup, rng)
        time.sleep(0.4)                                # arm while plenty of appends remain

        lead_id, lead_st = cluster.leader()
        if point.startswith("master."):
            target, port = lead_id, config.MASTERS[lead_id]
        else:
            last_chunk = lead_st["chunks"][-1]
            target = rng.choice(last_chunk["replicas"])
            port = config.CHUNKSERVERS[target]
        rpc.call(config.addr(port), "SetFailpoint", {"name": point, "count": 1})

        delay = round(rng.uniform(0.3, 4.0), 1)
        fired = False
        end = time.time() + 30
        while time.time() < end:
            if cluster.procs[target].poll() is not None:
                fired = True
                t_crash = time.time()
                if target in config.MASTERS:
                    try:
                        row["failover_s"] = failover_time(cluster, target, t_crash)
                    except TimeoutError:
                        row["failover_s"] = None
                time.sleep(max(0.0, delay - (time.time() - t_crash)))
                cluster.start(target)
                break
            time.sleep(0.05)
        row.update(target=target, fired=fired, restart_delay_s=delay)

        for t in threads:
            t.join(timeout=120)
        row["quiesced"] = wait_quiet(cluster)
        res = checker.check(FILE, logs)
        row.update(sent=res["logical_sent"], acked=res["logical_acked"],
                   visible=res["records_visible"], duplicates=len(res["duplicates"]),
                   lost=len(res["lost"]), mismatch=len(res["mismatch"]),
                   committed_unacked=len(res["committed_unacked"]),
                   orphans=res["orphan_index_entries"],
                   retries=sum(c.retries for c in clients),
                   max_append_s=latencies(logs), client_errors=len(errors),
                   # a trial only counts if the crash really happened mid-workload
                   ok=fired and res["ok"] and not errors
                   and res["logical_acked"] == n_clients * per_client)
        lead = cluster.leader()
        if lead:
            st = lead[1]["stats"]
            row.update(dedup_hits=st["dedup_hits"], aborted=st["aborted"],
                       dedup_size=lead[1]["dedup_size"])
        rec_c = rec_a = 0
        for m in config.MASTERS:                       # whichever master led the recovery
            s = cluster.status(m)
            if s:
                rec_c += s["reconciled"]["commit"]
                rec_a += s["reconciled"]["abort"]
        row["reconcile"] = f"{rec_c}c/{rec_a}a"
    except Exception as e:
        row.update(ok=False, error=f"{type(e).__name__}: {e}")
    finally:
        cluster.stop_all()
    return row


def print_table(rows, cols):
    widths = [max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    for r in rows:
        print("  ".join(str(r.get(c, "")).ljust(w) for c, w in zip(cols, widths)))


COLS = ["point", "mode", "trial", "target", "fired", "failover_s", "sent", "acked", "visible",
        "duplicates", "lost", "mismatch", "orphans", "retries", "dedup_hits", "aborted",
        "reconcile", "dedup_size", "max_append_s", "ok"]


def campaign(args, dedup):
    rows = []
    points = args.points or failpoints.POINTS
    for point in points:
        for trial in range(args.trials):
            row = run_trial(point, trial, args.out, dedup=dedup)
            rows.append(row)
            print(json.dumps(row), flush=True)
    print()
    print_table(rows, COLS)
    name = "campaign" if dedup else "baseline"
    json.dump(rows, open(os.path.join(args.out, f"{name}.json"), "w"), indent=1)
    bad = [r for r in rows if not r.get("ok")]
    print(f"\n{len(rows) - len(bad)}/{len(rows)} trials passed; "
          f"total duplicates={sum(r.get('duplicates', 0) for r in rows)} "
          f"lost={sum(r.get('lost', 0) for r in rows)}")
    return 0 if (dedup and not bad) or not dedup else 1


def repair(args):
    work = os.path.join(args.out, "repair")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    rng = random.Random("repair")
    cluster = Cluster(work, chunk_size=CHUNK)
    out = {}
    try:
        cluster.start_all()
        cluster.wait_ready()
        threads, logs, clients, errors = run_clients(2, 60, work, True, rng)
        time.sleep(1.5)
        lead_id, st = cluster.leader()
        victim = rng.choice(st["chunks"][-1]["replicas"])
        t_kill = time.time()
        cluster.kill(victim)
        t_degraded = t_healed = None
        end = time.time() + 40
        while time.time() < end:
            lead = cluster.leader()
            if lead:
                health = [c["health"] for c in lead[1]["chunks"]]
                holds_victim = any(victim in c["replicas"] for c in lead[1]["chunks"])
                if t_degraded is None and any(h != "healthy" for h in health):
                    t_degraded = time.time()                # degraded, or already repairing
                if t_degraded and not holds_victim and all(h == "healthy" for h in health):
                    t_healed = time.time()
                    break
            time.sleep(0.2)
        for t in threads:
            t.join(timeout=120)
        out.update(victim=victim, degraded_detected_s=round(t_degraded - t_kill, 1) if t_degraded else None,
                   fully_replicated_again_s=round(t_healed - t_kill, 1) if t_healed else None)
        lead = cluster.leader()
        chunks = lead[1]["chunks"]
        out["chunks"] = len(chunks)
        out["all_chunks_3_live_replicas"] = all(len(c["live"]) == 3 for c in chunks)
        out["repairs_committed"] = lead[1]["stats"]["repairs"]
        out["repaired_chunks_version_bumped"] = sum(1 for c in chunks if c["version"] > 1)

        cluster.start(victim)                          # the old server returns with stale copies
        time.sleep(4)
        held = set(cluster.status(victim)["chunks"]) if cluster.status(victim) else None
        lead = cluster.leader()
        mine = {str(c["id"]) for c in lead[1]["chunks"] if victim in c["replicas"]}
        out["returned_server_stale_chunks_left"] = sorted(held - mine) if held is not None else None
        res = checker.check(FILE, logs)
        out.update(visible=res["records_visible"], duplicates=len(res["duplicates"]),
                   lost=len(res["lost"]), mismatch=len(res["mismatch"]), checker_ok=res["ok"])
        out["ok"] = (res["ok"] and t_healed is not None and out["all_chunks_3_live_replicas"]
                     and not out["returned_server_stale_chunks_left"] and not errors
                     and out["repaired_chunks_version_bumped"] > 0)
    finally:
        cluster.stop_all()
    print(json.dumps(out, indent=1))
    json.dump(out, open(os.path.join(args.out, "repair.json"), "w"), indent=1)
    return 0 if out.get("ok") else 1


def restart(args):
    work = os.path.join(args.out, "restart")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    rng = random.Random("restart")
    cluster = Cluster(work, chunk_size=CHUNK)
    out = {}
    try:
        cluster.start_all()
        cluster.wait_ready()
        log = os.path.join(work, "c1.jsonl")
        cl = Client("c1", log)
        for n in range(30):
            cl.append(FILE, payload(f"c1-{n}", rng), f"c1-{n}")
        for m in config.MASTERS:                       # lose every master at once
            cluster.kill(m)
        time.sleep(1)
        for m in config.MASTERS:
            cluster.start(m)
        cluster.wait_ready()
        # replay the last acknowledged request by hand: the dedup table must have survived
        again = cl._master("AppendBegin", {"req_id": "c1:30", "client": "c1", "seq": 30,
                                           "file": FILE, "size": 1, "low_water": 29})
        # the client process restarts too: it must continue from its log, not reuse old IDs
        cl.log.close()
        cl2 = Client("c1", log)
        out["restarted_client_resumes_at_seq"] = cl2.seq + 1
        for n in range(30, 35):
            cl2.append(FILE, payload(f"c1-{n}", rng), f"c1-{n}")
        res = checker.check(FILE, [log])
        out.update(visible=res["records_visible"], duplicates=len(res["duplicates"]),
                   lost=len(res["lost"]), retry_status_after_restart=again["status"])
        out["ok"] = (res["ok"] and res["records_visible"] == 35 and again["status"] == "committed"
                     and out["restarted_client_resumes_at_seq"] == 31)
    finally:
        cluster.stop_all()
    print(json.dumps(out, indent=1))
    json.dump(out, open(os.path.join(args.out, "restart.json"), "w"), indent=1)
    return 0 if out.get("ok") else 1


def _wait(pred, timeout, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(step)
    return None


def snapshot(args):
    """A follower misses so much that the leader has already compacted its log: it must catch up
    through InstallSnapshot, then everything must also survive a restart from snapshot + log."""
    work = os.path.join(args.out, "snapshot")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    rng = random.Random("snapshot")
    cluster = Cluster(work, chunk_size=CHUNK, env={"GFS_SNAPSHOT_EVERY": "40"})
    out = {}
    try:
        cluster.start_all()
        lead = cluster.wait_ready()
        lagger = next(m for m in config.MASTERS if m != lead)
        cluster.kill(lagger)
        log = os.path.join(work, "c1.jsonl")
        cl = Client("c1", log)
        for n in range(150):
            cl.append(FILE, payload(f"c1-{n}", rng), f"c1-{n}")
        st = cluster.status(lead)
        out.update(leader=lead, lagging_follower=lagger, leader_snap_index=st["snap_index"],
                   leader_log_entries_kept=st["log_entries"], leader_commit=st["commit"])

        t0 = time.time()
        cluster.start(lagger)
        target = st["commit"]
        caught = _wait(lambda: (cluster.status(lagger) or {}).get("applied", 0) >= target, 20)
        out["follower_caught_up_s"] = round(time.time() - t0, 2) if caught else None
        lst = cluster.status(lagger) or {}
        events = [e["kind"] for e in lst.get("events", [])]
        out["follower_installed_snapshot"] = "snapshot_installed" in events
        out["follower_snap_index"] = lst.get("snap_index")

        cluster.kill(lead)                              # the caught-up follower may now lead
        new = cluster.wait_leader(exclude=lead)
        out["new_leader_after_kill"] = new
        for n in range(150, 170):
            cl.append(FILE, payload(f"c1-{n}", rng), f"c1-{n}")
        cluster.start(lead)

        for m in config.MASTERS:                        # full restart: rebuild from snapshot + log
            cluster.kill(m)
        for m in config.MASTERS:
            cluster.start(m)
        cluster.wait_ready()
        res = checker.check(FILE, [log])
        out.update(visible=res["records_visible"], duplicates=len(res["duplicates"]),
                   lost=len(res["lost"]))
        out["ok"] = (res["ok"] and res["records_visible"] == 170 and st["snap_index"] > 0
                     and out["follower_installed_snapshot"] and caught is not None)
    finally:
        cluster.stop_all()
    print(json.dumps(out, indent=1))
    json.dump(out, open(os.path.join(args.out, "snapshot.json"), "w"), indent=1)
    return 0 if out.get("ok") else 1


def partition(args):
    """Cut the leader off from the other two masters while clients and chunk servers can still
    reach it. The majority must elect a new leader, the old one must step down (check-quorum),
    and no append may be duplicated or lost across the split."""
    work = os.path.join(args.out, "partition")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    rng = random.Random("partition")
    cluster = Cluster(work, chunk_size=CHUNK)
    out = {}
    try:
        cluster.start_all()
        cluster.wait_ready()
        threads, logs, clients, errors = run_clients(3, 40, work, True, rng)
        time.sleep(1.0)
        old, st = cluster.leader()
        old_term = st["term"]
        t0 = time.time()
        cluster.partition(old)
        new = _wait(lambda: (lambda l: l[0] if l and l[0] != old else None)(cluster.leader()), 10)
        out.update(isolated_leader=old, new_leader=new,
                   new_leader_elected_s=round(time.time() - t0, 2) if new else None)
        down = _wait(lambda: (cluster.status(old) or {}).get("role") != "leader", 10)
        out["old_leader_stepped_down_s"] = round(time.time() - t0, 2) if down else None
        time.sleep(max(0.0, 4.0 - (time.time() - t0)))
        cluster.heal()
        t_heal = time.time()

        def rejoined():                                 # old master is current again
            lead, s = cluster.leader(), cluster.status(old)
            return bool(lead and s and s["applied"] >= lead[1]["commit"] - 5)
        ok_rejoin = _wait(rejoined, 15)
        out["old_master_rejoined_s"] = round(time.time() - t_heal, 2) if ok_rejoin else None
        for t in threads:
            t.join(timeout=120)
        wait_quiet(cluster)
        time.sleep(1.0)
        res = checker.check(FILE, logs)
        lead_id, lst = cluster.leader()
        sts = {m: cluster.status(m) for m in config.MASTERS}
        out["final_leader"], out["final_term"] = lead_id, lst["term"]
        out["terms_advanced"] = lst["term"] - old_term
        out["masters_agree"] = len({json.dumps([s["files"], s["stats"]["committed"]])
                                    for s in sts.values() if s}) == 1
        out.update(sent=res["logical_sent"], acked=res["logical_acked"],
                   visible=res["records_visible"], duplicates=len(res["duplicates"]),
                   lost=len(res["lost"]), retries=sum(c.retries for c in clients),
                   client_errors=len(errors))
        out["ok"] = (res["ok"] and new is not None and down is not None and ok_rejoin is not None
                     and out["masters_agree"] and not errors and res["logical_acked"] == 120)
    finally:
        cluster.stop_all()
    print(json.dumps(out, indent=1))
    json.dump(out, open(os.path.join(args.out, "partition.json"), "w"), indent=1)
    return 0 if out.get("ok") else 1


def failover(args, kills=20):
    """Kill the leader repeatedly on one cluster (with appends running) and measure the time until
    a new leader is elected and ready to serve."""
    work = os.path.join(args.out, "failover")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    rng = random.Random("failover")
    cluster = Cluster(work, chunk_size=CHUNK)
    times, observed, out = [], [], {}
    try:
        cluster.start_all()
        cluster.wait_ready()
        threads, logs, clients, errors = run_clients(2, 10_000, work, True, rng)
        for _ in range(kills):
            time.sleep(1.5)                            # let the cluster settle
            old = cluster.wait_leader()
            t0 = time.time()
            cluster.kill(old)
            new = cluster.wait_leader(timeout=15, exclude=old)
            seen = time.time() - t0
            # Cluster-side failover: kill -> the new leader has applied its first entry and serves.
            evs = [e for m in config.MASTERS if m != old
                   for e in (cluster.status(m) or {}).get("events", []) if e["ts"] >= t0 - 0.2]
            ready = [e["ts"] for e in evs if e["kind"] == "leader_ready" and e["node"] == new]
            times.append(round(min(ready) - t0, 2) if ready else round(seen, 2))
            observed.append(round(seen, 2))
            if times[-1] > 2.0:                         # keep the evidence for slow elections
                out.setdefault("slow_elections", []).append({
                    "seconds": times[-1], "events": sorted(
                        f"{e['ts'] - t0:+.2f}s {e['node']} {e['kind']} {e.get('term', '')}"
                        for e in evs)})
            time.sleep(0.5)
            cluster.start(old)
        for c in clients:                              # stop the endless clients
            c.deadline = 0
        times.sort()
        out.update({"kills": kills, "min_s": times[0], "p50_s": times[len(times) // 2],
                    "p95_s": times[int(0.95 * (len(times) - 1))], "max_s": times[-1],
                    "all_s": times, "harness_observed_max_s": max(observed)})
        out["ok"] = out["p95_s"] <= 2.0
    finally:
        cluster.stop_all()
    print(json.dumps(out, indent=1))
    json.dump(out, open(os.path.join(args.out, "failover.json"), "w"), indent=1)
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["campaign", "baseline", "repair", "restart", "snapshot",
                                     "partition", "failover", "all"])
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--points", nargs="*")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                  "..", "results"))
    args = ap.parse_args()
    args.out = os.path.abspath(args.out)
    os.makedirs(args.out, exist_ok=True)
    codes = []
    if args.mode in ("campaign", "all"):
        codes.append(campaign(args, True))
    if args.mode in ("baseline", "all"):
        codes.append(campaign(args, False))
    if args.mode in ("repair", "all"):
        codes.append(repair(args))
    if args.mode in ("restart", "all"):
        codes.append(restart(args))
    if args.mode in ("snapshot", "all"):
        codes.append(snapshot(args))
    if args.mode in ("partition", "all"):
        codes.append(partition(args))
    if args.mode in ("failover", "all"):
        codes.append(failover(args))
    sys.exit(1 if any(codes) else 0)
