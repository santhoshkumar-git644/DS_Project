"""Presentation demo: the cluster, two client workloads, and a dashboard with a control panel.

    python demo.py                 start everything and open http://127.0.0.1:8765
    python demo.py --autoplay      also run the full scenario automatically
    python demo.py --cleanup       kill node processes left over from a crashed run

In the browser you can crash the leader at any of the 4 master crash points, crash a chunk server
mid-write, partition the leader, kill/restart any node, and press "Verify" to run the independent
checker against both workloads:
    exactly-once   clients reuse their request ID on retry       -> expect 0 duplicates
    at-least-once  clients take a fresh ID on retry (plain GFS)  -> duplicates after master crashes
Press Ctrl+C to stop; every node process is shut down.
"""
import argparse
import collections
import os
import random
import shutil
import sys
import threading
import time
import webbrowser

from gfs import checker, config, failpoints, rpc
from gfs.client import AppendFailed, Client
from gfs.cluster import ROOT, Cluster, busy_ports, cleanup_stale
from gfs.dashboard import Dashboard

POINT_STORY = {
    "master.before_intent": "the next append reaches the leader, which dies before logging anything. "
                            "The client retries on the new leader; nothing was reserved.",
    "master.after_intent": "the leader logs the intent (offset reserved, replicated to a majority) and "
                           "dies before replying. The retry finds the same intent and reuses its offset.",
    "master.before_done": "all 3 replicas hold the record, but the leader dies before committing "
                          "completion. The new leader's reconcile (or the retry) commits it exactly once.",
    "master.after_done": "completion is committed, then the leader dies before telling the client. "
                         "The client never got its ack, so it retries - and must NOT append twice.",
    "cs.mid_write": "a chunk server writes the record but dies before fsyncing it. That replica "
                    "never has it, so the append is aborted and redone at a new offset.",
}


def now():
    return time.strftime("%H:%M:%S")


class Workload:
    """Three clients appending continuously to one file, in one retry mode."""

    def __init__(self, mode, work, n=3, pause=0.12):
        self.mode, self.dedup = mode, mode == "exactly-once"
        self.file = "/" + mode
        self.stop_ev = threading.Event()
        self.clients, self.logs, self.threads, self.errors = [], [], [], []
        self.rng = random.Random(mode)
        for i in range(1, n + 1):
            name = f"{'x' if self.dedup else 'b'}{i}"
            log = os.path.join(work, f"{mode}-{name}.jsonl")
            self.logs.append(log)
            self.clients.append(Client(name, log, dedup=self.dedup, deadline=600))
        self.n_sent = collections.Counter()
        self.pause = pause

    def _run(self, cl):
        n = 0
        while not self.stop_ev.is_set():
            logical = f"{cl.name}-{n}"
            try:
                self.n_sent[cl.name] += 1
                cl.append(self.file, f"{logical}|{'x' * self.rng.randint(40, 200)}".encode(), logical)
            except AppendFailed as e:
                self.errors.append(str(e))
            n += 1
            time.sleep(self.pause)

    def start(self):
        self.stop_ev.clear()
        self.threads = [threading.Thread(target=self._run, args=(c,), daemon=True)
                        for c in self.clients]
        for t in self.threads:
            t.start()

    def stop(self):
        self.stop_ev.set()

    @property
    def running(self):
        return any(t.is_alive() for t in self.threads) and not self.stop_ev.is_set()

    def summary(self):
        acked = sum(c.acked for c in self.clients)
        return {"mode": self.mode, "file": self.file, "running": self.running,
                "acked": acked, "in_flight": sum(self.n_sent.values()) - acked,
                "retries": sum(c.retries for c in self.clients),
                "last_error": next((c.last_error for c in self.clients if c.last_error), None),
                "errors": len(self.errors)}


class Controller:
    def __init__(self, cluster, work):
        self.cluster, self.work = cluster, work
        self.notes = collections.deque(maxlen=40)
        self.workloads = {m: Workload(m, work) for m in ("exactly-once", "at-least-once")}
        self.expected_down = set()
        self.auto_restart = True
        self.restart_delay = 3.0
        self.busy = None
        self.verify = None
        self.leader = None
        self._partition_gen = 0
        self._handled = set()
        threading.Thread(target=self._supervise, daemon=True).start()

    # ------------------------------------------------------------------ narration
    def note(self, text, level="info"):
        self.notes.appendleft({"ts": time.time(), "text": text, "level": level})
        print(f"[{now()}] {text}", flush=True)

    # ------------------------------------------------------------------ supervisor
    def _supervise(self):
        while True:
            lead = self.cluster.leader()
            if lead:
                self.leader = lead[0]
            for node, p in list(self.cluster.procs.items()):
                if p.poll() is None or node in self.expected_down or (node, p.pid) in self._handled:
                    continue
                self._handled.add((node, p.pid))
                how = "at the injected crash point" if p.returncode == 99 else f"(exit {p.returncode})"
                self.note(f"{node} crashed {how}.", "bad")
                threading.Thread(target=self._after_crash, args=(node,), daemon=True).start()
            time.sleep(0.15)

    def _after_crash(self, node):
        t0 = time.time()
        if node in config.MASTERS:
            try:
                new = self.cluster.wait_leader(timeout=10, exclude=node)
                self.note(f"Raft elected {new} as the new leader {time.time() - t0:.2f}s after the "
                          "crash. Clients retry the same request IDs against it.", "good")
            except TimeoutError:
                self.note("No new leader yet - is a majority of masters up?", "bad")
        if self.auto_restart:
            time.sleep(max(0.0, self.restart_delay - (time.time() - t0)))
            if self.cluster.procs[node].poll() is not None and node not in self.expected_down:
                self.cluster.start(node)
                what = "replays its log and catches up as a follower" if node in config.MASTERS \
                    else "rejoins; heartbeats resume"
                self.note(f"{node} restarted and {what}.", "info")

    # ------------------------------------------------------------------ helpers
    def _leader(self):
        lead = self.cluster.leader()
        if not lead:
            raise RuntimeError("no leader right now - wait a second and try again")
        return lead

    def _ensure_workload(self):
        if not any(w.running for w in self.workloads.values()):
            self.workloads["exactly-once"].start()
            self.note("Started the exactly-once workload so there are appends in flight.")
            time.sleep(1.0)

    # ------------------------------------------------------------------ actions
    def act(self, name, args):
        fn = getattr(self, "do_" + name, None)
        if fn is None:
            return {"error": f"unknown action {name}"}
        return fn(**args) or {"ok": True}

    def do_workload(self, mode, on):
        w = self.workloads[mode]
        if on and not w.running:
            w.start()
            how = ("reuse the same request ID on every retry" if w.dedup else
                   "take a NEW request ID on every retry, like plain GFS")
            self.note(f"Started 3 {mode} clients appending to {w.file}. They {how}.")
        elif not on and w.running:
            w.stop()
            self.note(f"Stopped the {mode} clients.")

    def do_crash_leader(self, point="master.after_done"):
        self._ensure_workload()
        leader, _ = self._leader()
        rpc.call(config.addr(config.MASTERS[leader]), "SetFailpoint", {"name": point, "count": 1})
        self.note(f"Armed '{point}' on leader {leader}: {POINT_STORY[point]}", "warn")

    def do_crash_chunkserver(self):
        self._ensure_workload()
        _, st = self._leader()
        mine = [c for c in st["chunks"] if c["file"] == "/exactly-once"] or st["chunks"]
        if not mine:
            raise RuntimeError("no chunks yet")
        target = random.choice([s for s in mine[-1]["replicas"] if s in mine[-1]["live"]])
        rpc.call(config.addr(config.CHUNKSERVERS[target]), "SetFailpoint",
                 {"name": "cs.mid_write", "count": 1})
        self.note(f"Armed 'cs.mid_write' on chunk server {target}: {POINT_STORY['cs.mid_write']}",
                  "warn")

    def _forget(self, node):
        """A process we stopped on purpose must never be reported as a crash."""
        p = self.cluster.procs.get(node)
        if p:
            self._handled.add((node, p.pid))

    def do_kill(self, node):
        self.expected_down.add(node)
        self._forget(node)
        self.cluster.kill(node)
        extra = (" Its chunks become degraded after the 2.5 s heartbeat timeout, then get copied "
                 "to a spare server." if node in config.CHUNKSERVERS else
                 " The other two masters still form a majority." if node in config.MASTERS else "")
        self.note(f"Killed {node} (stays down until you restart it).{extra}", "bad")

    def do_restart(self, node):
        self._forget(node)
        if not self.cluster.alive(node):
            self.cluster.start(node)
            self.expected_down.discard(node)
            extra = (" Copies of chunks that were re-homed while it was away are stale and get "
                     "deleted on its next heartbeat." if node in config.CHUNKSERVERS else "")
            self.note(f"Restarted {node}.{extra}", "info")

    def do_kill_replica(self):
        _, st = self._leader()
        mine = [c for c in st["chunks"] if c["file"] == "/exactly-once"] or st["chunks"]
        if not mine:
            raise RuntimeError("no chunks yet")
        self.do_kill(mine[-1]["replicas"][0])

    def do_partition(self, seconds=8):
        leader, _ = self._leader()
        self.cluster.partition(leader)
        self._partition_gen += 1
        gen = self._partition_gen
        self.note(f"Partitioned leader {leader} from the other masters (clients and chunk servers "
                  f"can still reach it). The majority elects a new leader; {leader} cannot reach a "
                  f"quorum, so it steps down. Healing in {seconds}s.", "warn")

        def heal_later():
            time.sleep(seconds)
            if self._partition_gen == gen:
                self.do_heal()
        threading.Thread(target=heal_later, daemon=True).start()

    def do_heal(self):
        self._partition_gen += 1
        self.cluster.heal()
        self.note("Partition healed. The old leader rejoins as a follower; pre-vote kept it from "
                  "inflating its term, so the current leader is not disturbed.", "good")

    def do_auto_restart(self, on):
        self.auto_restart = bool(on)
        self.note(f"Auto-restart of crashed nodes {'on' if on else 'off'}.")

    def do_verify(self):
        out = {}
        for mode, w in self.workloads.items():
            if not any(os.path.exists(p) for p in w.logs):
                continue
            res = checker.check(w.file, [p for p in w.logs if os.path.exists(p)])
            out[mode] = {"acked": res["logical_acked"], "visible": res["records_visible"],
                         "duplicates": sum(n - 1 for n in res["duplicates"].values()),
                         "lost": len(res["lost"]), "mismatch": len(res["mismatch"]),
                         "in_flight": len(res["committed_unacked"]), "ok": res["ok"]}
        if not out:
            raise RuntimeError("start a workload first")
        self.verify = {"ts": time.time(), "results": out}
        parts = [f"{m}: {r['acked']} acked, {r['visible']} visible, {r['duplicates']} duplicate(s), "
                 f"{r['lost']} lost" for m, r in out.items()]
        self.note("Checker: " + " | ".join(parts),
                  "good" if out.get("exactly-once", {}).get("ok", True) else "bad")
        return {"ok": True, "results": out}

    def do_autoplay(self):
        if self.busy:
            return {"error": "already running"}
        threading.Thread(target=self._autoplay, daemon=True).start()

    def _autoplay(self):
        self.busy = "autoplay"
        try:
            self.note("Autoplay: the full scenario takes about 2 minutes.")
            self.do_workload("exactly-once", True)
            self.do_workload("at-least-once", True)
            time.sleep(6)
            self.note("Scene 1 - crash the leader after it commits an append but before it replies.")
            self.do_crash_leader("master.after_done")
            time.sleep(9)
            self.note("Scene 1b - crash the next leader after the replicas have the record but "
                      "before it commits completion.")
            self.do_crash_leader("master.before_done")
            time.sleep(9)
            self.do_verify()
            time.sleep(6)
            self.note("Scene 2 - a chunk server dies in the middle of writing a record.")
            self.do_crash_chunkserver()
            time.sleep(10)
            self.note("Scene 3 - network partition isolates the leader.")
            self.do_partition(seconds=7)
            time.sleep(12)
            self.note("Scene 4 - a chunk server dies for good; watch its chunks get re-replicated.")
            self.do_kill_replica()
            time.sleep(12)
            down = [n for n in self.expected_down if n in config.CHUNKSERVERS]
            for n in down:
                self.do_restart(n)
            time.sleep(6)
            self.do_workload("exactly-once", False)
            self.do_workload("at-least-once", False)
            time.sleep(3)
            self.do_verify()
            self.note("Autoplay finished. Exactly-once: no duplicates, nothing lost. At-least-once: "
                      "the same crashes left duplicate records.", "good")
        except Exception as e:
            self.note(f"Autoplay stopped: {e}", "bad")
        finally:
            self.busy = None

    # ------------------------------------------------------------------ state for the page
    def state(self):
        return {"notes": list(self.notes)[:8],
                "workloads": {m: w.summary() for m, w in self.workloads.items()},
                "verify": self.verify, "auto_restart": self.auto_restart, "busy": self.busy,
                "down": sorted(self.expected_down),
                "alive": {n: self.cluster.alive(n) for n in
                          list(config.MASTERS) + list(config.CHUNKSERVERS)},
                "points": failpoints.POINTS}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--autoplay", action="store_true", help="run the full scenario automatically")
    ap.add_argument("--cleanup", action="store_true", help="kill leftover node processes and exit")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    if args.cleanup:
        print(f"killed {cleanup_stale()} leftover processes; busy ports: {busy_ports()}")
        return
    busy = busy_ports()
    if busy:
        killed = cleanup_stale()
        time.sleep(1.0)
        busy = busy_ports()
        if busy:
            sys.exit(f"ports {busy} are in use (another demo still running?). Close it, or run "
                     "'python demo.py --cleanup'.")
        print(f"cleaned up {killed} leftover processes from a previous run")

    work = os.path.join(ROOT, "run")
    shutil.rmtree(work, ignore_errors=True)
    cluster = Cluster(work, chunk_size=8192, env={"GFS_SNAPSHOT_EVERY": "300"})
    ctl = Controller(cluster, work)
    try:
        cluster.start_all()
        dash = Dashboard(ctl)
        dash.start()
        url = f"http://{config.HOST}:{config.DASHBOARD_PORT}"
        cluster.wait_ready()
        ctl.note(f"Cluster ready: 3 Raft masters, 5 chunk servers. Leader is "
                 f"{cluster.leader()[0]}. Start a workload, then break something.", "good")
        print(f"\n  Dashboard: {url}\n  Ctrl+C to stop.\n", flush=True)
        if not args.no_browser and not os.environ.get("GFS_NO_BROWSER"):
            webbrowser.open(url)
        if args.autoplay:
            ctl.do_autoplay()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        for w in ctl.workloads.values():
            w.stop()
        for n in list(cluster.procs):                 # do not report shutdown as crashes
            ctl.expected_down.add(n)
            ctl._forget(n)
        cluster.stop_all()


if __name__ == "__main__":
    main()
