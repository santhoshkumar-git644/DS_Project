"""Master node: one of three Raft-replicated masters. Only the leader serves clients.

Append protocol (leader side):
  AppendBegin   log INTENT (reserves chunk + offset, deduplicated by request ID)
  client writes the record to all 3 replicas (each fsyncs data, then its index)
  AppendCommit  verify every replica holds it, then log DONE; the ack is sent only after DONE is
                committed by a Raft majority
A background loop on the leader settles intents left unfinished by a crashed leader or client, and
repairs chunks that lost a replica.
"""
import argparse
import collections
import os
import threading
import time

from . import config, failpoints as fp, rpc
from .raft import NotLeader, ProposeFailed, Raft
from .statemachine import MasterState


class Master:
    def __init__(self, mid):
        self.id = mid
        self.state = MasterState()
        self.events = collections.deque(maxlen=400)
        self._event_seq = 0
        self._ev_lock = threading.Lock()
        self.last_seen = {}              # chunk server id -> {"ts": wall time, "chunks": {id: version}}
        self.repairing = {}              # chunk id -> target server id
        self.known_dead = set()
        self.reconciled = {"commit": 0, "abort": 0}   # this master's own work as leader
        peers = {m: a for m, a in config.master_addrs().items() if m != mid}
        self.raft = Raft(mid, peers, os.path.join(config.DATA_DIR, mid), self.state.apply,
                         snapshot_fn=self.state.dump, restore_fn=self.state.load, emit=self.emit)
        self.server = rpc.serve(config.MASTERS[mid], {
            "RequestVote": self.raft.on_request_vote,
            "AppendEntries": self.raft.on_append_entries,
            "InstallSnapshot": self.raft.on_install_snapshot,
            "SetPartition": self.set_partition,
            "AppendBegin": self._client(self.append_begin),
            "AppendCommit": self._client(self.append_commit),
            "ListRecords": self._client(self.list_records),
            "Heartbeat": self.heartbeat,
            "Status": self.status,
            "SetFailpoint": self.set_failpoint,
        })
        threading.Thread(target=self._monitor, daemon=True).start()

    # ------------------------------------------------------------------ helpers
    def emit(self, kind, **kw):
        with self._ev_lock:
            self._event_seq += 1
            self.events.append({"seq": self._event_seq, "ts": time.time(), "node": self.id,
                                "kind": kind, **kw})

    def _client(self, fn):
        """Wrap a client-facing handler: redirect if not leader, surface Raft failures as errors."""
        def wrapped(req):
            if not self.raft.ready():
                st = self.raft.status()
                if st["role"] == "leader":
                    return {"error": "not_ready"}
                lid = st["leader"]
                return {"error": "not_leader",
                        "leader": config.master_addrs().get(lid) if lid else None}
            try:
                return fn(req)
            except NotLeader as e:
                return {"error": "not_leader",
                        "leader": config.master_addrs().get(e.leader) if e.leader else None}
            except ProposeFailed as e:
                return {"error": f"propose_failed: {e}"}
        return wrapped

    def _live(self):
        now = time.time()
        return {s for s, v in list(self.last_seen.items()) if now - v["ts"] < config.DEAD_AFTER}

    def _addr(self, server_id):
        return config.chunkserver_addrs()[server_id]

    def _replica_list(self, ids):
        return [[s, self._addr(s)] for s in ids]

    # ------------------------------------------------------------------ append path
    def _choose_chunk(self, file):
        with self.state.lock:
            f = self.state.files.get(file)
            last = f["chunks"][-1] if f and f["chunks"] else None
            ch = dict(self.state.chunks[last]) if last is not None else None
        if f is None:
            self.raft.propose({"op": "CREATE", "file": file})
        live = self._live()
        if ch is not None and not ch["sealed"] and all(s in live for s in ch["replicas"]):
            return last
        if len(live) < config.REPLICATION:
            raise ProposeFailed("fewer than 3 live chunk servers")
        if ch is not None and not ch["sealed"]:                 # degraded: stop writing to it
            self.raft.propose({"op": "SEAL", "chunk": last})
            self.emit("sealed_degraded", chunk=last)
        load = self.state.server_load()
        replicas = sorted(live, key=lambda s: (load.get(s, 0), s))[:config.REPLICATION]
        r = self.raft.propose({"op": "ADD_CHUNK", "file": file, "after": last,
                               "replicas": replicas})
        if r["status"] == "nofile":
            raise ProposeFailed("file vanished")
        return r["chunk"]

    def _reply(self, rec):
        with self.state.lock:
            ch = self.state.chunks[rec["chunk"]]
            replicas = list(ch["replicas"])
        return {"status": rec["status"], "chunk": rec["chunk"], "offset": rec["offset"],
                "version": rec["version"], "attempt": rec["attempt"],
                "replicas": self._replica_list(replicas)}

    def append_begin(self, req):
        fp.hit("master.before_intent")
        for _ in range(6):
            cid = self._choose_chunk(req["file"])
            r = self.raft.propose({"op": "INTENT", "req": req["req_id"], "client": req["client"],
                                   "seq": req["seq"], "file": req["file"], "chunk": cid,
                                   "size": req["size"], "ts": time.time(),
                                   "low_water": req.get("low_water")})
            if r["status"] in ("intent", "committed"):
                if r.get("dedup"):
                    self.emit("dedup_hit", req=req["req_id"], was=r["status"],
                              chunk=r["chunk"], offset=r["offset"])
                fp.hit("master.after_intent")
                return self._reply(r)
        return {"error": "no_chunk_available"}

    def _has(self, server, chunk, req_id, offset, size):
        try:
            r = rpc.call(self._addr(server), "Has", {"chunk": chunk, "req": req_id,
                                                     "offset": offset, "size": size}, timeout=0.6)
            return bool(r.get("has"))
        except rpc.RpcError:
            return None

    def append_commit(self, req):
        req_id = req["req_id"]
        with self.state.lock:
            rec = dict(self.state.appends.get(req_id) or {})
            replicas = list(self.state.chunks[rec["chunk"]]["replicas"]) if rec else []
        if not rec:
            return {"error": "unknown_request"}
        if rec["state"] == "committed":
            return self._reply(dict(rec, status="committed"))
        if rec["state"] == "aborted":
            return {"status": "aborted"}
        if not all(self._has(s, rec["chunk"], req_id, rec["offset"], rec["size"]) is True
                   for s in replicas):
            return {"status": "incomplete"}
        fp.hit("master.before_done")
        r = self.raft.propose({"op": "DONE", "req": req_id})
        fp.hit("master.after_done")
        if r["status"] == "committed":
            return self._reply(r)
        return {"status": r["status"]}

    def list_records(self, req):
        self.raft.propose({"op": "NOOP"})               # barrier: read only after a majority agrees
        with self.state.lock:
            f = self.state.files.get(req["file"])
            if f is None:
                return {"records": [], "chunks": {}}
            records = list(f["records"])
            chunks = {str(c): self._replica_list(self.state.chunks[c]["replicas"])
                      for c in f["chunks"]}
        return {"records": records, "chunks": chunks}

    # ------------------------------------------------------------------ heartbeats, status
    def heartbeat(self, req):
        sid, now = req["id"], time.time()
        prev = self.last_seen.get(sid)
        self.last_seen[sid] = {"ts": now, "chunks": req["chunks"]}
        if prev is None or now - prev["ts"] > config.DEAD_AFTER:
            self.known_dead.discard(sid)
            self.emit("server_up", server=sid)
        reply = {"delete": [], "set_version": {}}
        if self.raft.ready():
            with self.state.lock:
                for cid_s, ver in req["chunks"].items():
                    cid = int(cid_s)
                    ch = self.state.chunks.get(cid)
                    if ch is None or self.repairing.get(cid) == sid:
                        continue        # unknown to us (we may be a stale leader) or mid-repair
                    if sid not in ch["replicas"]:
                        # Delete only a copy we KNOW is outdated: the chunk moved on to a newer
                        # version without this server. A fresh repair copy carries the newer
                        # version, so a leader with stale metadata never deletes it.
                        if ver < ch["version"]:
                            reply["delete"].append(cid)
                    elif ver < ch["version"]:
                        reply["set_version"][cid_s] = ch["version"]
        return reply

    def status(self, req):
        st = self.raft.status()
        snap = self.state.snapshot()
        live = self._live()
        chunks = []
        for cid, c in sorted(snap["chunks"].items()):
            alive = [s for s in c["replicas"] if s in live]
            health = ("repairing" if cid in self.repairing else
                      "healthy" if len(alive) == len(c["replicas"]) else "degraded")
            chunks.append({"id": cid, "file": c["file"], "replicas": c["replicas"],
                           "live": alive, "version": c["version"], "sealed": c["sealed"],
                           "used": c["next"], "health": health})
        now = time.time()
        servers = {s: {"live": s in live,
                       "age": round(now - self.last_seen[s]["ts"], 1) if s in self.last_seen else None,
                       "chunks": len(self.last_seen[s]["chunks"]) if s in self.last_seen else 0}
                   for s in config.CHUNKSERVERS}
        since = req.get("since", 0)
        with self._ev_lock:
            events = [e for e in self.events if e["seq"] > since]
        return {**st, "event_seq": self._event_seq, "reconciled": dict(self.reconciled),
                "partitioned_from": sorted(rpc.BLOCKED), "chunks": chunks, "servers": servers,
                "stats": snap["stats"],
                "pending": snap["pending"], "dedup_size": snap["dedup_size"],
                "files": snap["files"], "events": events}

    def set_failpoint(self, req):
        fp.arm(req["name"], req.get("count", 1))
        return {"ok": True}

    def set_partition(self, req):
        """Drop all RPCs from this master to the given addresses (empty list = heal)."""
        rpc.BLOCKED.clear()
        rpc.BLOCKED.update(req.get("blocked", []))
        self.emit("partition" if rpc.BLOCKED else "partition_healed",
                  blocked=sorted(rpc.BLOCKED))
        return {"ok": True}

    # ------------------------------------------------------------------ leader background work
    def _monitor(self):
        while True:
            time.sleep(0.5)
            try:
                if self.raft.ready():
                    self._liveness_events()
                    self._reconcile()
                    self._repair()
            except Exception as e:
                self.emit("monitor_error", error=f"{type(e).__name__}: {e}")

    def _liveness_events(self):
        live = self._live()
        for s in list(self.last_seen):
            if s not in live and s not in self.known_dead:
                self.known_dead.add(s)
                self.emit("server_dead", server=s)

    def _reconcile(self):
        """Settle intents whose writer crashed: commit if every replica has the record, else abort."""
        now = time.time()
        with self.state.lock:
            pending = [(r, dict(rec), list(self.state.chunks[rec["chunk"]]["replicas"]))
                       for r, rec in self.state.appends.items()
                       if rec["state"] == "intent" and now - rec["ts"] > config.RECONCILE_GRACE]
        for req_id, rec, replicas in pending:
            answers = [self._has(s, rec["chunk"], req_id, rec["offset"], rec["size"])
                       for s in replicas]
            if all(a is True for a in answers):
                r = self.raft.propose({"op": "DONE", "req": req_id})
                if r["status"] == "committed":
                    self.reconciled["commit"] += 1
                    self.emit("reconcile_commit", req=req_id, chunk=rec["chunk"])
            elif any(a is None for a in answers) and now - rec["ts"] <= config.ABORT_AFTER:
                continue                                        # replica may come back; wait
            else:
                r = self.raft.propose({"op": "ABORT", "req": req_id})
                if r["status"] == "aborted":
                    self.reconciled["abort"] += 1
                    self.emit("reconcile_abort", req=req_id, chunk=rec["chunk"])

    def _repair(self):
        live = self._live()
        load = self.state.server_load()
        with self.state.lock:
            chunks = [(cid, dict(c)) for cid, c in self.state.chunks.items()]
        for cid, ch in chunks:
            if cid in self.repairing:
                continue
            dead = [s for s in ch["replicas"] if s not in live]
            sources = [s for s in ch["replicas"] if s in live]
            targets = sorted((s for s in live if s not in ch["replicas"]),
                             key=lambda s: (load.get(s, 0), s))
            if not dead or not sources or not targets:
                continue
            self.repairing[cid] = targets[0]
            threading.Thread(target=self._do_repair,
                             args=(cid, ch, dead[0], sources[0], targets[0]), daemon=True).start()

    def _do_repair(self, cid, ch, dead, source, target):
        t0 = time.time()
        try:
            if not ch["sealed"]:
                self.raft.propose({"op": "SEAL", "chunk": cid})
            self.emit("repair_start", chunk=cid, lost=dead, source=source, target=target)
            new_version = ch["version"] + 1
            rpc.call(self._addr(target), "CopyChunk",
                     {"chunk": cid, "source": self._addr(source), "version": new_version},
                     timeout=30)
            replicas = [s for s in ch["replicas"] if s != dead] + [target]
            r = self.raft.propose({"op": "SET_REPLICAS", "chunk": cid, "replicas": replicas,
                                   "expect_version": ch["version"], "version": new_version})
            self.emit("repair_done" if r["status"] == "ok" else "repair_conflict", chunk=cid,
                      seconds=round(time.time() - t0, 2))
        except Exception as e:
            self.emit("repair_failed", chunk=cid, error=f"{type(e).__name__}: {e}")
        finally:
            self.repairing.pop(cid, None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, choices=sorted(config.MASTERS))
    args = ap.parse_args()
    Master(args.id)
    print(f"master {args.id} listening on {config.MASTERS[args.id]}", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
