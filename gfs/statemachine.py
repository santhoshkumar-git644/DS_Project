"""Master metadata as a deterministic state machine. Raft applies the same commands in the same
order on all three masters, so every master ends with identical state.

Append lifecycle in the dedup table (keyed by request ID "client:seq"):
    INTENT  -> state "intent"     offset reserved, no ack allowed yet
    DONE    -> state "committed"  record is now visible to readers (this is the ack point)
    ABORT / SEAL / SET_REPLICAS -> state "aborted"; a retry of the same ID reserves a new offset

A record exists for readers if and only if its DONE entry is committed.
"""
import json
import threading

from . import config


class MasterState:
    def __init__(self):
        self.lock = threading.RLock()
        self.files = {}          # name -> {"chunks": [ids], "records": [{req, chunk, offset, size}]}
        self.chunks = {}         # id -> {file, replicas, version, next, sealed}
        self.appends = {}        # req -> {state, file, chunk, offset, size, ts, attempt, client, seq}
        self.client_reqs = {}    # client -> set of req ids still in the dedup table
        self.next_chunk = 1
        self.stats = dict(dedup_hits=0, committed=0, aborted=0, intents=0, trimmed=0,
                          sealed=0, repairs=0)

    def apply(self, cmd, index):
        with self.lock:
            return getattr(self, "op_" + cmd["op"].lower())(cmd)

    # ------------------------------------------------------------------ commands
    def op_noop(self, cmd):
        return {"status": "ok"}

    def op_create(self, cmd):
        self.files.setdefault(cmd["file"], {"chunks": [], "records": []})
        return {"status": "ok"}

    def op_add_chunk(self, cmd):
        f = self.files.get(cmd["file"])
        if f is None:
            return {"status": "nofile"}
        last = f["chunks"][-1] if f["chunks"] else None
        if last != cmd["after"]:                       # someone else already added one
            return {"status": "exists", "chunk": last}
        cid = self.next_chunk
        self.next_chunk += 1
        self.chunks[cid] = {"file": cmd["file"], "replicas": list(cmd["replicas"]), "version": 1,
                            "next": 0, "sealed": False}
        f["chunks"].append(cid)
        return {"status": "ok", "chunk": cid}

    def op_intent(self, cmd):
        req = cmd["req"]
        rec = self.appends.get(req)
        attempt = 1
        if rec is not None:
            if rec["state"] in ("committed", "intent"):  # retry of a known request: deduplicate
                self.stats["dedup_hits"] += 1
                return dict(rec, status=rec["state"], dedup=True)
            attempt = rec["attempt"] + 1                 # aborted earlier: reserve a fresh offset
        chunk = self.chunks.get(cmd["chunk"])
        if chunk is None or chunk["sealed"]:
            return {"status": "sealed"}
        if chunk["next"] + cmd["size"] > config.CHUNK_SIZE:
            chunk["sealed"] = True
            self.stats["sealed"] += 1
            return {"status": "full"}
        rec = {"state": "intent", "file": cmd["file"], "chunk": cmd["chunk"],
               "offset": chunk["next"], "size": cmd["size"], "ts": cmd["ts"],
               "attempt": attempt, "client": cmd["client"], "seq": cmd["seq"],
               "version": chunk["version"]}
        chunk["next"] += cmd["size"]
        self.appends[req] = rec
        self.client_reqs.setdefault(cmd["client"], set()).add(req)
        self.stats["intents"] += 1
        self._trim(cmd["client"], cmd.get("low_water"))
        return dict(rec, status="intent")

    def _trim(self, client, low_water):
        """The client will never send a seq <= low_water again, so finished entries can go.
        An entry still in "intent" is kept: the reconcile loop must settle it first."""
        if low_water is None:
            return
        for req in list(self.client_reqs.get(client, ())):
            rec = self.appends[req]
            if rec["seq"] <= low_water and rec["state"] in ("committed", "aborted"):
                del self.appends[req]
                self.client_reqs[client].discard(req)
                self.stats["trimmed"] += 1

    def op_done(self, cmd):
        rec = self.appends.get(cmd["req"])
        if rec is None:
            return {"status": "unknown"}
        if rec["state"] == "committed":
            return dict(rec, status="committed")
        if rec["state"] == "aborted":
            return {"status": "aborted"}
        rec["state"] = "committed"
        self.files[rec["file"]]["records"].append(
            {"req": cmd["req"], "chunk": rec["chunk"], "offset": rec["offset"], "size": rec["size"]})
        self.stats["committed"] += 1
        return dict(rec, status="committed")

    def op_abort(self, cmd):
        rec = self.appends.get(cmd["req"])
        if rec is not None and rec["state"] == "intent":
            rec["state"] = "aborted"
            self.stats["aborted"] += 1
        return {"status": rec["state"] if rec else "unknown"}

    def _abort_chunk_intents(self, cid):
        for rec in self.appends.values():
            if rec["chunk"] == cid and rec["state"] == "intent":
                rec["state"] = "aborted"
                self.stats["aborted"] += 1

    def op_seal(self, cmd):
        chunk = self.chunks.get(cmd["chunk"])
        if chunk is not None and not chunk["sealed"]:
            chunk["sealed"] = True
            self.stats["sealed"] += 1
            self._abort_chunk_intents(cmd["chunk"])
        return {"status": "ok"}

    def op_set_replicas(self, cmd):
        chunk = self.chunks.get(cmd["chunk"])
        if chunk is None or chunk["version"] != cmd["expect_version"]:
            return {"status": "conflict"}
        chunk["replicas"] = list(cmd["replicas"])
        chunk["version"] = cmd["version"]
        self._abort_chunk_intents(cmd["chunk"])
        self.stats["repairs"] += 1
        return {"status": "ok"}

    # ------------------------------------------------------------------ snapshots
    def dump(self):
        """JSON-safe deep copy of the whole state (Raft snapshot payload)."""
        with self.lock:
            return json.loads(json.dumps({
                "files": self.files, "chunks": self.chunks, "appends": self.appends,
                "client_reqs": {c: sorted(r) for c, r in self.client_reqs.items()},
                "next_chunk": self.next_chunk, "stats": self.stats}))

    def load(self, d):
        with self.lock:
            self.files = d["files"]
            self.chunks = {int(k): v for k, v in d["chunks"].items()}
            self.appends = d["appends"]
            self.client_reqs = {c: set(r) for c, r in d["client_reqs"].items()}
            self.next_chunk = d["next_chunk"]
            self.stats = d["stats"]

    # ------------------------------------------------------------------ queries
    def server_load(self):
        with self.lock:
            load = {}
            for c in self.chunks.values():
                for s in c["replicas"]:
                    load[s] = load.get(s, 0) + 1
            return load

    def snapshot(self):
        with self.lock:
            return {"chunks": {cid: dict(c) for cid, c in self.chunks.items()},
                    "pending": sum(1 for r in self.appends.values() if r["state"] == "intent"),
                    "dedup_size": len(self.appends), "stats": dict(self.stats),
                    "files": {n: len(f["records"]) for n, f in self.files.items()}}
