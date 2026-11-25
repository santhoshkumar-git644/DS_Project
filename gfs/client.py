"""GFS client with exactly-once append.

Acknowledgement rule: append() returns (and writes ACK to the client log) only after the master
reports the record committed, which means all 3 replicas fsynced it AND a Raft majority of masters
committed its completion entry.

Request IDs are "client:seq", assigned and logged (SENT) before the first transmission. A retry
reuses the same ID, so the master can recognise it. With dedup=False the client instead takes a
fresh ID for every retry, which reproduces plain GFS's at-least-once behaviour (used as a baseline).
"""
import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

from . import config, rpc


class Retry(Exception):
    pass


class AppendFailed(Exception):
    pass


class Client:
    def __init__(self, name, log_path=None, dedup=True, deadline=90.0, parallel_writes=True):
        self.name, self.dedup, self.deadline = name, dedup, deadline
        self.masters = list(config.master_addrs().values())
        self.hint = None
        self.seq = 0
        self.acked_through = 0           # highest seq such that every seq <= it is acked
        self.retries = 0
        self.acked = 0
        self.last_error = None
        self.timings = []                # per successful append: (begin, write, commit) seconds
        self.pool = ThreadPoolExecutor(max_workers=config.REPLICATION) if parallel_writes else None
        if log_path and os.path.exists(log_path):
            self._resume(log_path)
        self.log = open(log_path, "a", buffering=1) if log_path else None

    def _resume(self, path):
        """A restarted client continues its sequence numbers from its durable log. Starting again
        at 1 would reuse old request IDs, and the master would answer 'already committed' for
        records it never stored."""
        acked = set()
        for line in open(path):
            try:
                e = json.loads(line)
            except ValueError:                       # torn last line from a crash
                continue
            name, _, seq = e.get("req", "").rpartition(":")
            if name != self.name or not seq.isdigit():
                continue
            self.seq = max(self.seq, int(seq))
            if e["ev"] == "ACK":
                acked.add(int(seq))
        while self.acked_through + 1 in acked:
            self.acked_through += 1

    # ------------------------------------------------------------------ durable client log
    def _log(self, **rec):
        if self.log:
            self.log.write(json.dumps(rec) + "\n")
            self.log.flush()
            os.fsync(self.log.fileno())

    # ------------------------------------------------------------------ master RPC with failover
    def _master(self, method, body, timeout=6.0):
        order = ([self.hint] if self.hint else []) + [m for m in self.masters if m != self.hint]
        last = "no master reachable"
        for a in order:
            try:
                r = rpc.call(a, method, body, timeout=timeout)
            except rpc.RpcError as e:
                last = str(e)
                continue
            err = r.get("error")
            if err == "not_leader":
                if r.get("leader"):
                    self.hint = r["leader"]
                last = err
                continue
            if err:
                raise Retry(err)
            self.hint = a
            return r
        raise Retry(last)

    # ------------------------------------------------------------------ append
    def _write(self, rid, addr, body):
        try:
            w = rpc.call(addr, "WriteAt", body, timeout=3.0)
        except rpc.RpcError as e:
            return str(e)
        return f"{rid}: {w['error']}" if "error" in w else None

    def _attempt(self, file, data, req_id, seq):
        t0 = time.perf_counter()
        b = self._master("AppendBegin", {"req_id": req_id, "client": self.name, "seq": seq,
                                         "file": file, "size": len(data),
                                         # baseline: every earlier ID is acked or abandoned
                                         "low_water": self.acked_through if self.dedup else seq - 1})
        if b["status"] == "committed":                   # an earlier attempt already committed
            return b
        t1 = time.perf_counter()
        body = {"chunk": b["chunk"], "req": req_id, "offset": b["offset"],
                "version": b["version"], "data": base64.b64encode(data).decode()}
        if self.pool:                                    # all replicas at once
            errors = list(self.pool.map(lambda r: self._write(r[0], r[1], body), b["replicas"]))
        else:
            errors = [self._write(rid, addr, body) for rid, addr in b["replicas"]]
        errors = [e for e in errors if e]
        if errors:
            raise Retry(errors[0])
        t2 = time.perf_counter()
        c = self._master("AppendCommit", {"req_id": req_id})
        if c.get("status") != "committed":
            raise Retry(c.get("status", "commit failed"))
        self.timings.append((t1 - t0, t2 - t1, time.perf_counter() - t2))
        return c

    def append(self, file, data, logical):
        """Append one record; blocks until it is acknowledged. Returns (chunk, offset)."""
        self.seq += 1
        seq = self.seq
        req_id = f"{self.name}:{seq}"
        self._log(ev="SENT", logical=logical, req=req_id, ts=time.time())
        stop = time.time() + self.deadline
        attempts = 0
        while True:
            try:
                r = self._attempt(file, data, req_id, seq)
                break
            except Retry as e:
                attempts += 1
                self.retries += 1
                self.last_error = str(e)
                if time.time() > stop:
                    self._log(ev="GAVE_UP", logical=logical, req=req_id)
                    raise AppendFailed(logical)
                if not self.dedup:                       # at-least-once baseline: forget the old ID
                    self.seq += 1
                    seq = self.seq
                    req_id = f"{self.name}:{seq}"
                    self._log(ev="SENT", logical=logical, req=req_id, ts=time.time())
                time.sleep(0.15)
        if self.dedup:
            self.acked_through = seq                     # sequential client: all earlier acked
        self.acked += 1
        self._log(ev="ACK", logical=logical, req=req_id, chunk=r["chunk"], offset=r["offset"],
                  attempts=attempts + 1, ts=time.time())
        return r["chunk"], r["offset"]

    # ------------------------------------------------------------------ read
    def read_all(self, file):
        """Return the file's records in commit order (only records whose DONE is committed)."""
        listing = self._master("ListRecords", {"file": file})
        out = []
        for rec in listing["records"]:
            replicas = listing["chunks"][str(rec["chunk"])]
            for rid, addr in replicas:
                try:
                    r = rpc.call(addr, "ReadAt", {"chunk": rec["chunk"], "offset": rec["offset"],
                                                  "size": rec["size"]}, timeout=2.0)
                    out.append(base64.b64decode(r["data"]))
                    break
                except rpc.RpcError:
                    continue
            else:
                raise AppendFailed(f"no replica readable for {rec}")
        return out
