"""Raft consensus: leader election, log replication, durable term/vote/log, snapshots.

The state machine is driven through apply_fn(cmd, index) once an entry is committed.
Every `snapshot_every` applied entries the state machine is saved (snapshot_fn) and the log prefix
is discarded; followers that fall behind the snapshot receive it through InstallSnapshot.
A leader that has not heard from a majority for one election timeout steps down (check-quorum),
so a leader cut off by a partition stops accepting requests it can never commit. Elections start
with a pre-vote round, so an isolated node cannot inflate its term and disrupt the cluster later.
Not implemented (documented limitation): membership changes.
"""
import json
import os
import random
import threading
import time

from . import config, rpc


class NotLeader(Exception):
    def __init__(self, leader=None):
        super().__init__("not leader")
        self.leader = leader


class ProposeFailed(Exception):
    pass


def _line(index, entry):
    return (json.dumps({"i": index, "t": entry["t"], "c": entry["c"]},
                       separators=(",", ":")) + "\n").encode()


class Raft:
    def __init__(self, node_id, peers, data_dir, apply_fn, snapshot_fn=None, restore_fn=None,
                 emit=None, snapshot_every=None):
        self.id = node_id
        self.peers = dict(peers)            # peer id -> address (excluding self)
        self.apply_fn = apply_fn
        self.snapshot_fn = snapshot_fn
        self.restore_fn = restore_fn
        self.snapshot_every = snapshot_every if snapshot_every is not None else config.SNAPSHOT_EVERY
        self.emit = emit or (lambda *a, **k: None)
        self.dir = data_dir
        os.makedirs(self.dir, exist_ok=True)

        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        self.apply_lock = threading.Lock()  # held while the state machine changes (apply / install)
        self.term = 0
        self.voted_for = None
        self.snap_index = 0                 # last log index covered by the snapshot
        self.snap_term = 0
        self._snap = None                   # {"index", "term", "state"} of the latest snapshot
        self.log = [{"t": 0, "c": None}]    # log[0] stands for snap_index; real entries follow
        self.commit = 0
        self.applied = 0
        self.role = "follower"
        self.leader = None
        self.next_index = {}
        self.match_index = {}
        self.last_ack = {}
        self.leader_since = 0.0
        self.last_leader_contact = 0.0      # last message from a real leader (NOT our own timer)
        self.noop_index = 0
        self.waiting = set()
        self.results = {}
        self.inflight = set()
        self._last_bcast = 0.0
        self._reset_timer()

        self._state_path = os.path.join(self.dir, "raft_state.json")
        self._log_path = os.path.join(self.dir, "raft_log.jsonl")
        self._snap_path = os.path.join(self.dir, "snapshot.json")
        self._load()
        self._logf = open(self._log_path, "ab")

        threading.Thread(target=self._ticker, daemon=True).start()
        threading.Thread(target=self._applier, daemon=True).start()

    # ---------------------------------------------------------------- log indexing
    def _last(self):
        return self.snap_index + len(self.log) - 1

    def _term(self, i):
        return self.log[i - self.snap_index]["t"]

    def _entry(self, i):
        return self.log[i - self.snap_index]

    # ---------------------------------------------------------------- persistence
    def _load(self):
        if os.path.exists(self._state_path):
            with open(self._state_path) as f:
                s = json.load(f)
            self.term, self.voted_for = s["term"], s["voted_for"]
        if os.path.exists(self._snap_path):
            with open(self._snap_path) as f:
                snap = json.load(f)
            self._snap = snap
            self.snap_index, self.snap_term = snap["index"], snap["term"]
            self.log = [{"t": self.snap_term, "c": None}]
            if self.restore_fn:
                self.restore_fn(snap["state"])
            self.commit = self.applied = self.snap_index
        if os.path.exists(self._log_path):
            good = 0
            with open(self._log_path, "rb") as f:
                for line in f:
                    try:
                        e = json.loads(line)
                    except ValueError:          # torn final write from a crash
                        break
                    if e["i"] > self.snap_index:
                        if e["i"] != self._last() + 1:
                            break               # gap: stop at the last contiguous entry
                        self.log.append({"t": e["t"], "c": e["c"]})
                    good += len(line)
            with open(self._log_path, "r+b") as f:
                f.truncate(good)

    def _persist_state(self):
        tmp = self._state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"term": self.term, "voted_for": self.voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._state_path)

    def _append_file(self, entries):
        """Persist the last len(entries) entries of the in-memory log."""
        first = self._last() - len(entries) + 1
        for k, e in enumerate(entries):
            self._logf.write(_line(first + k, e))
        self._logf.flush()
        os.fsync(self._logf.fileno())

    def _rewrite_log(self):
        self._logf.close()
        tmp = self._log_path + ".tmp"
        with open(tmp, "wb") as f:
            for i in range(self.snap_index + 1, self._last() + 1):
                f.write(_line(i, self._entry(i)))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._log_path)
        self._logf = open(self._log_path, "ab")

    def _write_snapshot(self, index, term, state):
        snap = {"index": index, "term": term, "state": state}
        tmp = self._snap_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snap, f, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snap_path)
        self._snap = snap

    # ---------------------------------------------------------------- helpers
    def _reset_timer(self):
        self.last_contact = time.monotonic()
        self.timeout = random.uniform(config.ELECTION_MIN, config.ELECTION_MAX)

    def _majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _become_follower(self, term):
        if term > self.term:
            self.term = term
            self.voted_for = None
            self._persist_state()
        if self.role == "leader":
            self.emit("stepped_down", term=self.term)
        if self.role != "follower":
            self.leader = None
        self.role = "follower"
        self.cv.notify_all()

    def ready(self):
        """Leader that has applied its own term's first entry, so its state machine is current."""
        with self.lock:
            return self.role == "leader" and self.applied >= self.noop_index

    def status(self):
        with self.lock:
            return {"id": self.id, "role": self.role, "term": self.term, "leader": self.leader,
                    "commit": self.commit, "applied": self.applied, "last": self._last(),
                    "snap_index": self.snap_index, "log_entries": len(self.log) - 1,
                    "ready": self.role == "leader" and self.applied >= self.noop_index}

    # ---------------------------------------------------------------- timers
    def _ticker(self):
        while True:
            time.sleep(0.02)
            with self.lock:
                now = time.monotonic()
                if self.role == "leader":
                    if now - self.leader_since > config.ELECTION_MAX:
                        heard = 1 + sum(1 for p in self.peers
                                        if now - self.last_ack.get(p, 0) < config.ELECTION_MAX)
                        if heard < self._majority():          # check-quorum
                            self.emit("lost_quorum", term=self.term)
                            self._become_follower(self.term)
                            self._reset_timer()
                            continue
                    if now - self._last_bcast >= config.RAFT_HEARTBEAT:
                        self._broadcast_locked()
                elif now - self.last_contact >= self.timeout:
                    self._start_election_locked()

    # ---------------------------------------------------------------- election
    def _start_election_locked(self):
        """Pre-vote: ask whether peers WOULD vote for us before touching our term. A node that is
        partitioned away cannot win a pre-vote, so it never inflates its term and cannot force
        the healthy cluster into a needless election when the partition heals."""
        self._reset_timer()
        self._prevote_round = getattr(self, "_prevote_round", 0) + 1
        rnd, last = self._prevote_round, self._last()
        votes = {"n": 1, "done": False}
        if not self.peers:
            self._real_election_locked()
            return
        for addr in self.peers.values():
            threading.Thread(target=self._ask_prevote,
                             args=(addr, rnd, self.term + 1, last, self._term(last), votes),
                             daemon=True).start()

    def _ask_prevote(self, addr, rnd, term, last_index, last_term, votes):
        try:
            resp = rpc.call(addr, "RequestVote", {"term": term, "candidate": self.id, "pre": True,
                                                  "last_index": last_index,
                                                  "last_term": last_term}, timeout=0.5)
        except rpc.RpcError:
            return
        with self.lock:
            if (self.role == "leader" or self._prevote_round != rnd or votes["done"]
                    or not resp["granted"]):
                return
            votes["n"] += 1
            if votes["n"] >= self._majority():
                votes["done"] = True
                self._real_election_locked()

    def _real_election_locked(self):
        self.term += 1
        self.role = "candidate"
        self.voted_for = self.id
        self.leader = None
        self._persist_state()
        self._reset_timer()
        term, last = self.term, self._last()
        self.emit("election_started", term=term)
        votes = {"n": 1}
        for pid, addr in self.peers.items():
            threading.Thread(target=self._ask_vote,
                             args=(addr, term, last, self._term(last), votes),
                             daemon=True).start()
        if not self.peers:
            self._become_leader_locked()

    def _ask_vote(self, addr, term, last_index, last_term, votes):
        try:
            resp = rpc.call(addr, "RequestVote", {"term": term, "candidate": self.id,
                                                  "last_index": last_index,
                                                  "last_term": last_term}, timeout=0.5)
        except rpc.RpcError:
            return
        with self.lock:
            if resp["term"] > self.term:
                self._become_follower(resp["term"])
                return
            if self.role != "candidate" or self.term != term or not resp["granted"]:
                return
            votes["n"] += 1
            if votes["n"] >= self._majority():
                self._become_leader_locked()

    def _become_leader_locked(self):
        self.role = "leader"
        self.leader = self.id
        self.leader_since = time.monotonic()
        self.last_ack = {}
        last = self._last()
        self.next_index = {p: last + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        noop = {"t": self.term, "c": {"op": "NOOP"}}
        self.log.append(noop)
        self._append_file([noop])
        self.noop_index = self._last()
        self.emit("became_leader", term=self.term)
        self._advance_commit()
        self._broadcast_locked()

    def on_request_vote(self, req):
        with self.lock:
            if req.get("pre"):                          # pre-vote: answer, change nothing
                last = self._last()
                up_to_date = (req["last_term"], req["last_index"]) >= (self._term(last), last)
                # Measured from the last real leader message: our own election timer is reset by
                # our own pre-votes, and using it made two followers keep refusing each other.
                heard_leader = self.role == "leader" or (
                    time.monotonic() - self.last_leader_contact < config.ELECTION_MIN)
                return {"term": self.term,
                        "granted": req["term"] > self.term and up_to_date and not heard_leader}
            if req["term"] < self.term:
                return {"term": self.term, "granted": False}
            if req["term"] > self.term:
                self._become_follower(req["term"])
            last = self._last()
            up_to_date = (req["last_term"], req["last_index"]) >= (self._term(last), last)
            if up_to_date and self.voted_for in (None, req["candidate"]):
                self.voted_for = req["candidate"]
                self._persist_state()
                self._reset_timer()
                return {"term": self.term, "granted": True}
            return {"term": self.term, "granted": False}

    # ---------------------------------------------------------------- replication
    def _broadcast_locked(self):
        self._last_bcast = time.monotonic()
        for pid in self.peers:
            if pid not in self.inflight:
                self.inflight.add(pid)
                threading.Thread(target=self._replicate_to, args=(pid,), daemon=True).start()

    def _replicate_to(self, pid):
        try:
            while True:
                with self.lock:
                    if self.role != "leader":
                        return
                    term = self.term
                    ni = self.next_index[pid]
                    if ni <= self.snap_index:           # follower needs entries we discarded
                        method, timeout = "InstallSnapshot", 10.0
                        req = {"term": term, "leader": self.id, "index": self._snap["index"],
                               "snap_term": self._snap["term"], "state": self._snap["state"]}
                    else:
                        method, timeout = "AppendEntries", 0.6
                        prev = ni - 1
                        req = {"term": term, "leader": self.id, "prev": prev,
                               "prev_term": self._term(prev),
                               "entries": self.log[ni - self.snap_index:ni - self.snap_index + 64],
                               "commit": self.commit}
                try:
                    resp = rpc.call(self.peers[pid], method, req, timeout=timeout)
                except rpc.RpcError:
                    return
                with self.lock:
                    if resp["term"] > self.term:
                        self._become_follower(resp["term"])
                        return
                    if self.role != "leader" or self.term != term:
                        return
                    self.last_ack[pid] = time.monotonic()
                    if method == "InstallSnapshot" or resp["success"]:
                        self.match_index[pid] = max(self.match_index[pid], resp["match"])
                        self.next_index[pid] = self.match_index[pid] + 1
                        self._advance_commit()
                        if self.next_index[pid] > self._last():
                            return
                    else:
                        self.next_index[pid] = max(1, min(self.next_index[pid] - 1,
                                                          resp.get("hint", 1)))
        finally:
            with self.lock:
                self.inflight.discard(pid)

    def _advance_commit(self):
        for n in range(self._last(), self.commit, -1):
            if self._term(n) != self.term:
                break
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if count >= self._majority():
                self.commit = n
                self.cv.notify_all()
                break

    def on_append_entries(self, req):
        with self.lock:
            if req["term"] < self.term:
                return {"term": self.term, "success": False, "hint": 1}
            if req["term"] > self.term or self.role != "follower":
                self._become_follower(req["term"])
            self.leader = req["leader"]
            self._reset_timer()
            self.last_leader_contact = time.monotonic()
            prev, prev_term, entries = req["prev"], req["prev_term"], req["entries"]
            if prev < self.snap_index:                  # part of it is already in our snapshot
                entries = entries[self.snap_index - prev:]
                prev, prev_term = self.snap_index, self.snap_term
            if prev > self._last() or self._term(prev) != prev_term:
                return {"term": self.term, "success": False,
                        "hint": max(1, min(prev, self._last() + 1))}
            idx, new = prev, []
            for e in entries:
                idx += 1
                if idx <= self._last():
                    if self._term(idx) == e["t"]:
                        continue
                    del self.log[idx - self.snap_index:]   # conflicting suffix from a deposed leader
                    self._rewrite_log()
                self.log.append(e)
                new.append(e)
            if new:
                self._append_file(new)
            match = prev + len(entries)
            if req["commit"] > self.commit:
                self.commit = max(self.commit, min(req["commit"], match))
                self.cv.notify_all()
            return {"term": self.term, "success": True, "match": match}

    def on_install_snapshot(self, req):
        with self.apply_lock:
            with self.lock:
                if req["term"] < self.term:
                    return {"term": self.term, "match": 0}
                if req["term"] > self.term or self.role != "follower":
                    self._become_follower(req["term"])
                self.leader = req["leader"]
                self._reset_timer()
                self.last_leader_contact = time.monotonic()
                idx, term = req["index"], req["snap_term"]
                if idx <= self.applied:                 # we already have everything it covers
                    return {"term": self.term, "match": idx}
                keep = (self.log[idx - self.snap_index + 1:]
                        if idx <= self._last() and self._term(idx) == term else [])
                self._write_snapshot(idx, term, req["state"])
                self.log = [{"t": term, "c": None}] + keep
                self.snap_index, self.snap_term = idx, term
                self._rewrite_log()
                if self.restore_fn:
                    self.restore_fn(req["state"])
                self.commit = max(self.commit, idx)
                self.applied = idx
                self.cv.notify_all()
                self.emit("snapshot_installed", index=idx)
                return {"term": self.term, "match": idx}

    # ---------------------------------------------------------------- state machine
    def _applier(self):
        while True:
            with self.lock:
                while self.applied >= self.commit:
                    self.cv.wait(0.5)
            with self.apply_lock:
                with self.lock:
                    if self.applied >= self.commit:
                        continue
                    i = self.applied + 1
                    e = self._entry(i)
                res = self.apply_fn(e["c"], i) if e["c"] is not None else None
                with self.lock:
                    if i in self.waiting:
                        self.results[i] = (e["t"], res)
                    self.applied = i
                    self.cv.notify_all()
                    now_ready = self.role == "leader" and i == self.noop_index
                if now_ready:
                    self.emit("leader_ready", term=e["t"])
                self._maybe_snapshot(i)

    def _maybe_snapshot(self, i):
        """Called with apply_lock held, right after applying entry i."""
        if not self.snapshot_fn or not self.snapshot_every or i - self.snap_index < self.snapshot_every:
            return
        state = self.snapshot_fn()
        with self.lock:
            term = self._term(i)
            self._write_snapshot(i, term, state)
            self.log = [{"t": term, "c": None}] + self.log[i - self.snap_index + 1:]
            self.snap_index, self.snap_term = i, term
            self._rewrite_log()
        self.emit("snapshot_taken", index=i)

    def propose(self, cmd, timeout=4.0):
        """Replicate cmd and return the state machine's result once it is applied.

        If leadership is lost the outcome is unknown (the entry may still be committed by the
        next leader), which is exactly why appends carry request IDs.
        """
        with self.lock:
            if self.role != "leader":
                raise NotLeader(self.leader)
            entry = {"t": self.term, "c": cmd}
            self.log.append(entry)
            self._append_file([entry])
            idx, term = self._last(), self.term
            self.waiting.add(idx)
            self._advance_commit()
            self._broadcast_locked()
            deadline = time.monotonic() + timeout
            try:
                while True:
                    if self.applied >= idx:
                        got = self.results.pop(idx, None)
                        if got is None or got[0] != term:
                            raise ProposeFailed("entry overwritten by a new leader")
                        return got[1]
                    if self.role != "leader" or self.term != term:
                        raise NotLeader(self.leader)
                    left = deadline - time.monotonic()
                    if left <= 0:
                        raise ProposeFailed("timeout waiting for a majority")
                    self.cv.wait(min(left, 0.1))
            finally:
                self.waiting.discard(idx)
                self.results.pop(idx, None)
