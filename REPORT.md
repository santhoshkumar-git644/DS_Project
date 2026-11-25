# Exactly-Once GFS: Implementation Report

**Team 48:** Santhosh and Harsha Vardhan, Distributed Systems.
**Project:** GFS-style distributed file system with reliable record append and Raft-replicated master
metadata.
**Status:** every goal of the proposal is implemented and verified (15/15 features pass in
`results/VERIFY.md`, last full run 2026-11-17). This report describes the code as it exists.
**Stack:** Python 3.13, gRPC (`grpcio` 1.84), no other dependency. Developed and tested on Windows 11.

---

## 1. What was built

A small distributed file system in which a client can **append records to a shared file and be sure
each acknowledged record is stored exactly once**, even if the master crashes in the middle of the
append or the client has to retry. It is made of:

| Piece | Count | Role |
| --- | --- | --- |
| Master | 3 | Replicate all metadata with Raft; the leader serves clients |
| Chunk server | 5 | Store 3-way replicated chunk files; heartbeat to the masters |
| Client library | any | Assigns request IDs, retries, keeps a durable log |
| Dashboard | 1 | Live view of the cluster plus a control panel for failure injection |
| Checker | 1 | Independent verifier of the stored bytes against client logs |

Every node is a separate **operating-system process** with its own port, data directory and fsyncs.
"Crashing" a node is a real `kill`, so nothing in memory survives, which is what a power cut does to a
machine. No credentials, cloud accounts or extra machines are needed.

### 1.1 Status against the proposal

| Proposal goal | Status | Where |
| --- | --- | --- |
| 1. Exactly-once record append | Done, verified | `statemachine.py`, `master.py`, `client.py` |
| 2. Precise acknowledgement rule | Done, verified | `master.append_commit`, `client._attempt` |
| 3. Durable, replicated metadata (Raft log, snapshots, compaction, dedup trimming) | Done, verified | `raft.py`, `statemachine.py` |
| 4. Fast leader failover (target about 2 s) | Done: p50 0.78 s, p95 0.94 s, max 0.97 s over 20 kills | `raft.py` |
| 5. Recovery of unfinished appends | Done, verified (both commit and abort paths) | `master._reconcile` |
| 6. Replica health and repair | Done, verified for one failed server at a time | `master._repair`, `chunkserver.py` |
| 7. Live fault-tolerance dashboard | Done, with a control panel | `dashboard.py`, `dashboard.html`, `demo.py` |
| 8. Crash at 5 points and compare with the client log | Done, verified | `failpoints.py`, `checker.py`, `tests/crash_tests.py` |

**Added beyond the proposal:** pre-vote and check-quorum in Raft, network-partition injection, client
restart recovery from its log, a baseline client that reproduces plain-GFS duplicates, a latency
benchmark, a one-command verification suite, an autoplay demo and a stale-process cleanup tool.

### 1.2 What differs from the proposal

| Proposal said | What was built | Why |
| --- | --- | --- |
| Go | Python | Go was not installed; the architecture is identical |
| gRPC with Protocol Buffers | gRPC with a JSON codec | no code-generation step; still real gRPC over HTTP/2 |
| Grafana + Prometheus | custom web dashboard | one process, no extra install, and it has controls |
| Docker Compose | local processes | the same code would run in containers unchanged |
| `tc` / `iptables` partitions | partitions injected inside the RPC layer | those tools do not exist on Windows |
| Raft "own implementation vs library" | own implementation | election, replication, snapshots all written here |
| Research cluster (RCE) | not needed | the whole campaign runs on one laptop |
| Primary chunk server orders appends (GFS) | the Raft-replicated master assigns offsets | makes a retry after any crash a table lookup |

---

## 2. Architecture

```
                         Dashboard + controls  http://127.0.0.1:8765
                               (HTTP, polls masters every 0.4 s)
                                          |
        +---------------------------------+---------------------------------+
        |                                 |                                 |
   +----v----+   AppendEntries /     +----v----+                       +----v----+
   | master  |   RequestVote /       | master  |                       | master  |
   |   m1    |<--- InstallSnapshot-->|   m2    |<--------------------->|   m3    |
   | :7001   |       (Raft)          | :7002   |                       | :7003   |
   +----^----+                       +----^----+                       +----^----+
        |  AppendBegin / AppendCommit / ListRecords (client -> leader)       |
        |  Heartbeat (chunk servers -> all 3 masters, every 0.4 s)           |
        |  Has / CopyChunk (leader -> chunk servers)                         |
   +----+------+         +-----------+----------+                           |
   |  client   |-------->| cs1 .. cs5 :7101..05 |<--------------------------+
   | (library) | WriteAt | chunk files + index  |
   +-----------+         +----------------------+
```

### 2.1 Where the data lives

| Node | On-disk files (under `data/<node>/`) | Meaning |
| --- | --- | --- |
| master | `raft_state.json` | current term and who this node voted for (atomic replace + fsync) |
| master | `raft_log.jsonl` | one JSON line per Raft entry `{"i": index, "t": term, "c": command}` |
| master | `snapshot.json` | latest state-machine snapshot `{"index", "term", "state"}` |
| chunk server | `<chunk>.dat` | the record bytes, at the offsets the master assigned |
| chunk server | `<chunk>.idx` | one line per durable record: request ID, offset, size, crc32 |
| chunk server | `<chunk>.ver` | chunk version (increases when the replica set changes) |
| client | `<name>.jsonl` | `SENT` and `ACK` lines, fsynced; the client's durable memory |

### 2.2 How one append flows

```
client                    leader master (Raft: 3 masters)         chunk servers (3 replicas)
  | 1. log SENT(client:seq)  |
  |    (fsync)               |
  |-- AppendBegin ---------->| propose INTENT ---> majority fsyncs it, then it is applied
  |                          |   reserves chunk + offset; remembers the request ID
  |<-- chunk, offset, ids ---|
  |-- WriteAt (3 in parallel) ---------------------------------->| write data, fsync,
  |                          |                                    | append index line, fsync
  |-- AppendCommit --------->|-- Has(req) x3 ------------------->| "do you hold it?"
  |                          | all 3 yes -> propose DONE ---> majority fsyncs it, applied
  |<-- committed ------------|   (this is the acknowledgement point)
  | 2. log ACK (fsync)       |
```

Per append: 2 client-to-master RPCs, 3 client-to-chunk-server RPCs, 3 master-to-chunk-server `Has`
RPCs, 2 Raft commits (4 `AppendEntries` messages), and about 14 fsyncs (client 2, masters 6, chunk
servers 6).

---

## 3. Networking

### 3.1 Transport

| Aspect | Choice |
| --- | --- |
| Protocol | gRPC over HTTP/2 over TCP, plaintext (no TLS or authentication; loopback only) |
| Addresses | `127.0.0.1`; masters 7001-7003, chunk servers 7101-7105, dashboard 8765 |
| Call style | unary request/response only; no streaming |
| Encoding | one generic service `/gfs.Node/<Method>`; request and response are JSON objects. A custom serializer replaces Protocol Buffers, so no `protoc` step exists |
| Binary data | record bytes travel base64-encoded inside the JSON (about 33% larger) |
| Server threads | each process runs a 48-thread gRPC worker pool |
| Channels | one cached channel and one cached callable per `(address, method)`; reconnect backoff set to 100-400 ms so a restarted node is reachable again within a fraction of a second |
| Failure semantics | `rpc.call` raises `RpcError` for a refused connection, a timeout, a dropped (partitioned) call, or an exception inside the handler. Handler exceptions are caught and returned as `{"rpc_error": ...}` so one bad request never kills the connection |
| Dashboard | plain HTTP: `GET /` (page), `GET /state` (JSON), `POST /action/<name>` (controls) |

### 3.2 Timeouts used (a missed timeout is how a failure is detected)

| Call | Timeout |
| --- | --- |
| Raft `RequestVote` (and pre-vote) | 0.5 s |
| Raft `AppendEntries` | 0.6 s |
| Raft `InstallSnapshot` | 10 s |
| Chunk-server `Heartbeat` | 0.4 s |
| Leader `Has` check | 0.6 s |
| Client `WriteAt` | 3 s |
| Client calls to a master | 6 s |
| Master `CopyChunk` / chunk-to-chunk `ReadChunk` (repair) | 30 s / 20 s |
| Raft `propose` waiting for a majority | 4 s |

### 3.3 RPC catalogue

**Master (every master serves all of these; only the leader answers client calls):**

| Method | Caller | Purpose |
| --- | --- | --- |
| `RequestVote` | master | election vote; with `pre: true` it is a pre-vote that changes no state |
| `AppendEntries` | leader | replicate log entries; also the heartbeat; carries the commit index |
| `InstallSnapshot` | leader | send the whole state to a follower that fell behind the compacted log |
| `AppendBegin` | client | reserve a chunk and offset for a request ID |
| `AppendCommit` | client | verify replicas and commit the append |
| `ListRecords` | client, checker | list committed records (after a read barrier) |
| `Heartbeat` | chunk server | liveness and chunk inventory; the reply can order deletions |
| `Status` | dashboard, tests | role, term, commit index, chunk table, counters, events |
| `SetFailpoint` | tests, demo | arm a crash point |
| `SetPartition` | tests, demo | block this master's calls to given addresses |

**Chunk server:**

| Method | Caller | Purpose |
| --- | --- | --- |
| `WriteAt` | client | durably store one record at an offset (idempotent per request ID) |
| `Has` | leader | "do you durably hold this request at this offset?" |
| `ReadAt` | client | read one record |
| `ReadChunk` | checker, repair | return the whole chunk file plus its index |
| `CopyChunk` | leader | pull a chunk from another server and store it (repair) |
| `ListIndex` | tests | list the index entries |
| `Status`, `SetFailpoint` | tests, demo | inspection and crash injection |

### 3.4 Client-side networking behaviour

- **Leader discovery:** the client remembers the last master that answered (`hint`). A master that is
  not the leader replies `not_leader` with the leader's address; the client follows it, otherwise tries
  the next master. A leader that has just been elected replies `not_ready` until it has applied its
  first entry, and the client retries.
- **Retry:** any failure (dropped connection, timeout, `propose_failed`, aborted or incomplete append)
  becomes a `Retry`. The client sleeps 150 ms and tries again with the **same request ID**, until a
  deadline.
- **Parallel writes:** the three replica writes go out at the same time from a 3-thread pool.

### 3.5 Network failures that are simulated

| Failure | How it is injected |
| --- | --- |
| Process crash / power loss | `Popen.kill()` (hard kill) or `os._exit(99)` at a named point in the code |
| Network partition | `rpc.BLOCKED`: a set of addresses a process refuses to call. `SetPartition` sets it on all three masters so no master-to-master RPC crosses the cut in either direction. Clients and chunk servers are left connected, which is the dangerous case: a stale leader that can still talk to clients |

### 3.6 Background traffic

Raft heartbeats: one `AppendEntries` every 80 ms from the leader to each follower. Chunk-server
heartbeats: each of 5 servers calls each of 3 masters every 0.4 s, which is about 37 calls per second
in total. The dashboard polls the three masters every 0.4 s.

---

## 4. Algorithms

### 4.1 Raft consensus (`raft.py`)

The masters agree on one ordered log of commands. Every master applies the same commands in the same
order to a deterministic state machine (section 4.2), so they all hold identical metadata.

**Roles and timing.** Every node starts as a follower. A follower that hears nothing from a leader for
a random 0.5-1.0 s (randomisation prevents split votes) starts an election. The leader sends
heartbeats every 80 ms.

**Election with pre-vote.**
1. The follower first runs a **pre-vote**: it asks the others "would you vote for me?" without changing
   any term. A node grants it only if the candidate's next term is higher, the candidate's log is at
   least as up to date as its own (compared by last term, then last index), and it has not heard from a
   leader in the last 0.5 s.
2. Only after a majority of pre-votes does the follower increment its term, vote for itself, persist
   that (fsync) and send real `RequestVote` calls. A majority of 2 out of 3 makes it leader.
3. A new leader immediately appends and replicates a **no-op** entry of its own term. Once that entry is
   applied the leader's state machine is known to be current (`ready()`), and only then does it serve
   clients.

Why pre-vote: a node cut off from the others would otherwise keep raising its term and, on rejoining,
force a needless election. Measured: during the partition test the term rose by 4 without pre-vote and
by 1 with it.

**Log replication.** The leader keeps `next_index` and `match_index` per follower and sends batches of
up to 64 entries with the previous entry's index and term. The follower rejects a batch whose previous
entry does not match and returns a hint, so the leader backs up quickly. On a conflict the follower
deletes its conflicting suffix and rewrites its log file. An entry is **committed** when stored on a
majority and belongs to the leader's current term (older-term entries are committed only indirectly,
which is what makes Raft safe).

**Durability.** Term and vote are written with write-to-temporary-file, fsync, atomic rename. Log
entries are appended to `raft_log.jsonl` and fsynced before they count. On restart a torn last line from
a crash is detected and truncated.

**Check-quorum.** A leader that has not heard acknowledgements from a majority for over 1 s steps down.
So a leader cut off by a partition stops accepting requests it can never commit, and clients move on to
the new leader.

**Snapshots and compaction.** After every 500 applied entries the master saves its whole state to
`snapshot.json` (temporary file, fsync, atomic rename) and then discards the log prefix. Crash safety
comes from the order: a crash between the two steps leaves a snapshot plus an old log, and recovery
skips entries at or below the snapshot index. A follower whose `next_index` has fallen below the
snapshot receives the snapshot through `InstallSnapshot` and then continues with normal entries.
Measured: after 317 commits the leader kept 37 log entries, and a follower that missed all of it caught
up in 1.3 s.

**Proposing a command.** `propose(cmd)` appends to the leader's log, waits until the entry is applied,
and returns the state machine's result. If leadership is lost while waiting the outcome is **unknown**:
the entry may still be committed by the next leader. That ambiguity is exactly the problem request IDs
solve.

**Read barrier.** `ListRecords` first proposes a no-op, so a leader that has been deposed without
knowing cannot return stale data.

### 4.2 The replicated state machine (`statemachine.py`)

The state is: files with their chunk lists and committed records, a chunk table (replicas, version,
next free offset, sealed flag), and the **dedup table** keyed by request ID. It changes only through
eight commands applied in log order. Offsets are assigned inside `apply`, so all three masters assign
the same offset.

| Command | Effect |
| --- | --- |
| `NOOP` | nothing (used for leader readiness and read barriers) |
| `CREATE` | create a file if missing |
| `ADD_CHUNK` | allocate a chunk with 3 replicas; ignored if another caller already added one (`after` check) |
| `INTENT` | the heart of deduplication, see 4.3 |
| `DONE` | mark an intent committed and add the record to the file (acknowledgement point) |
| `ABORT` | mark an unfinished intent aborted |
| `SEAL` | stop writing to a chunk and abort its unfinished intents |
| `SET_REPLICAS` | change a chunk's replica set and bump its version; refuses if the expected version is stale |

### 4.3 Exactly-once record append

**Idea.** Delivery is at-least-once because the client retries until it is acknowledged. The *effect*
is exactly-once because every retry carries the **same request ID** and the replicated dedup table turns
a repeat into a lookup.

**Request lifecycle in the dedup table:**

```
 (no entry) --INTENT--> intent --DONE--> committed        (visible to readers)
                          |
                          +--ABORT / SEAL / SET_REPLICAS--> aborted --INTENT (retry)--> intent
                                                                      (new offset, attempt + 1)
```

**`INTENT` rules** (applied identically on every master):
1. If the request ID is known and is `committed` or `intent`, return the stored result (same chunk,
   same offset) and count a dedup hit. Nothing new is reserved.
2. If it is known and `aborted`, reserve a fresh offset and increment `attempt`.
3. If the chunk is already sealed, report `sealed`. If the record does not fit, seal the chunk and
   report `full`. Either way the leader allocates a new chunk and proposes again.
4. Otherwise reserve `offset = chunk.next`, advance `chunk.next` by the record size, and store the
   entry.

**Acknowledgement rule.** The client is acknowledged only when:
(a) all three replicas have fsynced the record data and then its index line, and
(b) the `DONE` entry is committed by a majority (2 of 3 masters).
Before logging `DONE` the leader asks every replica `Has(request, offset, size)`. A record is visible
to readers **if and only if** its `DONE` entry is committed. Bytes written by an aborted attempt may sit
on a replica but are never referenced.

**Idempotent chunk writes.** `WriteAt` returns success immediately if the same request ID is already
indexed at the same offset, so rewriting after a retry is harmless. A write is "present" only after the
data fsync and the index fsync both complete; a crash between them (`cs.mid_write`) leaves a replica that
honestly answers `Has = false`.

**Dedup table trimming.** Every `AppendBegin` carries the client's *low-water mark*: the highest
sequence number such that it and all lower ones are finished. Entries at or below it that are
`committed` or `aborted` are deleted when the intent is applied; an entry still in `intent` is kept for
reconcile. Measured: the table never exceeded 3 entries after 180 appends.

**Why a crash cannot create a duplicate (case analysis).**

| The leader or a chunk server dies... | What happens |
| --- | --- |
| before logging the intent | nothing reserved; the retry reserves normally |
| after the intent is committed | the retry finds the intent and reuses the same offset |
| after replicas hold the record, before `DONE` | the retry, or the new leader's reconcile, commits it once |
| after `DONE`, before the client hears | the retry returns the original offset; no second write |
| a chunk server mid-write | that replica lacks the record, so the append aborts and is redone at a new offset |

### 4.4 Recovery of unfinished appends (`master._reconcile`)

Runs on the leader every 0.5 s. For each intent older than 1.5 s (so slow-but-alive appends are not
disturbed) it asks every replica `Has`:

| Answers | Action |
| --- | --- |
| all three have it | propose `DONE` (counted as a reconcile commit) |
| some replica says no, none unreachable | propose `ABORT` |
| a replica is unreachable | wait; abort only after 4 s |

If a client is merely slow and reconcile aborts its intent, the client's `AppendCommit` sees `aborted`
and retries at a new offset. This is safe, because commit and abort both go through the log and a
committed entry is never aborted.

### 4.5 Failure detection and replica repair

**Detector.** Each chunk server sends a heartbeat (its id and a chunk-to-version inventory) to **all
three** masters every 0.4 s, so a newly elected leader already knows who is alive. No heartbeat for
2.5 s means dead. The timeout is a tradeoff: shorter detects faster but can mistake a long pause for a
failure.

**Appends to a degraded chunk.** If an unsealed chunk has a dead replica, the leader seals it (aborting
its unfinished intents) and allocates a new chunk on 3 live servers, chosen as the three with the fewest
chunk replicas (ties by id).

**Repair (`_repair`, `_do_repair`).** For each chunk with a dead replica, a live source and a live spare:
1. seal the chunk if it is not sealed;
2. tell the spare to `CopyChunk` from the source: it pulls the whole data file and index, fsyncs both
   and records the new version;
3. propose `SET_REPLICAS` with the new replica list and `version + 1`, guarded by the expected old
   version so two repairs cannot clash.

The request-ID index is copied with the data. Without it, a retry that reached the repaired copy would
not be recognised.

**Stale replicas.** On each heartbeat the leader compares the server's chunk versions with its own. A
copy is told to **delete itself** only when the chunk has moved on to a newer version without this
server. A copy at the current version, or of an unknown chunk, is never deleted, so a leader with stale
metadata cannot destroy fresh data. A server in the replica set with an old version is told to update its
version number.

Measured: a dead server was noticed after 2.4 s, and all 7 chunks were back to 3 live replicas by 2.6 s.

### 4.6 Client algorithm (`client.py`)

1. Assign `client:seq`, write `SENT` to the log, fsync. This happens **before** transmission.
2. Loop: `AppendBegin`, parallel `WriteAt` to the 3 replicas, `AppendCommit`. On any failure, sleep and
   retry with the same ID. On success write `ACK` with chunk and offset, fsync, advance the low-water mark.
3. **Restart recovery:** on startup the client re-reads its own log and continues numbering from the
   highest sequence used. Without this, a restarted client would reuse old IDs and the master would
   answer "already committed" for records it never stored.
4. **Baseline mode** (`dedup=False`): the client takes a *new* ID on every retry, which reproduces plain
   GFS's at-least-once behaviour and is used to show the duplicates the mechanism prevents.

### 4.7 Verification algorithm (`checker.py`)

The checker does not trust the master's bookkeeping.
1. Read each client's log: which logical records were sent, which were acknowledged.
2. Ask the leader for the committed record list (after a read barrier).
3. For each chunk, fetch the **raw bytes and index from every reachable replica**, cut out each
   committed record, and compare the copies byte for byte.
4. Parse each record's logical ID from its own payload (the text before `|`) and count how many times
   each appears.

Violations: **duplicates** (a logical record visible more than once), **lost** (acknowledged but not
visible), **mismatch** (replicas differ, or a replica lacks the record). Informational: committed but not
yet acknowledged, and **orphans** (index entries for aborted attempts, never visible).

### 4.8 Fault injection (`failpoints.py`)

A failpoint is a named line of code where the process dies instantly with `os._exit(99)`, skipping all
cleanup like a power cut. It is armed by an environment variable or by the `SetFailpoint` RPC with a
count, so it fires once. The five points:

| Failpoint | Dies at |
| --- | --- |
| `master.before_intent` | leader received the append, nothing logged |
| `master.after_intent` | intent committed, reply not sent, no chunk written |
| `cs.mid_write` | chunk server wrote data but not yet fsynced it or indexed it |
| `master.before_done` | all replicas hold the record, `DONE` not logged |
| `master.after_done` | `DONE` committed, client not told |

---

## 5. File-by-file description

Line counts are non-blank lines.

### 5.1 The system (`gfs/`)

#### `gfs/config.py` (26 lines)
Topology and every tunable in one place: the ports above, replication factor 3, chunk size (256 KB
default, set smaller in tests so a trial spans many chunks), data directory, Raft timings (heartbeat
0.08 s, election 0.5-1.0 s, snapshot every 500 entries), chunk-server heartbeat 0.4 s and death timeout
2.5 s, reconcile grace 1.5 s and abort-after 4 s. Several values can be overridden by environment
variables, which is how the tests and the demo change them for child processes. Helper functions build
`host:port` addresses.

#### `gfs/rpc.py` (71 lines) - networking
The gRPC layer described in section 3. `serve(port, methods)` starts a gRPC server whose single generic
handler dispatches `/gfs.Node/<Method>` to a Python function, with JSON in and out. `call(addr, method,
body, timeout)` uses cached channels and raises `RpcError` on any failure. `BLOCKED` is the set of
addresses refused for partition simulation. **Algorithms:** none; it is plumbing.

#### `gfs/failpoints.py` (33 lines)
Section 4.8. `arm(name, count)`, `hit(name)`, `disarm_all()`; the list `POINTS` names the five crash
points. `hit` is called at the exact lines where a crash is wanted.

#### `gfs/raft.py` (467 lines) - consensus
The `Raft` class: persistence (`_load`, `_persist_state`, `_append_file`, `_rewrite_log`,
`_write_snapshot`), the ticker thread (heartbeats, election timeout, check-quorum), pre-vote and real
elections (`_start_election_locked`, `_ask_prevote`, `_real_election_locked`, `_ask_vote`,
`_become_leader_locked`), vote handling (`on_request_vote`), replication (`_replicate_to`,
`_advance_commit`, `on_append_entries`, `on_install_snapshot`), the applier thread and snapshotting
(`_applier`, `_maybe_snapshot`), and `propose`. It knows nothing about files or chunks: it calls the
`apply_fn` it was given for each committed entry, plus `snapshot_fn` / `restore_fn` to save and load the
state. **Algorithms:** Raft leader election with pre-vote, log replication with consistency check and
fast backtracking, commit by majority of current-term entries, check-quorum, snapshots with
InstallSnapshot. **Networking:** `RequestVote`, `AppendEntries`, `InstallSnapshot`, one short-lived
thread per peer per broadcast.

#### `gfs/statemachine.py` (150 lines) - replicated metadata
`MasterState`: the eight commands (`op_noop` ... `op_set_replicas`), `_trim` for the dedup table,
`dump` / `load` for snapshots, and read-only helpers (`server_load`, `snapshot`). It is deterministic and
has no clocks or randomness; the timestamp used by reconcile is supplied by the leader inside the command.
**Algorithms:** request-ID deduplication, offset reservation, chunk sealing, versioned replica
reconfiguration, low-water-mark trimming. **Networking:** none (pure in-memory logic, which is why it can
be unit-tested without a cluster).

#### `gfs/master.py` (302 lines) - the master process
The `Master` class wires Raft, the state machine and an RPC server together. `_client` wraps every
client-facing handler: it redirects to the leader or answers `not_ready`. The append path is
`_choose_chunk` (pick or allocate a chunk), `append_begin` and `append_commit`; `list_records` adds the
read barrier. `heartbeat` records liveness and returns delete / version orders. `_monitor` runs the
leader's background loop every 0.5 s: `_liveness_events`, `_reconcile` (4.4) and `_repair` /
`_do_repair` (4.5). It also keeps a ring of events for the dashboard and answers `Status`.
**Algorithms:** exactly-once append protocol, reconcile, failure detector, replica placement and repair.
**Networking:** serves all master RPCs; calls `Has` and `CopyChunk` on chunk servers.

#### `gfs/chunkserver.py` (189 lines) - the storage node
The `ChunkServer` class: `write_at` (the durable, idempotent write with the `cs.mid_write` failpoint),
`has`, `read_at`, `read_chunk`, `list_index`, `copy_chunk` (repair), `_delete` and the heartbeat thread.
On start `_recover` rebuilds the in-memory index from the `.idx` files and ignores a torn last line.
Open file handles are cached per chunk, because reopening a file on every write cost about 50 ms on
Windows and cut throughput by four times. **Algorithms:** write-ahead style two-step durability
(data, then index), per-chunk locking, version check against stale writers, whole-chunk copy for repair.
**Networking:** serves chunk RPCs; sends heartbeats to all masters and acts on their replies.

#### `gfs/client.py` (158 lines) - the client library
`Client.append` and `_attempt` implement 4.6, `_master` does leader discovery and redirects, `_resume`
rebuilds state from the log, `_log` writes the fsynced log, `read_all` reads committed records.
`timings` records the per-phase latency used by the benchmark. **Networking:** calls masters (with
failover) and chunk servers (3 parallel writes).

#### `gfs/checker.py` (90 lines) - verification
Section 4.7. `check(file, log_paths)` returns a dictionary with `ok`, counts and the lists of
duplicates, lost records and mismatches. It can also be run as a command. **Networking:** `ListRecords`
and `ReadChunk` calls.

#### `gfs/cluster.py` (148 lines) - process control
The `Cluster` class starts each node as `python -m gfs.master --id m1` (and so on) with output sent to
`logs/<node>.log`, kills nodes hard, restarts them, partitions and heals, and queries status. It records
process IDs in `run/pids.json` so that `cleanup_stale()` can kill nodes left behind by a crashed demo and
wait for the ports to be released; `busy_ports()` tests whether the cluster ports are free. Helpers
`leader()`, `wait_ready()` and `wait_leader()` poll `Status`. **Networking:** `Status` and
`SetPartition` calls.

#### `gfs/dashboard.py` (116 lines) - dashboard server
`Poller` polls the three masters every 0.4 s and merges their event streams (it notices a master
restart because the event sequence number goes back). `Dashboard` is an HTTP server that serves the page,
`/state`, and, if a controller is attached, `POST /action/<name>`. Read-only when started alone
(`python -m gfs.dashboard`). **Networking:** gRPC `Status` calls out; HTTP in.

#### `gfs/dashboard.html` (269 lines) - dashboard page
One self-contained page (HTML, CSS, JavaScript, light and dark themes). Every 500 ms it fetches `/state`
and redraws: the narration banner, control buttons, client workloads, checker result table, three master
cards (role, term, log size, snapshot index, partition flag), five chunk-server cards, counters
(records committed, retries recognised, intents aborted, unfinished appends, chunks not fully
replicated, repairs, dedup table size), the chunk replica table (replicas coloured by liveness,
version, fill bar, healthy / degraded / repairing) and the event log. Every button is an HTTP POST to
the controller.

#### `gfs/__init__.py` (1 line)
Marks the folder as a Python package.

### 5.2 The demo

#### `demo.py` (339 lines)
`python demo.py` starts the whole cluster plus the dashboard with controls. `Workload` runs three
clients appending forever in one retry mode (exactly-once or at-least-once). `Controller` implements the
buttons: start or stop workloads, crash the leader at a chosen point, crash a chunk server mid-write,
kill or restart a node, kill a replica to trigger repair, partition and heal, auto-restart, **Verify**
(runs the checker on both workloads), and **autoplay**, a scripted scenario of about two minutes. A
supervisor thread notices processes that exit (exit code 99 means an injected crash), times the failover,
writes plain-English narration for the banner, and restarts crashed nodes after 3 s. Stopping a node on
purpose is never reported as a crash. At start-up it checks that the cluster ports are free and cleans
up leftovers. `--cleanup`, `--autoplay` and `--no-browser` are the options.

### 5.3 Tests (`tests/`)

#### `tests/test_statemachine.py` (105 lines)
Thirteen unit tests of the state machine with no processes or network: offsets in order, a retry of an
in-flight intent returns the same offset without reserving again, a retry after commit returns the
original result, `DONE` is idempotent, abort then retry gets a new offset, abort never undoes a commit,
a full chunk is sealed, sealing aborts pending intents, `SET_REPLICAS` checks and bumps the version,
`ADD_CHUNK` is idempotent under races, trimming keeps unacknowledged and unsettled entries, and
snapshot dump and load round-trips. Run with `python -m unittest tests.test_statemachine -v`.

#### `tests/smoke.py` (37 lines)
A 10-second sanity check: start the cluster, append 20 records, kill the leader, wait for a new one,
append 20 more, read everything back and run the checker.

#### `tests/crash_tests.py` (440 lines)
The scenario runner, one function per mode:

| Mode | What it does | What it checks |
| --- | --- | --- |
| `campaign` | for each of the 5 crash points: fresh cluster, 3 clients x 60 appends, arm the failpoint on the leader (or on a replica of the newest chunk for `cs.mid_write`), restart the dead node after a random 0.3-4 s, run the checker | crash fired; 0 duplicates, 0 lost, 0 mismatch; all 180 acknowledged; failover time measured from the new leader's `leader_ready` event |
| `baseline` | same, but the client takes a new ID on each retry | shows duplicates appear (so the checker really can detect them) |
| `repair` | kill a chunk server under load | degraded detected, 3 live replicas restored, versions bumped, returning server keeps no stale chunk, no data loss |
| `restart` | kill all 3 masters, restart; then restart the client | all records survive, a replayed ID is still "committed", the client continues at the right sequence number |
| `snapshot` | a follower is down while the leader compacts its log, then catches up; full restart | snapshot installed, log compacted, nothing lost |
| `partition` | cut the leader off from the other masters for 4 s, then heal | new leader elected, old leader steps down, term rises by 1, masters agree, no duplicate or lost record |
| `failover` | kill the leader 20 times in a row under load | p50, p95 and max time to a serving leader |

#### `tests/bench.py` (71 lines)
Measures where an append's time goes (begin, write, commit) and the effect of parallel replica writes,
for one client sequential, one client parallel, and three concurrent clients.

#### `tests/verify_all.py` (212 lines)
Runs the unit tests and every scenario, then starts `demo.py` and drives it through its HTTP control API
(press the crash button, verify, then kill the demo and confirm `--cleanup` frees the ports). It scores
15 features from the evidence and writes `results/VERIFY.md`. About 7 minutes.

### 5.4 Documents and configuration

| File | Content |
| --- | --- |
| `README.md` (151 lines) | how to run, layout, protocol summary, results, design tradeoffs, differences from the proposal |
| `DEMO.md` (65 lines) | a step-by-step presentation script with what to click, what the audience sees and what to say, plus likely questions and answers |
| `REPORT.md` | this document |
| `report.tex` (175 lines) | the original **proposal** (2 pages, written before implementation). It still says Go, Protocol Buffers, Grafana and Docker; see 1.2 |
| `requirements.txt` | `grpcio>=1.60` |
| `.gitignore` | ignores `.env`, caches, run data, logs |
| `results/` | evidence from the last full run: `VERIFY.md`, per-scenario JSON files, per-trial campaign and baseline data, benchmark data, `demo_*.png` dashboard screenshots |

---

## 6. Verification and results

All figures are from `python tests/verify_all.py` on 2026-11-17 (one laptop, 3 masters and 5 chunk
servers as local processes). The full suite takes about 7 minutes and scores 15 features, all passing.
On 2026-11-25 the suite was run again on a fresh copy of the project (new virtual environment, only the
README steps) and also passed 15/15, with slightly different timings (failover p50 0.72 s, p95 0.92 s);
its checklist is `results/VERIFY_cleanroom_2026-11-25.md`.

| Experiment | Result |
| --- | --- |
| 13 unit tests of the state machine | all pass |
| 25 crash trials (5 points x 5), exactly-once client, 180 requests each | **25/25 pass: 0 duplicates, 0 lost, 0 replica mismatches**; the crash fired in all 25; 218 retries recognised by request ID |
| 10 baseline trials (new ID per retry) | 8 produce duplicates, 13 duplicate records in total; the two `cs.mid_write` trials stay clean because completion needs all 3 replicas |
| Unfinished appends settled by new leaders | 6 committed, 37 aborted |
| 20 back-to-back leader kills under load | p50 0.78 s, p95 0.94 s, max 0.97 s |
| Chunk-server failure | detected 2.4 s after the kill; all chunks at 3 live replicas by 2.6 s; no stale chunks left on the returning server |
| Restart all masters, then the client | 35/35 records, replayed ID still "committed", client resumed at sequence 31 |
| Snapshot and InstallSnapshot | 37 log entries kept after 317 commits; lagging follower caught up in 1.3 s |
| Leader partitioned from the other masters | new leader in 0.78 s; old leader stepped down after 1.1 s; term rose by 1; 0 duplicates, 0 lost |
| Live demo through its HTTP API | crash button changed the leader; exactly-once 0 duplicates, at-least-once 1; `--cleanup` freed all 8 ports |

**Performance** (`python tests/bench.py`):

| Configuration | Appends/s | p50 | p95 | begin / write / commit (p50) |
| --- | --- | --- | --- | --- |
| 1 client, sequential replica writes | 19.0 | 43.7 ms | 100.8 ms | 10.4 / 15.3 / 17.2 ms |
| 1 client, parallel replica writes | 19.7 | 43.9 ms | 97.9 ms | 11.9 / 7.5 / 21.4 ms |
| 3 concurrent clients | 46.0 | 61.4 ms | 106.0 ms | 16.6 / 11.3 / 28.9 ms |

About three quarters of an append is the two Raft commits. Parallel replica writes halve the write
phase but barely change the total.

---

## 7. Limitations and things not tested

These are real gaps, listed so they are not discovered by someone else first.

**Not tested**
- Overlapping failures: a second chunk-server failure during a repair, or two masters down at once. With
  two of three masters down there is no majority, so appends stop by design (consistency over
  availability).
- Long soak runs; only short trials of up to about 3 minutes were run.
- Linux and containers; everything was tested on Windows 11 only.

**Design limits**
- No Raft membership changes, no TLS or authentication, loopback only.
- Metadata is held in memory on every master. The list of committed records per file is never trimmed
  (only the dedup table is), so it grows without bound.
- Repair and `ReadChunk` move a whole chunk in a single JSON message. With the 32 MB message limit this
  works for the 256 KB default chunk but **would fail for real 64 MB GFS chunks**; it would need
  streaming.
- Each Raft entry is fsynced on its own while holding the Raft lock, so there is no group commit and
  throughput is bounded by fsync latency.
- Taking a snapshot copies the whole state under the state-machine lock, which pauses the master for a
  time proportional to its size.
- Aborted attempts leave orphan bytes on the replicas; there is no garbage collection of them.
- The client reads (`read_all`) do not verify the CRC they receive; only the checker does.
- Each client has one append in flight at a time; the low-water mark relies on that.
- An append whose client dies for good after the replicas hold the record may be committed by reconcile
  without ever being acknowledged. That record appears at most once, which is the intended meaning of
  "exactly-once for acknowledged requests".
- Python's global interpreter lock limits throughput; the figures above describe this implementation, not
  the algorithm.
