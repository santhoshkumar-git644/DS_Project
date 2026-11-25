# Exactly-Once GFS: record append with Raft-replicated metadata

Team 48 (Santhosh and Harsha Vardhan), Distributed Systems.

A GFS-style file system whose **record append is exactly-once for every acknowledged request**, even
when the master crashes mid-append. Master metadata is replicated across 3 masters with Raft
(snapshots, check-quorum, pre-vote). 5 chunk servers store 3-way replicated chunks and re-replicate
lost copies. A live dashboard lets you crash, partition and kill nodes, then verify the stored bytes.

Everything runs as local processes: 3 masters on ports 7001-7003, 5 chunk servers on 7101-7105, and
the dashboard on 8765. No credentials, cloud accounts or extra machines are needed.

## Run it

```
pip install -r requirements.txt        # grpcio only; Python 3.10+
python demo.py                         # cluster + dashboard with controls: http://127.0.0.1:8765
python demo.py --autoplay              # same, plays the whole failure scenario by itself (~2 min)
python tests/verify_all.py             # verify every feature -> results/VERIFY.md (~20 min)
```

Presenting? See **[DEMO.md](DEMO.md)** for the click-by-click script, talking points and Q&A. The
slides are in `slides/ExactlyOnceGFS.pptx` (15 plain slides with speaker notes; a PDF copy is next to it).
The full written description of every file, algorithm and network call is **[REPORT.md](REPORT.md)**.

If a run fails with "ports ... are already in use", another demo or test is still running: stop it
(Ctrl+C in its terminal) or run `python demo.py --cleanup`, then try again.

Other entry points:

| Command | What it does |
| --- | --- |
| `python tests/smoke.py` | 10-second sanity check (appends, leader kill, read back) |
| `python -m unittest tests.test_statemachine -v` | 13 unit tests of the replicated state machine |
| `python tests/crash_tests.py campaign --trials 5` | 5 crash points x N trials, exactly-once client |
| `python tests/crash_tests.py baseline --trials 2` | same crashes, client takes a new ID per retry (plain GFS) |
| `python tests/crash_tests.py repair\|restart\|snapshot\|partition` | the scenario tests |
| `python tests/bench.py` | where an append's time goes; parallel vs sequential replica writes |
| `python demo.py --cleanup` | kill node processes left behind by a crashed run |

## Layout

| Path | Role |
| --- | --- |
| `gfs/raft.py` | Raft: elections with pre-vote, replication, check-quorum, durable log, snapshots + InstallSnapshot |
| `gfs/statemachine.py` | Replicated master state: namespace, chunk map, append dedup table, snapshot dump/load |
| `gfs/master.py` | Client RPCs, append protocol, unfinished-append reconciliation, replica health and repair |
| `gfs/chunkserver.py` | Chunk files + per-chunk request-ID index, heartbeats, replica copy, stale-copy deletion |
| `gfs/client.py` | Request IDs, durable client log (resumed after restart), retries, baseline mode |
| `gfs/failpoints.py` | The 5 named crash points |
| `gfs/checker.py` | Independent verifier: raw replica bytes vs. each client's acknowledged-request log |
| `gfs/rpc.py` | gRPC with a JSON codec; simulated partitions |
| `gfs/dashboard.py`, `dashboard.html` | Live dashboard (read-only, or with controls when run by `demo.py`) |
| `gfs/cluster.py` | Start / kill / restart / partition nodes; leftover-process cleanup |
| `demo.py` | Presentation demo: workloads, controls, narration, autoplay |
| `tests/` | Unit tests, crash campaign, scenarios, benchmark, `verify_all.py` |

## The append protocol

1. The client assigns request ID `client:seq` and writes `SENT` to its fsynced log **before**
   sending. After a restart it continues numbering from that log, so old IDs are never reused.
2. `AppendBegin`: the leader commits an **INTENT** entry through Raft. Applying it reserves a chunk
   and offset and records the request in the dedup table. A repeated ID returns the existing
   reservation, or the existing committed result, instead of reserving again.
3. The client writes the record to all 3 replicas in parallel. Each replica fsyncs the data, then
   fsyncs an index line `(request ID, offset, size, crc)`. Writes are idempotent per request ID.
4. `AppendCommit`: the leader asks every replica `Has(request ID)`. Only if all 3 confirm does it
   commit a **DONE** entry through Raft.
5. **Acknowledgement rule:** the client is told "committed" only after DONE is committed by a
   majority (2 of 3 masters), which implies all 3 replicas had fsynced the record. A record is visible
   to readers if and only if its DONE entry is committed.

**Crash recovery.** A new leader runs a reconcile loop. For every intent older than 1.5 s it asks the
replicas which of them hold the request:
- **All 3 have it:** commit DONE.
- **Some are missing it:** ABORT. The reserved region is never referenced, and a retry of the same ID
  reserves a new offset.
- **A replica is unreachable:** wait up to 4 s, then abort.

**Dedup table trimming.** Every request carries the client's low-water mark, meaning every sequence
number at or below it is finished. Committed and aborted entries at or below the mark are dropped.
Unsettled intents are kept until reconcile settles them.

## Replica health and repair

Chunk servers send heartbeats to all 3 masters every 0.4 s, so a new leader already knows who is alive.
After 2.5 s of silence a server is considered dead:
1. Its chunks become **degraded**, and new appends go to fresh chunks on healthy servers.
2. The leader seals each degraded chunk and copies it, data plus request-ID index, from a healthy
   replica to a spare server.
3. The leader commits `SET_REPLICAS` with a bumped chunk version.

When the dead server returns, any of its copies with an older version than a chunk that moved on are
deleted. A master deletes only copies it *knows* are outdated. A leader with stale metadata (for
example, one cut off by a partition) never deletes chunks it doesn't know about.

## Raft details

- **Snapshots:** every 500 applied entries (300 in the demo, 40 in the snapshot test) the state
  machine is saved and the log prefix discarded. A follower that falls behind the snapshot gets it
  through `InstallSnapshot`.
- **Check-quorum:** a leader that hasn't heard from a majority for 1 s steps down, so a leader cut
  off by a partition stops accepting requests it can never commit.
- **Pre-vote:** a node asks "would you vote for me?" before raising its term. An isolated node
  therefore can't inflate its term and force a needless election when it rejoins (measured: the
  term rose by 4 without pre-vote, by 1 with it).

## The 5 crash points

| Failpoint | Where the process dies |
| --- | --- |
| `master.before_intent` | leader got the append, nothing logged |
| `master.after_intent` | intent committed, no chunk written, no reply |
| `cs.mid_write` | a chunk server wrote data but died before fsync + index (partial replica) |
| `master.before_done` | all replicas hold the record, completion not committed |
| `master.after_done` | completion committed, client never hears back |

The checker reads raw chunk bytes from every replica and compares them with each client's log. It
reports three kinds of violation:
- **duplicates:** a logical record visible twice
- **lost:** acknowledged but not visible
- **mismatch:** replicas disagree

A trial counts only if its crash actually fired while appends were in flight.

## Measured results

Latest full run: `python tests/verify_all.py` (2026-11-17), **15/15 features pass**. The checklist
with evidence is in `results/VERIFY.md`; per-trial data is in `results/*.json`. Each crash trial uses a
fresh cluster, 3 concurrent clients x 60 appends (180 requests), one injected crash, and a restart of
the crashed process after a random 0.3-4 s delay.

| Experiment | Trials | Result |
| --- | --- | --- |
| Exactly-once client, 5 crash points x 5 trials | 25 | **25/25 pass: 0 duplicates, 0 lost, 0 replica mismatches**; the crash fired in every trial; 218 retries recognised by request ID |
| Baseline: fresh ID per retry (plain at-least-once) | 10 | 8/10 trials produce duplicates (13 duplicate records in total); the two `cs.mid_write` trials pass because completion needs all 3 replicas |
| Unfinished appends settled by new leaders | all trials | 6 committed (all replicas had them), 37 aborted (some replica lacked them) |
| Leader failover, 20 back-to-back leader kills under load | 20 | kill to new leader serving: p50 0.78 s, p95 0.94 s, max 0.97 s (target was under 2 s) |
| Replica repair: kill a chunk server under load | 1 | degraded detected 2.4 s after the kill; all 7 chunks back to 3 live replicas by 2.6 s; 4 chunks moved to version 2; returned server kept 0 stale chunks; 120/120 records |
| Restart all 3 masters, then restart the client | 1 | all 35 records survive; replayed request ID still answered "committed"; the restarted client resumed at sequence 31 |
| Snapshot and InstallSnapshot | 1 | leader kept 37 log entries after 317 commits; a follower that missed all of it caught up in 1.3 s; 170/170 records after a full restart |
| Partition the leader from the other masters | 1 | new leader in 0.78 s; isolated leader stepped down after 1.1 s; the term rose by 1; 120/120 records, 0 duplicates |
| Fresh-copy check of this README (new virtual environment, only the steps above), 2026-11-25 | 1 | unit tests, smoke test and the full verification all pass, 15/15; failover p50 0.72 s, p95 0.92 s; checklist in `results/VERIFY_cleanroom_2026-11-25.md` |
| Live demo driven through its HTTP API | 1 | crash button changed the leader; the checker took 0.7 s: exactly-once 0 duplicates, at-least-once 1 duplicate; `--cleanup` freed all 8 ports after a hard kill |

Timings vary a little from run to run (the figures in this table are from the first full run; the
fresh-copy run above gave slightly different ones), but the pass/fail results were the same.

Client-visible append stall during a failure (worst case in the campaign): 1.0-1.7 s on a master crash,
2.6 s on a chunk-server crash (the 2.5 s dead-server timeout before the degraded chunk is sealed).
Aborted attempts leave 33 orphaned index entries on replicas across the 5 `cs.mid_write` trials; they
are never visible to readers.

Interpretation: baseline duplicates appear whenever a master dies while appends are in flight, because a
client that cannot tell whether its first attempt committed retries under a new ID, and the new leader
then commits the first attempt too. Request IDs turn that retry into a no-op.

## Design tradeoffs

| Decision | What it buys | What it costs | Alternative |
| --- | --- | --- | --- |
| Master assigns offsets through Raft | One source of truth, so a retry after any crash is a table lookup | Two Raft commits per append; the begin and commit phases are about three quarters of the 44 ms median | Primary chunk server assigns offsets under a lease: fewer master round trips, but needs a second recovery protocol |
| Ack only after **all 3** replicas fsync | Any surviving replica can serve any acknowledged record | One dead replica stalls appends to its chunk until the 2.5 s timeout seals it (2.6 s worst stall measured) | Quorum writes (2 of 3) with background repair: lower tail latency, but replicas can lag |
| 2.5 s dead-server timeout | Repair starts about 3 s after a crash | A long pause (GC, swap) could trigger a needless repair | Longer timeout: fewer false alarms, slower repair |
| Reconcile grace 1.5 s / abort after 4 s | Doesn't abort appends that are merely slow | Unfinished appends linger a few seconds after a crash | Shorter: faster cleanup, more aborted retries |
| fsync on every Raft entry and chunk write | Nothing acknowledged is lost on power failure | fsync dominates latency | Group commit (batch fsyncs): more throughput, slightly higher latency |
| Parallel replica writes | Write phase 15 ms to 7.5 ms | Client sends 3 copies (3x client bandwidth) | GFS-style pipelined chain: 1x client bandwidth, more latency per hop |
| Snapshot every N entries | Bounded log; fast restart; laggards catch up | Each snapshot serialises the full state | Incremental snapshots / tiered storage |
| Pre-vote + check-quorum | Stable leadership across partitions | Up to one extra round trip before an election (failover unchanged, about 0.7 s) | Plain Raft: simpler, but disruptive after partitions |
| Client-chosen IDs `client:seq` | Retries work against any leader | Client must keep its sequence durable (done: resumed from its log) | Server-issued IDs: needs an extra round trip first |
| JSON over gRPC | No code generation, easy to debug | Larger messages, more CPU than protobuf | Protocol Buffers |

**Performance** (`python tests/bench.py`, this laptop):

| Configuration | Appends/s | p50 | p95 | Phases (p50) |
| --- | --- | --- | --- | --- |
| 1 client, sequential replica writes | 19.0 | 43.7 ms | 100.8 ms | begin 10.4 / write 15.3 / commit 17.2 ms |
| 1 client, parallel replica writes | 19.7 | 43.9 ms | 97.9 ms | begin 11.9 / write 7.5 / commit 21.4 ms |
| 3 concurrent clients, parallel writes | 46.0 | 61.4 ms | 106.0 ms | begin 16.6 / write 11.3 / commit 28.9 ms |

Keeping chunk files open instead of reopening them per write took throughput from 4.9 to 19
appends/s. On Windows each close triggered an antivirus scan that grew with the file.

## Differences from the original proposal

* **Python, not Go** (Go was not installed). Same architecture.
* **gRPC with a JSON codec** instead of Protocol Buffers: no code generation step. Still real gRPC.
* **Raft is our own implementation**, now with snapshots and log compaction (proposal goal 3).
  Membership changes are not supported.
* **Offsets are assigned by the Raft-replicated master**, not by a primary chunk server as in GFS.
* **The dashboard is a custom web page** with controls, not Grafana/Prometheus.
* **No Docker Compose:** nodes are local processes. Partitions are injected in the RPC layer instead
  of with `tc`/`iptables`.
* **The RCE was not needed:** everything runs on a laptop.
* **Chunk sizes are small in tests:** 2-8 KB (256 KB default) so tests span many chunks. Real GFS
  uses 64 MB.
