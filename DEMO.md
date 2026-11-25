# Presenting the demo

**The point in one sentence:** when the master crashes in the middle of an append, a client that
retries must not create a second record, and this system shows that it doesn't, by breaking the
system on stage and checking the stored bytes afterwards.

## Before you present (5 minutes)

1. `pip install -r requirements.txt` (only `grpcio`).
2. `python demo.py --cleanup`: kills node processes left over from an earlier run.
3. `python demo.py`: starts 3 masters, 5 chunk servers and the dashboard, and opens
   http://127.0.0.1:8765. Wait for the green banner "Cluster ready".
4. Set the browser zoom to 80-90% so the controls and the master cards fit on the projector.
5. If port 8765 is taken: in PowerShell `$env:GFS_DASHBOARD_PORT=8790; python demo.py`.

**Fallbacks:**
- **Hands-free run:** `python demo.py --autoplay` plays the whole scenario by itself in about 2 minutes.
- **No live demo:** screenshots of every scene are in `results/demo_*.png`.
- **Full results:** the checklist is in `results/VERIFY.md`.

## Live script (about 8 minutes)

The banner under **"What is happening"** narrates every step in plain words. Read it aloud if you
lose your place.

| # | Click | What the audience sees | What to say |
|---|---|---|---|
| 0 | nothing | 3 master cards (one **leader**), 5 chunk servers | "Metadata is replicated on 3 masters with Raft. Data is 3-way replicated on chunk servers." |
| 1 | **Start exactly-once clients**, then **Start at-least-once clients** | both workload counters climbing | "Same cluster, two kinds of client. The left one reuses its request ID when it retries. The right one takes a new ID, like original GFS." |
| 2 | **Crash the leader: after completion, before the ack** (red button) | leader card goes DOWN; banner shows the crash and "new leader ... 0.7 s"; events show `dedup_hit ... was=committed` | "The append was committed, but the leader died before the client heard back. The client must retry. Here the retry is recognised (`dedup_hit`) and gets back the original offset instead of writing again." |
| 3 | **Verify now** | Checker table: exactly-once **0 duplicates**, at-least-once **duplicates** in red | "The checker reads the raw bytes from every replica and compares them with each client's log of acknowledged appends. Same crash, same cluster: one client type gets a duplicate, ours doesn't." |
| 4 | **Crash the leader: before completion** | another failover; `reconcile_commit` may appear | "Now the data is on all 3 replicas but completion was never logged. The new leader finds the unfinished intent and finishes it exactly once." |
| 5 | **Chunk server dies mid-write** | a chunk server crashes; *intents aborted* rises | "That replica wrote the bytes but never made them durable, so it honestly says it doesn't have the record. The append is aborted and redone at a new offset. The old bytes are never visible to readers." |
| 6 | **Partition the leader** | leader card shows **partitioned**; events `lost_quorum`, `stepped_down`, new leader; healed after 8 s | "The leader can still talk to clients but not to the other masters. A new leader is elected, and the old one steps down by itself because it can't reach a majority (check-quorum). After the heal the term went up by only one, because pre-vote stopped the cut-off node from inflating its term." |
| 7 | **Kill a chunk server (repair)** | server card goes DEAD, chunks turn **degraded**, then **v2** with a new server | "After 2.5 s without a heartbeat the master copies each affected chunk, including its request-ID index, to a spare server and bumps the chunk version." |
| 8 | **restart** on that server card | its replica count drops to about 0 | "Its old copies are outdated (lower version), so the master tells it to delete them." |
| 9 | point at the master cards | "log N entries + snapshot @X" | "The Raft log doesn't grow forever. Every 300 entries the master snapshots its state, and a follower that falls too far behind is sent the snapshot." |
| 10 | stop both workloads, **Verify now** | final table | "Thousands of appends and seven kinds of failure: zero duplicates and zero lost acknowledged records." |

## Questions you may get

**"Exactly-once delivery is impossible. What do you actually guarantee?"**
Delivery is at-least-once, because the client retries until it gets an ack. The *effect* is
exactly-once: every retry carries the same request ID, and the replicated dedup table turns
repeats into no-ops. The guarantee covers acknowledged appends. An append that was never
acknowledged appears at most once.

**"When exactly is an append acknowledged?"**
When all 3 replicas have fsynced the record and its completion entry is committed by 2 of the 3
masters. A record is visible to readers if and only if that completion entry is committed.

**"Why does the at-least-once client only show 1-3 duplicates?"**
Only appends that are in flight at the moment of the crash are at risk. Every crash is another
chance, and even one duplicate is a correctness bug for an application such as a ledger or a queue.

**"What does exactly-once cost?"**
Two Raft commits per append. On this laptop that's about 44 ms median per append, 19 appends/s
for one client and 46/s for three (`python tests/bench.py`). The Raft commits dominate. Writing
the replicas in parallel halved the replica-write phase but barely changed the total.

**"What if two masters die?"**
There's no majority, so no metadata change can commit and appends stop. Safety is preserved: the
system is consistent but not available in that case (CP). One master failure is tolerated.

**"Doesn't the dedup table grow forever?"**
No. Each request carries the client's low-water mark (every sequence number below it is finished),
and entries at or below it are dropped. Snapshots then keep the Raft log small too.

**"How do you know the checker isn't just trusting the master?"**
It reads raw chunk bytes from every replica and parses each record's own identity out of the
payload. It doesn't use the dedup table, so a duplicate would show up even if the master's
bookkeeping were wrong. It caught the duplicates in the baseline runs.

**"Why not let the primary chunk server choose offsets, like GFS?"**
That saves a master round trip, but then a retry after a crash depends on the primary's state too,
which needs leases and a second recovery protocol. Putting offsets in the Raft-replicated master
makes one place the source of truth. See the tradeoffs in README.md.

**"Is this a real network?"**
Every node is a separate OS process talking gRPC over TCP, with its own data directory, fsyncs and
crash behaviour. Crashes are real process kills. Partitions are injected in the RPC layer on both
sides of the cut.
