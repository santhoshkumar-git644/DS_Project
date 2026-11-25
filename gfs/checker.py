"""Independent verification: compare what the cluster stores with what clients were acknowledged.

Reads the client logs (SENT / ACK lines) and the list of committed records, then reads the raw
chunk bytes from every reachable replica (one ReadChunk per replica) and slices out each record.
Violations:
    duplicates   a logical record visible to readers more than once
    lost         acknowledged by a client but not visible to readers
    mismatch     replicas disagree on a record's bytes, or a record is missing on a replica
Informational:
    committed_unacked  committed but the client has not (yet) seen the ack: in flight, or the
                       client crashed after commit
    orphans            records physically on a replica but never committed (aborted attempts)
"""
import base64
import collections
import json

from . import client as client_mod, rpc


def load_logs(paths):
    sent, acked = set(), {}
    for p in paths:
        with open(p) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:             # log being written right now
                    continue
                if e["ev"] == "SENT":
                    sent.add(e["logical"])
                elif e["ev"] == "ACK":
                    acked[e["logical"]] = e
    return sent, acked


def _fetch(addr, cid):
    """(chunk bytes, index entries) from one replica, or None if unreachable."""
    try:
        r = rpc.call(addr, "ReadChunk", {"chunk": cid}, timeout=5.0)
    except rpc.RpcError:
        return None
    entries = [json.loads(l) for l in base64.b64decode(r["idx"]).splitlines() if l.strip()]
    return base64.b64decode(r["dat"]), entries


def check(file, log_paths):
    sent, acked = load_logs(log_paths)
    c = client_mod.Client("checker")
    listing = c._master("ListRecords", {"file": file})
    by_chunk = collections.defaultdict(list)
    for rec in listing["records"]:
        by_chunk[rec["chunk"]].append(rec)

    counts = collections.Counter()
    mismatch, unreachable, orphans = [], 0, 0
    for cid_s, replicas in listing["chunks"].items():
        cid = int(cid_s)
        recs = by_chunk.get(cid, [])
        copies = {}
        for rid, addr in replicas:
            got = _fetch(addr, cid)
            if got is None:
                unreachable += 1
            else:
                copies[rid] = got
        if recs and not copies:
            mismatch.append({"chunk": cid, "why": "no replica readable"})
            continue
        committed = {(r["req"], r["offset"]) for r in recs}
        for rid, (dat, entries) in copies.items():
            orphans += sum(1 for e in entries if (e["req"], e["off"]) not in committed)
        for rec in recs:
            o, n = rec["offset"], rec["size"]
            pieces = {rid: dat[o:o + n] for rid, (dat, _) in copies.items()}
            short = [rid for rid, p in pieces.items() if len(p) != n]
            if short:
                mismatch.append({"rec": rec, "why": f"record missing on {short}"})
            if len(set(pieces.values())) > 1:
                mismatch.append({"rec": rec, "why": "replicas differ"})
            data = next(iter(pieces.values()))
            counts[data.split(b"|", 1)[0].decode(errors="replace")] += 1

    duplicates = {l: n for l, n in counts.items() if n > 1}
    lost = sorted(l for l in acked if counts[l] == 0)
    committed_unacked = sorted(l for l in counts if l not in acked)
    never_acked = sorted(l for l in sent if l not in acked and counts[l] == 0)
    return {
        "ok": not duplicates and not lost and not mismatch,
        "records_visible": sum(counts.values()), "logical_sent": len(sent),
        "logical_acked": len(acked),
        "duplicates": duplicates, "lost": lost, "mismatch": mismatch,
        "committed_unacked": committed_unacked, "never_acked": never_acked,
        "orphan_index_entries": orphans, "unreachable_reads": unreachable,
    }


if __name__ == "__main__":
    import sys
    res = check(sys.argv[1], sys.argv[2:])
    print(json.dumps(res, indent=2))
    sys.exit(0 if res["ok"] else 1)
