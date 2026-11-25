"""Chunk server: stores chunk replicas as files and tracks which request IDs they contain.

Files per chunk in data/<id>/:
    <chunk>.dat   record bytes at their assigned offsets
    <chunk>.idx   one JSON line per durably written record: request ID, offset, size, crc32
    <chunk>.ver   chunk version (bumped when the replica set changes)

A record counts as present only once its data AND its index line are fsynced, so a crash in
between (failpoint cs.mid_write) leaves a replica that honestly reports it does not hold the record.
"""
import argparse
import base64
import json
import os
import threading
import time
import zlib
from collections import defaultdict

from . import config, failpoints as fp, rpc


class ChunkServer:
    def __init__(self, sid):
        self.id = sid
        self.dir = os.path.join(config.DATA_DIR, sid)
        os.makedirs(self.dir, exist_ok=True)
        self.locks = defaultdict(threading.Lock)
        self.index = {}                  # chunk -> {req: (offset, size, crc)}
        self.versions = {}
        self.handles = {}                # chunk -> open {"dat", "idx"} files (avoid reopen per write)
        self._recover()
        self.server = rpc.serve(config.CHUNKSERVERS[sid], {
            "WriteAt": self.write_at, "Has": self.has, "ReadAt": self.read_at,
            "ReadChunk": self.read_chunk, "CopyChunk": self.copy_chunk,
            "ListIndex": self.list_index, "Status": self.status,
            "SetFailpoint": self.set_failpoint,
        })
        threading.Thread(target=self._heartbeats, daemon=True).start()

    # ------------------------------------------------------------------ files
    def _p(self, chunk, ext):
        return os.path.join(self.dir, f"{chunk}.{ext}")

    def _recover(self):
        for name in os.listdir(self.dir):
            if not name.endswith(".dat"):
                continue
            cid = int(name[:-4])
            idx = {}
            if os.path.exists(self._p(cid, "idx")):
                with open(self._p(cid, "idx"), "rb") as f:
                    for line in f:
                        try:
                            e = json.loads(line)
                        except ValueError:      # torn last line
                            break
                        idx[e["req"]] = (e["off"], e["size"], e["crc"])
            self.index[cid] = idx
            if os.path.exists(self._p(cid, "ver")):
                self.versions[cid] = int(open(self._p(cid, "ver")).read() or 1)

    def _h(self, cid):
        """Open handles for a chunk, kept for its lifetime (call with the chunk lock held).
        Reopening per write cost ~50 ms on Windows, mostly antivirus scanning on close."""
        h = self.handles.get(cid)
        if h is None:
            dat = self._p(cid, "dat")
            h = {"dat": open(dat, "r+b" if os.path.exists(dat) else "w+b"),
                 "idx": open(self._p(cid, "idx"), "ab")}
            self.handles[cid] = h
        return h

    def _close(self, cid):
        h = self.handles.pop(cid, None)
        if h:
            h["dat"].close()
            h["idx"].close()

    def _set_version(self, cid, version):
        with open(self._p(cid, "ver"), "w") as f:
            f.write(str(version))
            f.flush()
            os.fsync(f.fileno())
        self.versions[cid] = version

    # ------------------------------------------------------------------ RPCs
    def write_at(self, req):
        cid, req_id, off = req["chunk"], req["req"], req["offset"]
        data = base64.b64decode(req["data"])
        crc = zlib.crc32(data)
        with self.locks[cid]:
            have = self.versions.get(cid)
            if have is None:
                self._set_version(cid, req["version"])
            elif req["version"] != have:
                return {"error": "stale_version", "have": have}
            idx = self.index.setdefault(cid, {})
            if idx.get(req_id) == (off, len(data), crc):
                return {"ok": True, "dup": True}        # retried write: already durable
            h = self._h(cid)
            f = h["dat"]
            f.seek(off)
            f.write(data)
            f.flush()
            fp.hit("cs.mid_write")                      # data written, not yet fsynced or indexed
            os.fsync(f.fileno())
            g = h["idx"]
            g.write((json.dumps({"req": req_id, "off": off, "size": len(data), "crc": crc})
                     + "\n").encode())
            g.flush()
            os.fsync(g.fileno())
            idx[req_id] = (off, len(data), crc)
        return {"ok": True}

    def has(self, req):
        e = self.index.get(req["chunk"], {}).get(req["req"])
        return {"has": bool(e and e[0] == req["offset"] and e[1] == req["size"])}

    def read_at(self, req):
        cid = req["chunk"]
        with self.locks[cid]:
            if cid not in self.index:
                return {"rpc_error": f"no chunk {cid} here"}
            f = self._h(cid)["dat"]
            f.seek(req["offset"])
            data = f.read(req["size"])
        return {"data": base64.b64encode(data).decode(), "crc": zlib.crc32(data)}

    def list_index(self, req):
        idx = self.index.get(req["chunk"], {})
        return {"entries": [{"req": r, "off": e[0], "size": e[1], "crc": e[2]}
                            for r, e in idx.items()]}

    def read_chunk(self, req):
        cid = req["chunk"]
        with self.locks[cid]:
            if cid not in self.index:
                return {"rpc_error": f"no chunk {cid} here"}
            f = self._h(cid)["dat"]
            f.seek(0)
            dat = f.read()
            with open(self._p(cid, "idx"), "rb") as g:
                idx = g.read()
        return {"dat": base64.b64encode(dat).decode(), "idx": base64.b64encode(idx).decode()}

    def copy_chunk(self, req):
        """Repair: pull a full replica (data + index) from a healthy source and make it durable."""
        cid = req["chunk"]
        src = rpc.call(req["source"], "ReadChunk", {"chunk": cid}, timeout=20)
        with self.locks[cid]:
            self._close(cid)
            for ext, key in (("dat", "dat"), ("idx", "idx")):
                with open(self._p(cid, ext), "wb") as f:
                    f.write(base64.b64decode(src[key]))
                    f.flush()
                    os.fsync(f.fileno())
            self._set_version(cid, req["version"])
            idx = {}
            for line in open(self._p(cid, "idx"), "rb"):
                e = json.loads(line)
                idx[e["req"]] = (e["off"], e["size"], e["crc"])
            self.index[cid] = idx
        return {"ok": True}

    def status(self, req):
        return {"id": self.id, "chunks": {str(c): self.versions.get(c) for c in self.index}}

    def set_failpoint(self, req):
        fp.arm(req["name"], req.get("count", 1))
        return {"ok": True}

    def _delete(self, cid):
        with self.locks[cid]:
            self._close(cid)                            # Windows cannot delete open files
            for ext in ("dat", "idx", "ver"):
                try:
                    os.remove(self._p(cid, ext))
                except FileNotFoundError:
                    pass
            self.index.pop(cid, None)
            self.versions.pop(cid, None)

    # ------------------------------------------------------------------ heartbeats
    def _heartbeats(self):
        while True:
            inventory = {str(c): self.versions.get(c, 0) for c in list(self.index)}
            for addr in config.master_addrs().values():
                try:
                    r = rpc.call(addr, "Heartbeat", {"id": self.id, "chunks": inventory},
                                 timeout=0.4)
                except rpc.RpcError:
                    continue
                for cid in r.get("delete", []):
                    self._delete(cid)                  # stale replica of a chunk that moved on
                for cid_s, ver in r.get("set_version", {}).items():
                    with self.locks[int(cid_s)]:
                        self._set_version(int(cid_s), ver)
            time.sleep(config.HEARTBEAT)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, choices=sorted(config.CHUNKSERVERS))
    args = ap.parse_args()
    ChunkServer(args.id)
    print(f"chunk server {args.id} listening on {config.CHUNKSERVERS[args.id]}", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
