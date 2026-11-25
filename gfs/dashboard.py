"""Live fault-tolerance dashboard: polls every master and serves one web page.

Shows leader and term, chunk-server health, per-chunk replica state (healthy / degraded /
repairing), append counters (commits, retries caught by dedup, aborts), and an event log of
elections, failures, recovery and repair.

    python -m gfs.dashboard      read-only view of a running cluster
    python demo.py               same page plus a control panel (crash, partition, verify...)

Open http://127.0.0.1:8765
"""
import collections
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config, rpc

HERE = os.path.dirname(os.path.abspath(__file__))


class Poller:
    def __init__(self):
        self.lock = threading.Lock()
        self.masters = {m: {"up": False, "status": None} for m in config.MASTERS}
        self.since = {m: 0 for m in config.MASTERS}
        self.events = collections.deque(maxlen=400)
        self.best = None                       # newest status from a ready leader
        threading.Thread(target=self._loop, daemon=True).start()

    def _note(self, node, kind, **kw):
        self.events.append({"ts": time.time(), "node": node, "kind": kind, **kw})

    def _loop(self):
        while True:
            for m, port in config.MASTERS.items():
                try:
                    st = rpc.call(config.addr(port), "Status", {"since": self.since[m]},
                                  timeout=0.4)
                    up = True
                except rpc.RpcError:
                    st, up = None, False
                with self.lock:
                    was = self.masters[m]["up"]
                    if up and not was and self.masters[m]["status"] is not None:
                        self._note(m, "node_up")
                    if was and not up:
                        self._note(m, "node_down")
                    self.masters[m]["up"] = up
                    if st:
                        if st["event_seq"] < self.since[m]:     # master restarted, seq reset
                            self.since[m] = 0
                        for e in st["events"]:
                            self.events.append(e)
                            self.since[m] = max(self.since[m], e["seq"])
                        self.masters[m]["status"] = {k: v for k, v in st.items()
                                                     if k not in ("events", "chunks", "servers")}
                        if st["role"] == "leader" and st["ready"]:
                            self.best = st
            time.sleep(0.4)

    def snapshot(self):
        with self.lock:
            leader = next((m for m, v in self.masters.items()
                           if v["up"] and v["status"] and v["status"]["role"] == "leader"
                           and v["status"]["ready"]), None)
            view = self.best or {}
            return {"ts": time.time(), "chunk_size": config.CHUNK_SIZE, "leader": leader,
                    "masters": self.masters,
                    "servers": view.get("servers", {}), "chunks": view.get("chunks", []),
                    "stats": view.get("stats", {}), "pending": view.get("pending", 0),
                    "dedup_size": view.get("dedup_size", 0), "files": view.get("files", {}),
                    "events": sorted(self.events, key=lambda e: -e["ts"])[:120]}


class Dashboard:
    """Serves the page and /state; with a controller it also accepts POST /action/<name>."""

    def __init__(self, controller=None):
        self.poller = Poller()
        self.controller = controller

    def _handler(self):
        dash = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, body, ctype):
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path.startswith("/state"):
                    state = dash.poller.snapshot()
                    state["controls"] = dash.controller.state() if dash.controller else None
                    self._send(json.dumps(state).encode(), "application/json")
                else:
                    with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                        self._send(f.read(), "text/html; charset=utf-8")

            def do_POST(self):
                if not dash.controller or not self.path.startswith("/action/"):
                    self.send_error(404)
                    return
                n = int(self.headers.get("Content-Length") or 0)
                args = json.loads(self.rfile.read(n) or b"{}")
                try:
                    result = dash.controller.act(self.path[len("/action/"):], args)
                except Exception as e:
                    result = {"error": f"{type(e).__name__}: {e}"}
                self._send(json.dumps(result).encode(), "application/json")

        return Handler

    def serve_forever(self, port=None):
        srv = ThreadingHTTPServer((config.HOST, port or config.DASHBOARD_PORT), self._handler())
        srv.daemon_threads = True
        srv.serve_forever()

    def start(self, port=None):
        srv = ThreadingHTTPServer((config.HOST, port or config.DASHBOARD_PORT), self._handler())
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv


def main():
    print(f"dashboard on http://{config.HOST}:{config.DASHBOARD_PORT}", flush=True)
    Dashboard().serve_forever()


if __name__ == "__main__":
    main()
