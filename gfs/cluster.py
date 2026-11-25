"""Start, kill and restart cluster nodes as local processes.

CLI:  python -m gfs.cluster status | cleanup
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time

from . import config, rpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


PID_FILE = os.path.join(ROOT, "run", "pids.json")


def busy_ports():
    """Cluster ports that something else is already listening on."""
    busy = []
    for port in list(config.MASTERS.values()) + list(config.CHUNKSERVERS.values()):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind((config.HOST, port))
        except OSError:
            busy.append(port)
        finally:
            s.close()
    return busy


def cleanup_stale():
    """Kill node processes left behind by a previous run that did not shut down cleanly."""
    if not os.path.exists(PID_FILE):
        return 0
    killed = 0
    for pid in json.load(open(PID_FILE)).values():
        try:
            os.kill(pid, signal.SIGTERM)
            killed += 1
        except (OSError, SystemError):
            pass
    os.remove(PID_FILE)
    end = time.time() + 5                               # give the OS time to release the ports
    while busy_ports() and time.time() < end:
        time.sleep(0.2)
    return killed


class Cluster:
    def __init__(self, workdir, chunk_size=None, env=None):
        self.workdir = os.path.abspath(workdir)
        self.data = os.path.join(self.workdir, "data")
        self.logs = os.path.join(self.workdir, "logs")
        os.makedirs(self.data, exist_ok=True)
        os.makedirs(self.logs, exist_ok=True)
        self.env = dict(os.environ, GFS_DATA=self.data, **(env or {}))
        if chunk_size:
            self.env["GFS_CHUNK_SIZE"] = str(chunk_size)
        self.procs = {}

    def _save_pids(self):
        os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)
        with open(PID_FILE, "w") as f:
            json.dump({n: p.pid for n, p in self.procs.items() if p.poll() is None}, f)

    # ------------------------------------------------------------------ processes
    def start(self, node, failpoints=""):
        module = "gfs.master" if node in config.MASTERS else (
            "gfs.dashboard" if node == "dashboard" else "gfs.chunkserver")
        args = [sys.executable, "-m", module] + ([] if node == "dashboard" else ["--id", node])
        log = open(os.path.join(self.logs, f"{node}.log"), "ab")
        env = dict(self.env, GFS_FAILPOINTS=failpoints)
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        self.procs[node] = subprocess.Popen(args, cwd=ROOT, env=env, stdout=log, stderr=log,
                                            creationflags=flags)
        self._save_pids()

    def kill(self, node):
        p = self.procs.get(node)
        if p and p.poll() is None:
            p.kill()
            p.wait()
        self._save_pids()

    def alive(self, node):
        p = self.procs.get(node)
        return bool(p and p.poll() is None)

    def restart(self, node):
        self.kill(node)
        self.start(node)

    def start_all(self, dashboard=False):
        # Fail early and clearly if an earlier run (or a demo in another terminal) still holds
        # the ports; otherwise the new nodes would half-start and talk to the old ones.
        end = time.time() + 3                           # a node killed just now may still be closing
        while busy_ports() and time.time() < end:
            time.sleep(0.2)
        busy = busy_ports()
        if busy:
            raise RuntimeError(
                f"ports {busy} are already in use: is a demo or test still running? "
                "Stop it (Ctrl+C in its terminal), or run: python demo.py --cleanup")
        for n in list(config.MASTERS) + list(config.CHUNKSERVERS):
            self.start(n)
        if dashboard:
            self.start("dashboard")

    def stop_all(self):
        for n in list(self.procs):
            self.kill(n)
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)

    def partition(self, isolated):
        """Cut master `isolated` off from the other masters (both directions). Clients and chunk
        servers can still reach every master, which is the dangerous case: a stale leader."""
        others = [m for m in config.MASTERS if m != isolated]
        addrs = config.master_addrs()
        rpc.call(addrs[isolated], "SetPartition", {"blocked": [addrs[m] for m in others]})
        for m in others:
            rpc.call(addrs[m], "SetPartition", {"blocked": [addrs[isolated]]})

    def heal(self):
        for m, a in config.master_addrs().items():
            try:
                rpc.call(a, "SetPartition", {"blocked": []})
            except rpc.RpcError:
                pass

    # ------------------------------------------------------------------ queries
    def status(self, node, timeout=0.5):
        port = config.MASTERS.get(node) or config.CHUNKSERVERS.get(node)
        try:
            return rpc.call(config.addr(port), "Status", {}, timeout=timeout)
        except rpc.RpcError:
            return None

    def leader(self):
        """(id, status) of a master that is leader and ready, else None."""
        for m in config.MASTERS:
            st = self.status(m)
            if st and st["role"] == "leader" and st["ready"]:
                return m, st
        return None

    def wait_ready(self, timeout=30):
        """Wait for a ready leader and for all 5 chunk servers to be seen alive."""
        end = time.time() + timeout
        while time.time() < end:
            lead = self.leader()
            if lead and sum(1 for s in lead[1]["servers"].values() if s["live"]) == len(config.CHUNKSERVERS):
                return lead[0]
            time.sleep(0.2)
        raise TimeoutError("cluster not ready")

    def wait_leader(self, timeout=15, exclude=None):
        end = time.time() + timeout
        while time.time() < end:
            lead = self.leader()
            if lead and lead[0] != exclude:
                return lead[0]
            time.sleep(0.05)
        raise TimeoutError("no leader elected")


if __name__ == "__main__":
    work = os.environ.get("GFS_WORKDIR", os.path.join(ROOT, "run"))
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    c = Cluster(work)
    if cmd == "status":
        for m in config.MASTERS:
            st = c.status(m)
            print(m, f"{st['role']} term={st['term']}" if st else "DOWN")
        for s in config.CHUNKSERVERS:
            print(s, "up" if c.status(s) else "DOWN")
    elif cmd == "cleanup":
        print(f"killed {cleanup_stale()} leftover node processes; busy ports now: {busy_ports()}")
    else:
        print("usage: python -m gfs.cluster status | cleanup")
