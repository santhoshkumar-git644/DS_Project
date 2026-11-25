"""Named crash points. When an armed failpoint is hit the process dies instantly (no cleanup),
which is what a power loss or kill -9 looks like to the rest of the cluster.

Crash points used by the test campaign:
  master.before_intent  leader received the append, nothing logged yet
  master.after_intent   intent committed in Raft, reply not yet sent, no chunk data written
  cs.mid_write          chunk server wrote data but crashed before fsync + index (partial replica write)
  master.before_done    all replicas hold the record, completion not yet committed
  master.after_done     completion committed, client not yet told
"""
import os
import sys
import threading

POINTS = ["master.before_intent", "master.after_intent", "cs.mid_write",
          "master.before_done", "master.after_done"]

_lock = threading.Lock()
_armed = {}


def arm(name, count=1):
    with _lock:
        _armed[name] = count


def disarm_all():
    with _lock:
        _armed.clear()


def hit(name):
    with _lock:
        left = _armed.get(name, 0)
        if left <= 0:
            return
        _armed[name] = left - 1
    sys.stderr.write(f"FAILPOINT {name}: crashing now\n")
    sys.stderr.flush()
    os._exit(99)


for _name in filter(None, os.environ.get("GFS_FAILPOINTS", "").split(",")):
    arm(_name.strip())
