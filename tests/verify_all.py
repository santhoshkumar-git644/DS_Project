"""Verify every feature of the project and write a checklist to results/VERIFY.md.

    python tests/verify_all.py            full run (~20 min)
    python tests/verify_all.py --quick    fewer crash trials (~10 min)

Runs the unit tests, every crash-test scenario, and drives the live demo through its HTTP control
API (crash the leader, verify, then kill the demo process and recover with --cleanup). Each feature
is judged from the evidence those runs produce.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from gfs import config
from gfs.cluster import busy_ports

OUT = os.path.join(ROOT, "results")
PY = sys.executable


def run(name, args, timeout=3600):
    t0 = time.time()
    log = os.path.join(OUT, f"verify_{name}.log")
    with open(log, "w") as f:
        code = subprocess.call(args, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT, timeout=timeout)
    print(f"  {name:<10} exit={code}  {time.time() - t0:5.0f}s  (log: results/verify_{name}.log)",
          flush=True)
    return code


def load(name):
    p = os.path.join(OUT, f"{name}.json")
    return json.load(open(p)) if os.path.exists(p) else None


def http(path, body=None, timeout=60):
    url = f"http://{config.HOST}:{config.DASHBOARD_PORT}{path}"
    data = None if body is None else json.dumps(body).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=timeout) as r:
        return json.loads(r.read())


def demo_check():
    """Start demo.py, drive it through the dashboard's control API, then kill it hard and make
    sure --cleanup removes every orphaned node process."""
    out = {}
    log = open(os.path.join(OUT, "verify_demo.log"), "w")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    p = subprocess.Popen([PY, "demo.py", "--no-browser"], cwd=ROOT, stdout=log,
                         stderr=subprocess.STDOUT, creationflags=flags)
    try:
        _drive_demo(out)
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        p.kill()                                        # simulate the presenter's laptop crashing
        p.wait()
    time.sleep(1)
    out["ports_busy_after_hard_kill"] = len(busy_ports())
    subprocess.call([PY, "demo.py", "--cleanup"], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    out["ports_busy_after_cleanup"] = len(busy_ports())
    json.dump(out, open(os.path.join(OUT, "demo_check.json"), "w"), indent=1)
    return out


def _drive_demo(out):
    end = time.time() + 40
    state = None
    while time.time() < end:
        try:
            state = http("/state", timeout=2)
            if state.get("leader") and state.get("controls"):
                break
        except OSError:
            pass
        time.sleep(0.5)
    out["dashboard_serves_state"] = bool(state and state.get("controls"))
    http("/action/workload", {"mode": "exactly-once", "on": True})
    http("/action/workload", {"mode": "at-least-once", "on": True})
    time.sleep(4)
    old = http("/state")["leader"]
    http("/action/crash_leader", {"point": "master.after_done"})
    time.sleep(8)
    st = http("/state")
    out["leader_changed_via_button"] = bool(st["leader"] and st["leader"] != old)
    out["crash_narrated"] = any("crashed at the injected crash point" in n["text"]
                                for n in st["controls"]["notes"])
    http("/action/workload", {"mode": "exactly-once", "on": False})
    http("/action/workload", {"mode": "at-least-once", "on": False})
    time.sleep(3)
    t0 = time.time()
    v = http("/action/verify", {}, timeout=120)         # a body makes it a POST
    out["verify_seconds"] = round(time.time() - t0, 1)
    out["verify_exactly_once"] = v["results"]["exactly-once"]
    out["verify_at_least_once"] = v["results"]["at-least-once"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()
    trials, btrials = (2, 1) if args.quick else (5, 2)
    os.makedirs(OUT, exist_ok=True)
    if busy_ports():
        sys.exit("cluster ports are busy: stop the running demo first (python demo.py --cleanup)")

    print("running:", flush=True)
    codes = {
        "unit": run("unit", [PY, "-m", "unittest", "tests.test_statemachine", "-v"]),
        "campaign": run("campaign", [PY, "tests/crash_tests.py", "campaign", "--trials", str(trials)]),
        "baseline": run("baseline", [PY, "tests/crash_tests.py", "baseline", "--trials", str(btrials)]),
        "repair": run("repair", [PY, "tests/crash_tests.py", "repair"]),
        "restart": run("restart", [PY, "tests/crash_tests.py", "restart"]),
        "snapshot": run("snapshot", [PY, "tests/crash_tests.py", "snapshot"]),
        "partition": run("partition", [PY, "tests/crash_tests.py", "partition"]),
        "failover": run("failover", [PY, "tests/crash_tests.py", "failover"]),
    }
    t0 = time.time()
    demo = demo_check()
    print(f"  demo       {demo.get('error', 'ok')}  {time.time() - t0:5.0f}s", flush=True)

    unit_log = open(os.path.join(OUT, "verify_unit.log")).read()
    unit_n = unit_log.count(" ... ok")
    camp, base = load("campaign") or [], load("baseline") or []
    repair, restart, snap, part = load("repair") or {}, load("restart") or {}, \
        load("snapshot") or {}, load("partition") or {}
    masters = [r for r in camp if r["point"].startswith("master.")]
    fo = [r.get("failover_s") for r in masters]
    per_point = {}
    for r in camp:
        per_point.setdefault(r["point"], []).append(r)
    rec = [tuple(int(x[:-1]) for x in r.get("reconcile", "0c/0a").split("/")) for r in camp + base]
    rc, ra = sum(c for c, _ in rec), sum(a for _, a in rec)
    dedup_sizes = [r.get("dedup_size", 0) for r in camp]
    eo, alo = demo.get("verify_exactly_once", {}), demo.get("verify_at_least_once", {})

    F = []
    def feat(name, ok, evidence):
        F.append((name, bool(ok), evidence))

    feat("State-machine rules (dedup, abort, seal, versions, trimming, snapshot round trip)",
         codes["unit"] == 0, f"{unit_n} unit tests pass")
    fov = load("failover") or {}
    feat("Raft leader election and failover (target: under 2 s)",
         masters and all(x is not None for x in fo) and fov.get("max_s", 99) <= 2.0,
         (f"new leader in all {len(masters)} master-crash trials ({min(fo):.2f}-{max(fo):.2f} s); "
          f"{fov.get('kills')} back-to-back leader kills: p50 {fov.get('p50_s')} s, "
          f"p95 {fov.get('p95_s')} s, max {fov.get('max_s')} s")
         if masters and all(x is not None for x in fo) else "missing")
    feat("Exactly-once append at all 5 crash points",
         codes["campaign"] == 0 and len(per_point) == 5 and all(r.get("fired") for r in camp),
         f"{sum(r.get('ok', False) for r in camp)}/{len(camp)} trials pass "
         f"({trials} per point, crash fired in {sum(bool(r.get('fired')) for r in camp)}); "
         f"duplicates={sum(r.get('duplicates', 0) for r in camp)}, "
         f"lost={sum(r.get('lost', 0) for r in camp)}")
    feat("Acknowledgement rule: every ack is on all 3 replicas, byte-identical",
         camp and all(r.get("lost") == 0 and r.get("mismatch") == 0 for r in camp)
         and repair.get("mismatch") == 0,
         "0 lost and 0 replica mismatches in every campaign trial and the repair run")
    # Where an earlier attempt was already logged, the retry must hit the dedup table.
    # (master.before_intent crashes before anything is logged, so it has nothing to recognise.)
    logged = [p for p in ("master.after_intent", "master.before_done", "master.after_done",
                          "cs.mid_write") if p in per_point]
    feat("Retries are recognised by request ID",
         len(logged) == 4 and all(r.get("dedup_hits", 0) > 0 for p in logged for r in per_point[p]),
         "every trial at the 4 crash points after the intent recognised its retries; "
         f"{sum(r.get('dedup_hits', 0) for r in camp)} retries recognised in total")
    feat("Recovery of unfinished appends (both commit and abort paths)",
         rc > 0 and ra > 0, f"new leaders committed {rc} and aborted {ra} unfinished appends")
    feat("Baseline (fresh ID per retry) does produce duplicates",
         sum(r.get("duplicates", 0) for r in base) > 0,
         f"{sum(r.get('duplicates', 0) for r in base)} duplicate records across {len(base)} baseline trials")
    feat("Dedup table trimming (low-water mark)",
         dedup_sizes and max(dedup_sizes) <= 12,
         f"dedup table held at most {max(dedup_sizes) if dedup_sizes else '?'} entries "
         f"after {camp[0]['sent'] if camp else '?'} appends per trial")
    feat("Durable metadata: all 3 masters restart; client restart resumes its IDs",
         restart.get("ok"),
         f"all records survive; replayed request ID answered "
         f"'{restart.get('retry_status_after_restart')}'; restarted client continued at seq "
         f"{restart.get('restarted_client_resumes_at_seq')} ({restart.get('visible')} records, "
         f"{restart.get('duplicates')} duplicates)")
    feat("Replica health detection and re-replication",
         repair.get("ok"),
         f"degraded detected {repair.get('degraded_detected_s')} s after the kill; 3 live replicas "
         f"again at {repair.get('fully_replicated_again_s')} s; {repair.get('repairs_committed')} repairs")
    feat("Stale replicas deleted, chunk versions bumped",
         repair.get("ok") and repair.get("returned_server_stale_chunks_left") == [],
         f"returned server kept 0 stale chunks; {repair.get('repaired_chunks_version_bumped')} "
         "chunks at version 2")
    feat("Raft snapshots, log compaction, InstallSnapshot",
         snap.get("ok"),
         f"leader kept {snap.get('leader_log_entries_kept')} log entries after "
         f"{snap.get('leader_commit')} commits; lagging follower caught up by snapshot in "
         f"{snap.get('follower_caught_up_s')} s; {snap.get('visible')} records after full restart")
    feat("Network partition: check-quorum and pre-vote",
         part.get("ok") and part.get("terms_advanced", 99) <= 2,
         f"new leader {part.get('new_leader_elected_s')} s, isolated leader stepped down "
         f"{part.get('old_leader_stepped_down_s')} s; term advanced by {part.get('terms_advanced')}; "
         f"{part.get('duplicates')} duplicates, {part.get('lost')} lost")
    feat("Live demo: dashboard, control buttons, narration, checker",
         demo.get("dashboard_serves_state") and demo.get("leader_changed_via_button")
         and demo.get("crash_narrated") and eo.get("ok"),
         f"crash button changed the leader; Verify took {demo.get('verify_seconds')} s: "
         f"exactly-once {eo.get('duplicates')} duplicates / {eo.get('lost')} lost, "
         f"at-least-once {alo.get('duplicates')} duplicates")
    feat("Recovery from a crashed demo (--cleanup)",
         demo.get("ports_busy_after_hard_kill", 0) > 0 and demo.get("ports_busy_after_cleanup") == 0,
         f"{demo.get('ports_busy_after_hard_kill')} ports held by orphans after killing the demo; "
         f"{demo.get('ports_busy_after_cleanup')} after --cleanup")

    lines = ["# Feature verification", "",
             f"Run {time.strftime('%Y-%m-%d %H:%M')} ({'quick' if args.quick else 'full'}). "
             "Generated by `python tests/verify_all.py`.", "",
             "| # | Feature | Result | Evidence |", "|---|---|---|---|"]
    for i, (name, ok, ev) in enumerate(F, 1):
        lines.append(f"| {i} | {name} | {'PASS' if ok else '**FAIL**'} | {ev} |")
    passed = sum(ok for _, ok, _ in F)
    lines += ["", f"**{passed}/{len(F)} features verified.**"]
    open(os.path.join(OUT, "VERIFY.md"), "w").write("\n".join(lines) + "\n")
    print()
    for i, (name, ok, ev) in enumerate(F, 1):
        print(f"{'PASS' if ok else 'FAIL'}  {i:2}. {name}\n        {ev}")
    print(f"\n{passed}/{len(F)} features verified -> results/VERIFY.md")
    sys.exit(0 if passed == len(F) else 1)


if __name__ == "__main__":
    main()
