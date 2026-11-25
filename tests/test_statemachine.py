"""Unit tests for the replicated master state machine (no processes, no network).

    python -m unittest tests.test_statemachine -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gfs import config
from gfs.statemachine import MasterState


def intent(sm, req, chunk=1, size=100, client="c1", seq=None, low_water=None):
    seq = seq if seq is not None else int(req.split(":")[1])
    return sm.apply({"op": "INTENT", "req": req, "client": client, "seq": seq, "file": "/f",
                     "chunk": chunk, "size": size, "ts": 0.0, "low_water": low_water}, 0)


class StateMachineTest(unittest.TestCase):
    def setUp(self):
        self.sm = MasterState()
        self.sm.apply({"op": "CREATE", "file": "/f"}, 0)
        r = self.sm.apply({"op": "ADD_CHUNK", "file": "/f", "after": None,
                           "replicas": ["cs1", "cs2", "cs3"]}, 0)
        self.assertEqual(r["chunk"], 1)

    def test_offsets_are_assigned_in_order(self):
        a, b = intent(self.sm, "c1:1", size=100), intent(self.sm, "c1:2", size=50)
        self.assertEqual((a["offset"], b["offset"]), (0, 100))

    def test_retry_of_inflight_intent_returns_same_offset(self):
        first = intent(self.sm, "c1:1")
        again = intent(self.sm, "c1:1")
        self.assertEqual(again["status"], "intent")
        self.assertEqual(again["offset"], first["offset"])
        self.assertTrue(again["dedup"])
        self.assertEqual(self.sm.chunks[1]["next"], 100, "a retry must not reserve space again")

    def test_retry_after_commit_returns_original_result(self):
        first = intent(self.sm, "c1:1")
        self.sm.apply({"op": "DONE", "req": "c1:1"}, 0)
        again = intent(self.sm, "c1:1")
        self.assertEqual((again["status"], again["offset"]), ("committed", first["offset"]))
        self.assertEqual(len(self.sm.files["/f"]["records"]), 1)
        self.assertEqual(self.sm.stats["dedup_hits"], 1)

    def test_done_is_idempotent(self):
        intent(self.sm, "c1:1")
        self.sm.apply({"op": "DONE", "req": "c1:1"}, 0)
        self.sm.apply({"op": "DONE", "req": "c1:1"}, 0)
        self.assertEqual(len(self.sm.files["/f"]["records"]), 1)

    def test_abort_then_retry_gets_a_new_offset(self):
        first = intent(self.sm, "c1:1")
        self.sm.apply({"op": "ABORT", "req": "c1:1"}, 0)
        self.assertEqual(self.sm.apply({"op": "DONE", "req": "c1:1"}, 0)["status"], "aborted")
        again = intent(self.sm, "c1:1")
        self.assertEqual(again["status"], "intent")
        self.assertEqual(again["attempt"], 2)
        self.assertGreater(again["offset"], first["offset"])

    def test_abort_never_undoes_a_commit(self):
        intent(self.sm, "c1:1")
        self.sm.apply({"op": "DONE", "req": "c1:1"}, 0)
        self.sm.apply({"op": "ABORT", "req": "c1:1"}, 0)
        self.assertEqual(self.sm.appends["c1:1"]["state"], "committed")

    def test_full_chunk_is_sealed(self):
        r = intent(self.sm, "c1:1", size=config.CHUNK_SIZE + 1)
        self.assertEqual(r["status"], "full")
        self.assertTrue(self.sm.chunks[1]["sealed"])
        self.assertEqual(intent(self.sm, "c1:2")["status"], "sealed")

    def test_seal_aborts_pending_intents(self):
        intent(self.sm, "c1:1")
        self.sm.apply({"op": "SEAL", "chunk": 1}, 0)
        self.assertEqual(self.sm.appends["c1:1"]["state"], "aborted")

    def test_set_replicas_checks_version_and_bumps_it(self):
        intent(self.sm, "c1:1")
        stale = self.sm.apply({"op": "SET_REPLICAS", "chunk": 1, "replicas": ["cs1", "cs2", "cs4"],
                               "expect_version": 7, "version": 8}, 0)
        self.assertEqual(stale["status"], "conflict")
        ok = self.sm.apply({"op": "SET_REPLICAS", "chunk": 1, "replicas": ["cs1", "cs2", "cs4"],
                            "expect_version": 1, "version": 2}, 0)
        self.assertEqual(ok["status"], "ok")
        self.assertEqual(self.sm.chunks[1]["version"], 2)
        self.assertEqual(self.sm.appends["c1:1"]["state"], "aborted")

    def test_add_chunk_is_idempotent_under_races(self):
        r = self.sm.apply({"op": "ADD_CHUNK", "file": "/f", "after": None,
                           "replicas": ["cs1", "cs2", "cs3"]}, 0)
        self.assertEqual((r["status"], r["chunk"]), ("exists", 1))

    def test_low_water_trims_only_acknowledged_committed_entries(self):
        for s in (1, 2):
            intent(self.sm, f"c1:{s}")
            self.sm.apply({"op": "DONE", "req": f"c1:{s}"}, 0)
        intent(self.sm, "c1:3", low_water=1)
        self.assertNotIn("c1:1", self.sm.appends)
        self.assertIn("c1:2", self.sm.appends, "seq 2 was not acknowledged yet")
        self.assertIn("c1:3", self.sm.appends)
        self.assertEqual(len(self.sm.files["/f"]["records"]), 2, "records survive trimming")

    def test_low_water_trims_aborted_but_keeps_unsettled_intents(self):
        intent(self.sm, "c1:1")
        self.sm.apply({"op": "ABORT", "req": "c1:1"}, 0)
        intent(self.sm, "c1:2")                       # still in flight: reconcile must settle it
        intent(self.sm, "c1:3", low_water=2)
        self.assertNotIn("c1:1", self.sm.appends)
        self.assertEqual(self.sm.appends["c1:2"]["state"], "intent")

    def test_dump_and_load_round_trip(self):
        intent(self.sm, "c1:1")
        self.sm.apply({"op": "DONE", "req": "c1:1"}, 0)
        intent(self.sm, "c1:2")
        copy = MasterState()
        copy.load(self.sm.dump())
        self.assertEqual(copy.dump(), self.sm.dump())
        self.assertEqual(intent(copy, "c1:2")["status"], "intent")
        self.assertEqual(intent(copy, "c1:1")["status"], "committed")


if __name__ == "__main__":
    unittest.main()
