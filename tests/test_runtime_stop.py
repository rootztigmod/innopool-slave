#!/usr/bin/env python3
"""Stop/kill leftovers: do not report idle while container runtimes remain."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
for _name in ("randomname", "requests", "blake3"):
    sys.modules.setdefault(_name, MagicMock())

import main as slave  # noqa: E402


class RuntimeStopTests(unittest.TestCase):
    def setUp(self):
        slave.PROCESSING_BATCH_IDS.clear()
        slave.PENDING_BATCH_IDS.clear()
        slave.READY_BATCH_IDS.clear()
        slave._STOPPED_BATCH_IDS.clear()
        slave._RUNTIME_PROCS.clear()
        slave._DRAINING.clear()
        slave._EMPTY_REVOKE_STREAK = 0
        slave._DOWNLOADING = 0
        slave._IDLE_SINCE_MS = None
        slave._LAST_IDLE_GAP_MS = 0

    def test_cmdline_match(self):
        cmd = "tig-runtime {\"n\":1} abcdef123 7 /app/algorithms/x.so --fuel 1"
        self.assertTrue(slave._cmdline_is_runtime(cmd))
        self.assertTrue(slave._cmdline_is_runtime(cmd, "abcdef123"))
        self.assertFalse(slave._cmdline_is_runtime(cmd, "otherhash"))
        self.assertTrue(slave._cmdline_is_runtime("tig-verifier abcdef123 7 /tmp/7.json", "abcdef123"))
        self.assertFalse(slave._cmdline_is_runtime("sleep infinity"))

    def test_cpu_slave_does_not_reap_gpu_containers(self):
        slave._SLAVE_NAME = "pool-cpu-test"
        self.assertNotIn("hypergraph", slave._managed_challenges())
        self.assertIn("vehicle_routing", slave._managed_challenges())
        slave._SLAVE_NAME = "pool-gpu-test"
        self.assertIn("hypergraph", slave._managed_challenges())
        self.assertNotIn("vehicle_routing", slave._managed_challenges())

    @patch.object(slave, "_signal_container_runtimes", return_value=["11"])
    def test_stop_batch_marks_draining_and_stays_running(self, _signal):
        slave.PROCESSING_BATCH_IDS["b1"] = {
            "batch": {
                "id": "b1",
                "challenge": "vehicle_routing",
                "rand_hash": "abc123",
                "algorithm": "hgs_advance",
                "settings": {},
                "num_nonces": 1,
            },
            "finished": set(),
            "start": 1,
        }
        slave._stop_batch("b1", "test")
        self.assertNotIn("b1", slave.PROCESSING_BATCH_IDS)
        self.assertIn("vehicle_routing", slave._DRAINING)
        self.assertEqual(slave._runtime_state(), "running")
        slave._mark_idle_if_quiet()
        self.assertIsNone(slave._IDLE_SINCE_MS)

    @patch.object(slave, "_signal_container_runtimes", return_value=[])
    def test_empty_assign_keeps_inflight(self, _signal):
        slave.PROCESSING_BATCH_IDS["b1"] = {
            "batch": {
                "id": "b1",
                "challenge": "satisfiability",
                "rand_hash": "fff",
                "algorithm": "sat_imp_v4",
                "settings": {},
                "num_nonces": 1,
            },
            "finished": set(),
            "start": 1,
        }
        slave._apply_master_assignment([], stale_only=False)
        slave._apply_master_assignment([], stale_only=False)
        slave._apply_master_assignment([], stale_only=False)
        self.assertIn("b1", slave.PROCESSING_BATCH_IDS)
        self.assertEqual(slave._DRAINING, {})

    @patch.object(slave, "_signal_container_runtimes", return_value=[])
    def test_stale_only_keeps_inflight(self, _signal):
        slave.PROCESSING_BATCH_IDS["b1"] = {
            "batch": {"id": "b1", "challenge": "job_scheduling", "rand_hash": "x", "settings": {}},
            "finished": set(),
            "start": 1,
        }
        slave._apply_master_assignment([], stale_only=True)
        self.assertIn("b1", slave.PROCESSING_BATCH_IDS)
        self.assertEqual(slave._DRAINING, {})

    @patch.object(slave, "_signal_container_runtimes", return_value=[])
    def test_omitted_batch_stops_immediately(self, _signal):
        slave.PROCESSING_BATCH_IDS["old"] = {
            "batch": {"id": "old", "challenge": "energy_arbitrage", "rand_hash": "x", "settings": {}},
            "finished": set(),
            "start": 1,
        }
        slave._apply_master_assignment(["new"], stale_only=False)
        self.assertNotIn("old", slave.PROCESSING_BATCH_IDS)
        self.assertIn("energy_arbitrage", slave._DRAINING)

    def test_stopped_batch_is_not_live(self):
        slave.PROCESSING_BATCH_IDS["b1"] = {"batch": {"id": "b1"}}
        self.assertTrue(slave._batch_is_live("b1"))
        slave._STOPPED_BATCH_IDS["b1"] = slave.now()
        self.assertFalse(slave._batch_is_live("b1"))
        slave.PROCESSING_BATCH_IDS.pop("b1", None)
        self.assertFalse(slave._batch_is_live("b1"))

    @patch.object(slave, "_challenge_has_runtimes", return_value=False)
    @patch.object(slave, "_signal_container_runtimes", return_value=[])
    def test_reap_clears_drain_then_idle(self, _signal, _has):
        slave._DRAINING["vehicle_routing"] = slave.now()
        self.assertFalse(slave._reap_draining())
        self.assertEqual(slave._DRAINING, {})
        slave._mark_idle_if_quiet()
        self.assertIsNotNone(slave._IDLE_SINCE_MS)
        self.assertEqual(slave._runtime_state(), "idle")


if __name__ == "__main__":
    unittest.main()
