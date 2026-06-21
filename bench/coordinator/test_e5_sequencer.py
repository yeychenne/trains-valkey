#!/usr/bin/env python3
"""Tests for the E5 fault sequencer (PR-RED-5a).

Stdlib `unittest` only — no pytest, no boto3, no AWS. Run with::

    python3 bench/coordinator/test_e5_sequencer.py
    # or: python3 -m unittest discover -s bench/coordinator -p 'test_*.py'

Exercises: schedule parsing + validation, fault → command expansion, the
inject/heal timeline ordering, the injected-clock fast-forward, and that every
shipped schedule under schedules/ parses and references known fault kinds.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import e5_sequencer as e5  # noqa: E402

THREE = e5.Ctx(instances=["i-0", "i-1", "i-2"],
               ips=["10.0.0.0", "10.0.0.1", "10.0.0.2"],
               iface="ens5")


def _sched(events, ring_size=3):
    return e5.Schedule(name="t", description="", ring_size=ring_size,
                       iface="ens5", events=[e5.Event.from_dict(e) for e in events])


class FaultExpansion(unittest.TestCase):
    def test_kill_is_permanent_no_heal(self):
        s = _sched([{"kind": "kill-proxy", "at": 5, "target": 2}])
        actions = e5.plan(s, THREE)
        self.assertEqual(len(actions), 1)  # inject only, no heal
        self.assertEqual(actions[0].node, 2)
        self.assertTrue(any("kill -9" in c for c in actions[0].commands))

    def test_partition_injects_and_heals(self):
        s = _sched([{"kind": "partition", "at": 10, "duration": 20,
                     "from": 1, "to": 2}])
        actions = e5.plan(s, THREE)
        self.assertEqual([a.at for a in actions], [10, 30])  # heal at at+duration
        self.assertTrue(any("-A OUTPUT -d 10.0.0.2 -j DROP" in c
                            for c in actions[0].commands))
        self.assertTrue(any("-D OUTPUT -d 10.0.0.2 -j DROP" in c
                            for c in actions[1].commands))

    def test_timeline_is_sorted_across_events(self):
        s = _sched([
            {"kind": "kill-proxy", "at": 45, "target": 2},
            {"kind": "kill-proxy", "at": 15, "target": 1},
        ], ring_size=5)
        actions = e5.plan(s, e5.Ctx(["i"] * 5, ["10.0.0.%d" % i for i in range(5)],
                                    "ens5"))
        self.assertEqual([a.at for a in actions], [15, 45])
        self.assertEqual([a.node for a in actions], [1, 2])

    def test_netem_loss_clears(self):
        s = _sched([{"kind": "netem-loss", "at": 0, "duration": 5,
                     "target": 0, "pct": 6}])
        inject, heal = e5.plan(s, THREE)
        self.assertTrue(any("netem loss 6%" in c for c in inject.commands))
        self.assertTrue(any("qdisc del" in c for c in heal.commands))

    def test_restart_proxy_uses_env_baked_relaunch_script(self):
        # A bare launch-node.sh has no NODE_ID/PEER_ADDRS env; restart must run
        # the per-node relaunch.sh the orchestrator stages.
        s = _sched([{"kind": "restart-proxy", "at": 5, "target": 2}])
        (inject,) = e5.plan(s, THREE)  # permanent action, no heal
        self.assertTrue(any("/opt/trains/relaunch.sh" in c for c in inject.commands))
        self.assertFalse(any("bash /opt/trains/launch-node.sh" in c for c in inject.commands))


class Execution(unittest.TestCase):
    def test_execute_dispatches_in_order_with_fastforward(self):
        s = _sched([{"kind": "partition", "at": 10, "duration": 20,
                     "from": 1, "to": 2}])
        disp = e5.MockDispatcher()
        slept: list[float] = []
        e5.execute(e5.plan(s, THREE), THREE, disp, sleep=slept.append,
                   log=lambda _m: None)
        # Two dispatches: inject on node 1's instance, heal on node 1's instance.
        self.assertEqual([c[0] for c in disp.calls], ["i-1", "i-1"])
        # Slept 10s to the inject, then 20s to the heal.
        self.assertEqual(slept, [10, 20])

    def test_dispatch_targets_the_acting_node_instance(self):
        s = _sched([{"kind": "kill-proxy", "at": 1, "target": 2}])
        disp = e5.MockDispatcher()
        e5.execute(e5.plan(s, THREE), THREE, disp, sleep=lambda _s: None,
                   log=lambda _m: None)
        self.assertEqual(disp.calls[0][0], "i-2")


class Validation(unittest.TestCase):
    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):
            _sched([{"kind": "nuke", "at": 0, "target": 0}]).validate()

    def test_node_out_of_range_rejected(self):
        with self.assertRaises(ValueError):
            _sched([{"kind": "kill-proxy", "at": 0, "target": 9}]).validate()

    def test_partition_needs_from_and_to(self):
        with self.assertRaises(ValueError):
            _sched([{"kind": "partition", "at": 0, "duration": 5,
                     "target": 1}]).validate()

    def test_empty_schedule_rejected(self):
        with self.assertRaises(ValueError):
            _sched([]).validate()


class ShippedSchedules(unittest.TestCase):
    def test_all_schedule_files_parse_and_validate(self):
        sched_dir = Path(__file__).parent / "schedules"
        files = sorted(sched_dir.glob("*.json"))
        self.assertGreaterEqual(len(files), 6, "expected the 6 Tier-1/2 schedules")
        for f in files:
            with self.subTest(schedule=f.name):
                s = e5.Schedule.load(f)  # raises on any validation error
                self.assertTrue(s.events)
                # Every event references a known fault and an in-range node.
                ctx = e5.Ctx([f"i-{i}" for i in range(s.ring_size)],
                             [f"10.0.0.{i}" for i in range(s.ring_size)], s.iface)
                e5.plan(s, ctx)  # expansion must not raise

    def test_multi_victim_needs_five_nodes(self):
        s = e5.Schedule.load(Path(__file__).parent / "schedules"
                             / "t1-multi-victim.json")
        self.assertEqual(s.ring_size, 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
