#!/usr/bin/env python3
"""E5 — Jepsen-style adversarial fault sequencer (PR-RED-5a).

Drives a *timed schedule* of faults against a running trains-valkey ring on EC2,
dispatching each fault (and its later heal) as an SSM `AWS-RunShellScript`
command at its scheduled offset. The workload itself (`trains-valkey-chaos
--mode load`) runs separately; this sequencer only injects the chaos, on a
clock, so a scenario like "partition nodes 1↔2 at T+10s for 20s, then SIGKILL
node 3 at T+30s" is reproducible.

Design notes
------------
* **No `faults.py`.** The original E5 plan assumed a `bench/coordinator/faults.py`
  with netem/iptables primitives; it never existed (fault injection lived inline
  in `scripts/bench-aws/coordinator.py`). The fault → shell-command generators
  therefore live here, in `FAULTS`.
* **Dependency-free + testable.** Stdlib only; `boto3` is imported lazily inside
  `SsmDispatcher` so the module imports (and its tests run) with no AWS SDK and
  no credentials. The dispatcher and the clock are injectable, so the whole
  schedule can be exercised in-process with a mock — see `test_e5_sequencer.py`.
* **Schedules are JSON** (not YAML) to avoid a PyYAML dependency in CI.

A schedule file is::

    {
      "name": "t1-partition",
      "description": "...",
      "ring_size": 3,
      "iface": "ens5",
      "events": [
        {"kind": "partition", "at": 10, "duration": 20, "from": 1, "to": 2}
      ]
    }

Run (real)::

    python3 e5_sequencer.py --schedule schedules/t1-partition.json \
        --instances i-aaa,i-bbb,i-ccc --ips 10.0.0.1,10.0.0.2,10.0.0.3

Run (dry — print the timeline, no SSM, no sleeps)::

    python3 e5_sequencer.py --schedule schedules/t1-partition.json --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Protocol

# Proxy pid file written by scripts/redis-chaos/launch-node.sh.
PROXY_PID = "/tmp/trains-valkey.pid"
DEFAULT_IFACE = "ens5"  # AL2023 EC2 primary ENI


# ── Fault → shell-command generators ──────────────────────────────────────────
#
# Each generator returns (inject_cmds, heal_cmds). `heal_cmds` is empty for a
# permanent fault (a kill). All commands are idempotent-ish and `|| true` where
# a missing rule/qdisc would otherwise fail the SSM invocation.

def _f_kill_proxy(ev: "Event", ctx: "Ctx") -> tuple[list[str], list[str]]:
    """Permanently SIGKILL the proxy on `target` (a clean crash to be masked)."""
    return (
        [f"kill -9 $(cat {PROXY_PID} 2>/dev/null) 2>/dev/null || true",
         f"echo killed proxy on node {ev.target}"],
        [],
    )


def _f_partition(ev: "Event", ctx: "Ctx") -> tuple[list[str], list[str]]:
    """One-way packet drop from node `from` to node `to` (ring port + RESP)."""
    dst = ctx.ip(ev.to)
    add = f"sudo iptables -A OUTPUT -d {dst} -j DROP"
    rm = f"sudo iptables -D OUTPUT -d {dst} -j DROP || true"
    return ([add, f"echo partitioned {ev.frm}->{ev.to} ({dst})"],
            [rm, f"echo healed partition {ev.frm}->{ev.to}"])


def _f_netem_loss(ev: "Event", ctx: "Ctx") -> tuple[list[str], list[str]]:
    """Add `pct`% random egress loss on `target`'s primary interface."""
    iface = ctx.iface
    add = f"sudo tc qdisc add dev {iface} root netem loss {ev.pct}%"
    rm = f"sudo tc qdisc del dev {iface} root netem || true"
    return ([add, f"echo netem loss {ev.pct}% on node {ev.target}"],
            [rm, f"echo cleared netem on node {ev.target}"])


def _f_netem_latency(ev: "Event", ctx: "Ctx") -> tuple[list[str], list[str]]:
    """Add `ms` ms egress latency on `target`'s primary interface."""
    iface = ctx.iface
    add = f"sudo tc qdisc add dev {iface} root netem delay {ev.ms}ms"
    rm = f"sudo tc qdisc del dev {iface} root netem || true"
    return ([add, f"echo netem delay {ev.ms}ms on node {ev.target}"],
            [rm, f"echo cleared netem on node {ev.target}"])


def _f_clock_skew(ev: "Event", ctx: "Ctx") -> tuple[list[str], list[str]]:
    """Step `target`'s wall clock by `secs` (can be negative), then restore."""
    add = (f"sudo date -s \"$(date -d '{ev.secs:+d} seconds' '+%Y-%m-%d %H:%M:%S')\" "
           f">/dev/null && echo skewed clock {ev.secs:+d}s on node {ev.target}")
    rm = ("sudo chronyc makestep 2>/dev/null || sudo ntpdate -u pool.ntp.org "
          "2>/dev/null || true")
    return ([add], [rm, f"echo restored clock on node {ev.target}"])


def _f_restart_proxy(ev: "Event", ctx: "Ctx") -> tuple[list[str], list[str]]:
    """Restart a previously-killed proxy so it rejoins via state transfer.

    Runs `/opt/trains/relaunch.sh` — a per-node script (with this node's
    NODE_ID / PEER_ADDRS / … env baked in) that the orchestrator stages during
    ring setup. A bare `launch-node.sh` has no env and would fail.
    """
    return (
        ["sudo bash /opt/trains/relaunch.sh </dev/null >/opt/trains/relaunch.out 2>&1 || "
         "echo 'relaunch failed — stage /opt/trains/relaunch.sh with the node env'",
         f"echo restarted proxy on node {ev.target}"],
        [],
    )


FAULTS: dict[str, Callable[["Event", "Ctx"], tuple[list[str], list[str]]]] = {
    "kill-proxy": _f_kill_proxy,
    "partition": _f_partition,
    "netem-loss": _f_netem_loss,
    "netem-latency": _f_netem_latency,
    "clock-skew": _f_clock_skew,
    "restart-proxy": _f_restart_proxy,
}

# Faults that act on a target node vs. those that act on a directed pair.
_PAIR_FAULTS = {"partition"}
_TARGET_FAULTS = set(FAULTS) - _PAIR_FAULTS


# ── Schedule model ────────────────────────────────────────────────────────────

@dataclass
class Event:
    kind: str
    at: float                 # offset seconds from sequence start
    duration: float = 0.0     # 0 ⇒ permanent (no heal)
    target: int | None = None
    frm: int | None = None    # 'from' node (pair faults)
    to: int | None = None
    pct: float | None = None
    ms: float | None = None
    secs: int | None = None

    @staticmethod
    def from_dict(d: dict) -> "Event":
        return Event(
            kind=d["kind"],
            at=float(d["at"]),
            duration=float(d.get("duration", 0)),
            target=d.get("target"),
            frm=d.get("from"),
            to=d.get("to"),
            pct=d.get("pct"),
            ms=d.get("ms"),
            secs=d.get("secs"),
        )


@dataclass
class Schedule:
    name: str
    description: str
    ring_size: int
    iface: str
    events: list[Event] = field(default_factory=list)

    @staticmethod
    def load(path: Path) -> "Schedule":
        d = json.loads(Path(path).read_text())
        sched = Schedule(
            name=d["name"],
            description=d.get("description", ""),
            ring_size=int(d["ring_size"]),
            iface=d.get("iface", DEFAULT_IFACE),
            events=[Event.from_dict(e) for e in d["events"]],
        )
        sched.validate()
        return sched

    def validate(self) -> None:
        """Reject malformed schedules early (used by tests + CLI)."""
        if not self.events:
            raise ValueError(f"schedule {self.name!r} has no events")
        for i, ev in enumerate(self.events):
            if ev.kind not in FAULTS:
                raise ValueError(
                    f"{self.name}: event {i} unknown fault kind {ev.kind!r}; "
                    f"known: {sorted(FAULTS)}")
            if ev.at < 0 or ev.duration < 0:
                raise ValueError(f"{self.name}: event {i} negative at/duration")
            if ev.kind in _PAIR_FAULTS:
                if ev.frm is None or ev.to is None:
                    raise ValueError(
                        f"{self.name}: event {i} ({ev.kind}) needs 'from' and 'to'")
                self._check_node(ev.frm, i)
                self._check_node(ev.to, i)
            else:
                if ev.target is None:
                    raise ValueError(
                        f"{self.name}: event {i} ({ev.kind}) needs 'target'")
                self._check_node(ev.target, i)

    def _check_node(self, node: int, i: int) -> None:
        if not (0 <= node < self.ring_size):
            raise ValueError(
                f"{self.name}: event {i} node {node} out of range "
                f"0..{self.ring_size - 1}")


# ── Dispatch + execution ──────────────────────────────────────────────────────

@dataclass
class Ctx:
    """Per-run context: node-index → instance-id / private-ip + the NIC name."""
    instances: list[str]
    ips: list[str]
    iface: str

    def instance(self, node: int) -> str:
        return self.instances[node]

    def ip(self, node: int) -> str:
        return self.ips[node]


class Dispatcher(Protocol):
    def run(self, instance_id: str, commands: list[str]) -> None: ...


@dataclass
class MockDispatcher:
    """Records (instance_id, commands) instead of calling SSM. For tests/dry-run."""
    calls: list[tuple[str, list[str]]] = field(default_factory=list)

    def run(self, instance_id: str, commands: list[str]) -> None:
        self.calls.append((instance_id, commands))


class SsmDispatcher:
    """Real dispatcher: fire-and-forget SSM RunShellScript (boto3, lazy import)."""

    def __init__(self, profile: str, region: str):
        import boto3  # lazy: tests + dry-run never import the SDK
        self._ssm = boto3.Session(
            profile_name=profile, region_name=region).client("ssm")

    def run(self, instance_id: str, commands: list[str]) -> None:
        self._ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": commands, "executionTimeout": ["120"]},
        )


# A planned action on the absolute timeline: (offset_s, node, commands, label).
@dataclass(order=True)
class Action:
    at: float
    node: int = field(compare=False)
    commands: list[str] = field(compare=False, default_factory=list)
    label: str = field(compare=False, default="")


def plan(sched: Schedule, ctx: Ctx) -> list[Action]:
    """Expand a schedule into a flat, time-sorted list of inject + heal actions."""
    actions: list[Action] = []
    for ev in sched.events:
        gen = FAULTS[ev.kind]
        inject, heal = gen(ev, ctx)
        node = ev.target if ev.target is not None else ev.frm
        actions.append(Action(ev.at, node, inject, f"{ev.kind} inject"))
        if heal and ev.duration > 0:
            actions.append(
                Action(ev.at + ev.duration, node, heal, f"{ev.kind} heal"))
    actions.sort()
    return actions


def execute(
    actions: list[Action],
    ctx: Ctx,
    dispatcher: Dispatcher,
    sleep: Callable[[float], None],
    log: Callable[[str], None] = print,
) -> None:
    """Walk the timeline, sleeping between actions and dispatching each.

    `sleep` is injected so tests fast-forward with a no-op (and assert ordering)
    while the real run uses `time.sleep`.
    """
    elapsed = 0.0
    for act in actions:
        wait = act.at - elapsed
        if wait > 0:
            sleep(wait)
            elapsed = act.at
        log(f"[T+{act.at:6.1f}s] node {act.node}: {act.label}")
        dispatcher.run(ctx.instance(act.node), act.commands)


def print_timeline(actions: list[Action], log: Callable[[str], None] = print) -> None:
    log("planned timeline (dry-run — no SSM dispatched):")
    for act in actions:
        log(f"  T+{act.at:6.1f}s  node {act.node}  {act.label}")
        for c in act.commands:
            log(f"             $ {c}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_ctx(args, sched: Schedule) -> Ctx:
    if args.instances and args.ips:
        instances = args.instances.split(",")
        ips = args.ips.split(",")
    else:
        # Fall back to CDK outputs (real runs); kept out of import path so tests
        # never touch the filesystem/AWS.
        from coordinator import load_cdk_outputs  # type: ignore
        out = load_cdk_outputs()
        instances = out["InstanceIds"].split(",")
        ips = out["PrivateIps"].split(",")
    if len(instances) < sched.ring_size or len(ips) < sched.ring_size:
        raise SystemExit(
            f"schedule needs {sched.ring_size} nodes but got "
            f"{len(instances)} instances / {len(ips)} ips")
    return Ctx(instances=instances, ips=ips, iface=sched.iface)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="E5 adversarial fault sequencer")
    ap.add_argument("--schedule", required=True, type=Path)
    ap.add_argument("--instances", help="comma-separated SSM instance ids (node order)")
    ap.add_argument("--ips", help="comma-separated private ips (node order)")
    ap.add_argument("--profile", default="default",
                    help="AWS profile (default: 'default' — workshop-profile is retired)")
    ap.add_argument("--region", default="eu-west-3")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the timeline and exit; no SSM, no sleeps")
    args = ap.parse_args(argv)

    sched = Schedule.load(args.schedule)

    if args.dry_run:
        # Dry-run needs no real instances; synthesize placeholders if absent.
        instances = (args.instances.split(",") if args.instances
                     else [f"i-node{i}" for i in range(sched.ring_size)])
        ips = (args.ips.split(",") if args.ips
               else [f"10.0.0.{i}" for i in range(sched.ring_size)])
        ctx = Ctx(instances=instances, ips=ips, iface=sched.iface)
        print(f"schedule: {sched.name} — {sched.description}")
        print_timeline(plan(sched, ctx))
        return 0

    ctx = _build_ctx(args, sched)
    dispatcher = SsmDispatcher(args.profile, args.region)
    import time
    print(f"running schedule {sched.name} against {sched.ring_size}-node ring")
    execute(plan(sched, ctx), ctx, dispatcher, time.sleep)
    print("schedule complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
