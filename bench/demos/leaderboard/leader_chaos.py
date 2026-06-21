#!/usr/bin/env python3
"""Leaderboard chaos workload for the trains-valkey proxy.

N producers continuously ZINCRBY random integer deltas to random player IDs in
a single sorted set. Each successful ZINCRBY is recorded with the player ID
and the cumulative score we expected after that operation. On verify-local
each survivor's `ZRANGE leaderboard 0 -1 WITHSCORES` must match the expected
final scores byte-for-byte.

Same two-phase split as the lock demo:
  --mode load          run the workload, emit acked-set JSON
  --mode verify-local  query one local engine, write a PartialReport
"""
from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import threading
import time
from contextlib import closing
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional


# ---------- minimal RESP client (same as lock_chaos.py) ---------------------


class Resp:
    READ_TIMEOUT = 15.0

    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.sock: Optional[socket.socket] = None
        self.buf = b""

    def _connect(self) -> None:
        s = socket.create_connection((self.host, self.port), timeout=self.READ_TIMEOUT)
        s.settimeout(self.READ_TIMEOUT)
        self.sock = s
        self.buf = b""

    def call(self, *args):
        if self.sock is None:
            self._connect()
        payload = self._encode(args)
        try:
            assert self.sock is not None
            self.sock.sendall(payload)
            return self._read_reply()
        except (BrokenPipeError, ConnectionResetError, socket.timeout, OSError):
            self.sock = None
            raise

    @staticmethod
    def _encode(args) -> bytes:
        out = [f"*{len(args)}\r\n".encode()]
        for a in args:
            if isinstance(a, int):
                a = str(a)
            if isinstance(a, str):
                a = a.encode()
            out.append(f"${len(a)}\r\n".encode())
            out.append(a)
            out.append(b"\r\n")
        return b"".join(out)

    def _readline(self) -> bytes:
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(4096) if self.sock else b""
            if not chunk:
                raise ConnectionResetError("peer closed")
            self.buf += chunk
        line, _, self.buf = self.buf.partition(b"\r\n")
        return line

    def _readn(self, n: int) -> bytes:
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(4096) if self.sock else b""
            if not chunk:
                raise ConnectionResetError("peer closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def _read_reply(self):
        line = self._readline()
        kind, rest = chr(line[0]), line[1:]
        if kind == "+":
            return rest.decode()
        if kind == "-":
            raise RuntimeError(rest.decode())
        if kind == ":":
            return int(rest)
        if kind == "$":
            n = int(rest)
            if n == -1:
                return None
            return self._readn(n).decode()
        if kind == "*":
            n = int(rest)
            return [self._read_reply() for _ in range(n)]
        raise RuntimeError(f"unexpected RESP byte: {kind!r}")


# ---------- workload --------------------------------------------------------


@dataclass
class ScoreEvent:
    t: float
    worker: int
    player: str
    delta: int
    new_score: int            # what the server returned (ZINCRBY returns new value)
    latency_ms: float


@dataclass
class AckedScoreSet:
    schema: int = 1
    workload: str = "leaderboard"
    started_at: float = 0.0
    finished_at: float = 0.0
    target_host: str = ""
    target_port: int = 0
    workers: int = 0
    players: list[str] = field(default_factory=list)
    leaderboard_key: str = "leaderboard"
    events: list[dict] = field(default_factory=list)
    expected_final_scores: dict[str, int] = field(default_factory=dict)


def worker(
    wid: int, host: str, port: int, players: list[str],
    stop: threading.Event, lock: threading.Lock, acked: AckedScoreSet,
):
    rng = random.Random(wid * 7901 + int(time.time()))
    cli = Resp(host, port)
    while not stop.is_set():
        player = rng.choice(players)
        delta = rng.randint(1, 100)
        t0 = time.monotonic()
        try:
            res = cli.call("ZINCRBY", acked.leaderboard_key, str(delta), player)
        except Exception:
            time.sleep(0.05)
            continue
        # ZINCRBY returns the new score as a bulk-string (RESP-2) or number
        new_score = int(float(res)) if isinstance(res, (str, int)) else int(res)
        dur = (time.monotonic() - t0) * 1000.0
        with lock:
            acked.events.append(asdict(ScoreEvent(time.monotonic(), wid, player, delta, new_score, dur)))


def cmd_load(args) -> int:
    players = [f"p{i:04d}" for i in range(args.players)]
    acked = AckedScoreSet(
        target_host=args.host, target_port=args.port,
        workers=args.workers, players=players,
        leaderboard_key=args.key,
        started_at=time.time(),
    )
    # ensure clean slate
    try:
        cli = Resp(args.host, args.port)
        cli.call("DEL", args.key)
    except Exception:
        pass
    lock = threading.Lock()
    stop = threading.Event()
    threads = [
        threading.Thread(target=worker, args=(i, args.host, args.port, players, stop, lock, acked), daemon=True)
        for i in range(args.workers)
    ]
    for t in threads:
        t.start()
    time.sleep(args.duration)
    stop.set()
    for t in threads:
        t.join(timeout=2.0)
    # compute expected per-player sum from the acked events
    expected = {}
    for ev in acked.events:
        expected[ev["player"]] = expected.get(ev["player"], 0) + ev["delta"]
    acked.expected_final_scores = expected
    acked.finished_at = time.time()
    Path(args.acked_out).write_text(json.dumps(asdict(acked), indent=2))
    print(f"[load] events={len(acked.events)}  players_touched={len(expected)}")
    return 0


def cmd_verify_local(args) -> int:
    acked = json.loads(Path(args.acked_in).read_text())
    cli = Resp(args.host, args.port)
    # ZRANGE all members WITHSCORES
    raw = cli.call("ZRANGE", acked["leaderboard_key"], "0", "-1", "WITHSCORES")
    observed = {}
    it = iter(raw or [])
    for m in it:
        s = next(it)
        observed[m] = int(float(s))
    expected = {k: int(v) for k, v in acked["expected_final_scores"].items()}
    diff = {}
    for k in set(observed) | set(expected):
        eo, ee = observed.get(k, 0), expected.get(k, 0)
        if eo != ee:
            diff[k] = {"observed": eo, "expected": ee, "delta": eo - ee}
    report = {
        "engine_host": args.host,
        "engine_port": args.port,
        "leaderboard_key": acked["leaderboard_key"],
        "members_observed": len(observed),
        "members_expected": len(expected),
        "all_match": len(diff) == 0,
        "divergent_members": diff,
    }
    Path(args.report_out).write_text(json.dumps(report, indent=2))
    print(json.dumps({k: v for k, v in report.items() if k != "divergent_members"}, indent=2))
    if diff:
        print(f"[verify-local] DIVERGENCE on {len(diff)} members (see report)")
    return 0 if report["all_match"] else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=["load", "verify-local"], required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--players", type=int, default=200)
    p.add_argument("--key", default="leaderboard")
    p.add_argument("--duration", type=float, default=20.0)
    p.add_argument("--acked-out")
    p.add_argument("--acked-in")
    p.add_argument("--report-out")
    args = p.parse_args()
    if args.mode == "load":
        if not args.acked_out:
            print("--acked-out required", file=sys.stderr)
            return 64
        return cmd_load(args)
    if args.mode == "verify-local":
        if not args.acked_in or not args.report_out:
            print("--acked-in and --report-out required", file=sys.stderr)
            return 64
        return cmd_verify_local(args)
    return 64


if __name__ == "__main__":
    sys.exit(main())
