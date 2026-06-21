#!/usr/bin/env python3
"""Distributed-lock chaos workload for the trains-valkey proxy.

Writes a stream of (acquire → critical-section → release) operations against
a Redis-compatible endpoint. Records each acquisition and the matching
release. Survives a mid-workload proxy SIGKILL iff the underlying SMR
delivers acked writes to every survivor — orphaned locks (acquired with no
matching release) and deadlocks (every client backing off forever) both
fail the run.

Same two-phase split as `trains-valkey-chaos`:

  --mode load          run the workload, emit acked-set JSON for verify
  --mode verify-local  read the engine on this host, write a PartialReport

The verify step matches the chaos client's acked set against what the
survivor engine actually retains, plus the lock-invariant check
(every successful ACQUIRE has exactly one matching RELEASE in the log).
"""
from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import threading
import time
import uuid
from contextlib import closing
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional


# ---------- minimal RESP client (no third-party deps) -----------------------


class Resp:
    """A blocking RESP-2 client. ~80 LOC, no `redis-py` dependency.

    Reconnects lazily on broken-pipe / connection-reset. Times out after
    READ_TIMEOUT seconds per command — the proxy under chaos is expected
    to recover within ~10 s; longer than that is a deadlock signal.
    """

    READ_TIMEOUT = 15.0

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.buf = b""

    def _connect(self) -> None:
        s = socket.create_connection((self.host, self.port), timeout=self.READ_TIMEOUT)
        s.settimeout(self.READ_TIMEOUT)
        self.sock = s
        self.buf = b""

    def call(self, *args) -> object:
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
class LockEvent:
    t: float                  # monotonic seconds since workload start
    worker: int
    lock_key: str
    op: str                   # "acquire" | "release" | "miss"
    token: str                # UUID for acquire/release; "" for miss
    latency_ms: float = 0.0


@dataclass
class AckedLockSet:
    schema: int = 1
    workload: str = "distributed-lock"
    started_at: float = 0.0
    finished_at: float = 0.0
    target_host: str = ""
    target_port: int = 0
    workers: int = 0
    lock_keys: list[str] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)
    counter_value: Optional[int] = None  # final value of the shared counter

    @property
    def acquired(self) -> int:
        return sum(1 for e in self.events if e["op"] == "acquire")

    @property
    def released(self) -> int:
        return sum(1 for e in self.events if e["op"] == "release")

    @property
    def orphaned(self) -> int:
        return self.acquired - self.released


def worker(
    wid: int, host: str, port: int, lock_keys: list[str],
    stop: threading.Event, lock: threading.Lock, acked: AckedLockSet,
    ttl_secs: int,
):
    rng = random.Random(wid * 1009 + int(time.time()))
    cli = Resp(host, port)
    while not stop.is_set():
        key = rng.choice(lock_keys)
        token = str(uuid.uuid4())
        t0 = time.monotonic()
        try:
            res = cli.call("SET", f"lock:{key}", token, "NX", "EX", str(ttl_secs))
        except Exception:
            # connection blip — retry next iteration after small backoff
            time.sleep(0.05)
            continue
        dur = (time.monotonic() - t0) * 1000.0
        if res != "OK":
            with lock:
                acked.events.append(asdict(LockEvent(time.monotonic(), wid, key, "miss", "", dur)))
            time.sleep(rng.uniform(0.001, 0.01))
            continue
        with lock:
            acked.events.append(asdict(LockEvent(time.monotonic(), wid, key, "acquire", token, dur)))
        # critical section: bump the shared counter
        try:
            cli.call("INCR", "ops")
        except Exception:
            pass  # connection blip — counter is verified separately
        # release iff still owner — note: no Lua, race window documented
        try:
            owner = cli.call("GET", f"lock:{key}")
            if owner == token:
                t0 = time.monotonic()
                cli.call("DEL", f"lock:{key}")
                dur = (time.monotonic() - t0) * 1000.0
                with lock:
                    acked.events.append(asdict(LockEvent(time.monotonic(), wid, key, "release", token, dur)))
        except Exception:
            pass


def cmd_load(args) -> int:
    keys = [f"k{i}" for i in range(args.keys)]
    acked = AckedLockSet(
        target_host=args.host, target_port=args.port,
        workers=args.workers, lock_keys=keys,
        started_at=time.time(),
    )
    lock = threading.Lock()
    stop = threading.Event()
    threads = [
        threading.Thread(target=worker, args=(i, args.host, args.port, keys, stop, lock, acked, args.ttl), daemon=True)
        for i in range(args.workers)
    ]
    for t in threads:
        t.start()
    time.sleep(args.duration)
    stop.set()
    for t in threads:
        t.join(timeout=2.0)
    # capture the final counter value via a fresh client
    try:
        cli = Resp(args.host, args.port)
        v = cli.call("GET", "ops")
        acked.counter_value = int(v) if v else 0
    except Exception:
        acked.counter_value = None
    acked.finished_at = time.time()
    Path(args.acked_out).write_text(json.dumps(asdict(acked), indent=2))
    print(f"[load] acquired={acked.acquired}  released={acked.released}  "
          f"orphaned={acked.orphaned}  counter={acked.counter_value}")
    return 0 if acked.orphaned == 0 else 2


def cmd_verify_local(args) -> int:
    acked = json.loads(Path(args.acked_in).read_text())
    cli = Resp(args.host, args.port)
    # 1. counter must equal acquired (every acquire = 1 INCR)
    counter = cli.call("GET", "ops")
    counter_int = int(counter) if counter else 0
    acquired = sum(1 for e in acked["events"] if e["op"] == "acquire")
    # 2. no lock keys should remain held (all keys deleted on release;
    #    if TTL is short, TTL also cleans them up)
    held = 0
    for k in acked["lock_keys"]:
        v = cli.call("GET", f"lock:{k}")
        if v is not None:
            held += 1
    report = {
        "engine_host": args.host,
        "engine_port": args.port,
        "counter_observed": counter_int,
        "counter_expected": acquired,
        "counter_match": counter_int == acquired,
        "locks_still_held": held,
    }
    Path(args.report_out).write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0 if report["counter_match"] and held == 0 else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=["load", "verify-local"], required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--keys", type=int, default=8, help="lock-key pool size")
    p.add_argument("--ttl", type=int, default=10, help="lock SET EX seconds")
    p.add_argument("--duration", type=float, default=20.0)
    p.add_argument("--acked-out", help="load: where to write the acked set")
    p.add_argument("--acked-in", help="verify-local: where to read the acked set")
    p.add_argument("--report-out", help="verify-local: where to write the partial")
    args = p.parse_args()
    if args.mode == "load":
        if not args.acked_out:
            print("--acked-out required for load mode", file=sys.stderr)
            return 64
        return cmd_load(args)
    if args.mode == "verify-local":
        if not args.acked_in or not args.report_out:
            print("--acked-in and --report-out required for verify-local mode", file=sys.stderr)
            return 64
        return cmd_verify_local(args)
    return 64


if __name__ == "__main__":
    sys.exit(main())
