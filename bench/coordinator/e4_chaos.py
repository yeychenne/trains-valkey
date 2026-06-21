#!/usr/bin/env python3
"""E4 chaos client — pipelined, rate-capped, latency-capturing.

Works against either stack:
  - sentinel://HOST:26379  → asks Sentinel for current primary, retries on
                              failover. Same flow as sentinel-chaos.py.
  - redis://HOST:PORT       → direct RESP connection, retries same host
                              on connection error (single-primary mode).

Workload:
  - SET chaos:k0..chaos:k{COUNT-1} = v0..v{COUNT-1}
  - Pipeline PIPELINE writes per batch (default 1 = serial).
  - Sustain TARGET_RATE writes/sec by sleeping between batches if needed
    (0 = max throughput).
  - Capture per-batch latency (batch_ack_time - batch_send_time);
    individual writes within a batch share the batch timing.

Records:
  - ACKED_OUT  : JSON list of [key, val, time_acked, primary_addr]
  - LATENCY_OUT: JSON object { batch_latency_ms: [...], throughput_wr_per_s: N }
"""
import json, socket, sys, time
import argparse


def resp_encode(args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a if isinstance(a, bytes) else a.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def _readline(sock):
    buf = b""
    while not buf.endswith(b"\r\n"):
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("peer closed")
        buf += ch
    return buf[:-2]


def read_reply(sock):
    line = _readline(sock).decode()
    if line.startswith("+"):
        return line[1:]
    if line.startswith("-"):
        raise RuntimeError(f"Redis error: {line[1:]}")
    if line.startswith(":"):
        return int(line[1:])
    if line.startswith("$"):
        n = int(line[1:])
        if n == -1:
            return None
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("peer closed")
            data += chunk
        sock.recv(2)
        return data
    if line.startswith("*"):
        n = int(line[1:])
        return [read_reply(sock) for _ in range(n)]
    raise RuntimeError(f"Unexpected reply prefix: {line!r}")


def ask_sentinel_for_master(sentinel_addr):
    host, port = sentinel_addr.split(":")
    s = socket.create_connection((host, int(port)), timeout=3)
    try:
        s.sendall(resp_encode([b"SENTINEL", b"get-master-addr-by-name", b"mymaster"]))
        reply = read_reply(s)
        if reply is None:
            raise RuntimeError("Sentinel returned nil for master")
        return reply[0].decode(), int(reply[1].decode())
    finally:
        s.close()


def connect(host, port, password):
    s = socket.create_connection((host, port), timeout=3)
    if password:
        s.sendall(resp_encode([b"AUTH", password.encode()]))
        r = read_reply(s)
        if r != "OK":
            raise RuntimeError(f"AUTH failed: {r}")
    # Disable Nagle for low-latency pipelining
    try:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass
    return s


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True,
                   help="sentinel://HOST:26379 or redis://HOST:PORT")
    p.add_argument("--password", default="", help="Redis AUTH password")
    p.add_argument("--count", type=int, default=2000)
    p.add_argument("--acked-out", required=True)
    p.add_argument("--latency-out", required=True)
    p.add_argument("--pipeline", type=int, default=1,
                   help="Writes per batch (1=serial, >1=pipelined)")
    p.add_argument("--target-rate", type=float, default=0,
                   help="Target writes/sec (0=max). Sleeps between batches if needed.")
    p.add_argument("--prefix", default="chaos:k")
    p.add_argument("--abandon-secs", type=float, default=60.0)
    args = p.parse_args()

    scheme, _, rest = args.target.partition("://")
    if scheme not in ("sentinel", "redis"):
        raise SystemExit(f"unknown target scheme: {scheme}")
    discover = scheme == "sentinel"

    acked = []  # [[k, v, t_ack, primary]]
    batch_latencies_ms = []
    i = 0
    start = time.time()
    last_print = start
    print(f"[e4-chaos] target={args.target} count={args.count} pipeline={args.pipeline} target_rate={args.target_rate}", flush=True)

    backoff = 0.2
    failover_window_start = None
    sock = None
    primary_label = None

    def connect_primary():
        nonlocal sock, primary_label
        if discover:
            host, port = ask_sentinel_for_master(rest)
        else:
            host, port = rest.split(":")
            port = int(port)
        sock = connect(host, port, args.password)
        primary_label = f"{host}:{port}"

    # Compute batch budget: if target_rate>0, each batch of PIPELINE writes
    # should occupy 1/target_rate * PIPELINE seconds (so writes/sec is steady).
    if args.target_rate > 0:
        per_batch_budget = args.pipeline / args.target_rate
    else:
        per_batch_budget = 0.0

    while i < args.count:
        try:
            if sock is None:
                connect_primary()
                print(f"[e4-chaos] primary={primary_label} at i={i} t={time.time()-start:.2f}s", flush=True)
                failover_window_start = None
        except Exception as e:
            elapsed = time.time() - start
            print(f"[e4-chaos] connect failed t={elapsed:.2f}s i={i}: {e}", flush=True)
            if failover_window_start is None:
                failover_window_start = time.time()
            if time.time() - failover_window_start > args.abandon_secs:
                print("[e4-chaos] abandoned", flush=True)
                break
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 2.0)
            continue
        backoff = 0.2

        # Pipeline a batch
        try:
            batch_start = time.time()
            batch_n = min(args.pipeline, args.count - i)
            payload = b""
            keys_vals = []
            for j in range(batch_n):
                k = f"{args.prefix}{i+j}".encode()
                v = f"v{i+j}".encode()
                payload += resp_encode([b"SET", k, v])
                keys_vals.append((k, v))
            sock.sendall(payload)
            for j in range(batch_n):
                r = read_reply(sock)
                t_ack = time.time()
                k, v = keys_vals[j]
                if r == "OK":
                    acked.append([k.decode(), v.decode(), t_ack, primary_label])
            batch_end = time.time()
            batch_latencies_ms.append((batch_end - batch_start) * 1000.0)
            i += batch_n

            # Periodic progress
            if batch_end - last_print >= 5.0:
                rate = i / (batch_end - start) if (batch_end - start) > 0 else 0
                print(f"[e4-chaos] progress: i={i} acked={len(acked)} t={batch_end-start:.2f}s rate={rate:.1f}wr/s primary={primary_label}", flush=True)
                last_print = batch_end

            # Rate cap
            if per_batch_budget > 0:
                slack = per_batch_budget - (batch_end - batch_start)
                if slack > 0:
                    time.sleep(slack)

        except (socket.error, ConnectionError, RuntimeError, socket.timeout) as e:
            elapsed = time.time() - start
            print(f"[e4-chaos] write/read err t={elapsed:.2f}s i={i} primary={primary_label}: {e}", flush=True)
            try:
                sock.close()
            except Exception:
                pass
            sock = None
            primary_label = None
            time.sleep(0.5)

    elapsed = time.time() - start
    throughput = len(acked) / elapsed if elapsed > 0 else 0
    print(f"[e4-chaos] DONE in {elapsed:.2f}s; acked {len(acked)} of {args.count}; rate {throughput:.1f}wr/s", flush=True)

    with open(args.acked_out, "w") as f:
        json.dump(acked, f)
    print(f"[e4-chaos] wrote {args.acked_out}", flush=True)

    # Percentiles
    batch_latencies_ms.sort()
    n = len(batch_latencies_ms)
    def pct(p):
        if n == 0:
            return 0.0
        idx = min(n - 1, int(n * p / 100))
        return round(batch_latencies_ms[idx], 3)
    latency_report = {
        "target": args.target,
        "count": args.count,
        "acked": len(acked),
        "pipeline": args.pipeline,
        "target_rate": args.target_rate,
        "elapsed_s": round(elapsed, 3),
        "throughput_wr_per_s": round(throughput, 2),
        "batch_latency_ms": {
            "n_batches": n,
            "p50": pct(50),
            "p95": pct(95),
            "p99": pct(99),
            "p999": pct(99.9),
            "max": round(batch_latencies_ms[-1], 3) if n else 0,
        },
    }
    with open(args.latency_out, "w") as f:
        json.dump(latency_report, f, indent=2)
    print(f"[e4-chaos] wrote {args.latency_out}", flush=True)
    print(json.dumps(latency_report, indent=2), flush=True)


if __name__ == "__main__":
    main()
