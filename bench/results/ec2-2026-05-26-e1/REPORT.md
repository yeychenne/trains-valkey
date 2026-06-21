# E1 — Live-fire chaos-2 EC2 retest (2026-05-26 afternoon)

**Topology.** Same as the morning REPORT (`bench/results/ec2-2026-05-26/REPORT.md`):
3-node `trains-valkey` ring on EC2 `t4g.small`, arm64 AL2023, self-managed Valkey
9.0.3 loopback per node, eu-west-3 (Paris). Instance ids:
`<instance-id>` (node 0, 10.0.0.162), `<instance-id>`
(node 1, 10.0.2.121), `<instance-id>` (node 2, 10.0.0.134).

**Binaries built from the integration branch `integration/e1-chaos-2-retest`**, which
merges PR-RD-5a (#20), PR-RD-5b (#21), PR-RD-6 (#23), and PR-RD-8 (#27).
PR-RD-6 (`apply_ring_socket_opts` calling `setsockopt(TCP_USER_TIMEOUT, 3 s)`)
and PR-RD-8 (`abort()` propagates to accepted-connection tasks) are the fixes
under test.

**Total spend:** < $0.05 compute + trivial S3/SSM. Teardown verified clean (3
instances `terminated`; zero `TrainsBench*` stacks remain; SSM password
parameter deleted).

## Workload

`trains-valkey-chaos --mode load --count 2000 --hold-secs 30` on node 0 ⇒
phase 1 writes `chaos:k0..k999` (1 000 SETs), 30 s hold, phase 2 writes
`chaos:k1000..k1999` (1 000 SETs). Kill (`fis-kill-redis` on node 2 via SSM)
dispatched 20 s after the chaos-load command was sent, intended to land
during phase 1 or early hold.

## Result

**Mixed.** A second positive demonstration of the headline (zero acked-write
loss + survivor convergence) at higher write volume, but the *during-the-
masked-window* claim — the entire point of E1 — was **not** validated. PR-RD-6
+ PR-RD-8 alone are not sufficient.

### What's confirmed

Direct `valkey-cli` read of each engine after the run hung:

| Engine | `DBSIZE` | `chaos:k0` | `chaos:k50` | `chaos:k100` | `chaos:k500` | `chaos:k999` |
|---|---|---|---|---|---|---|
| node-0 (alive) | **1000** | v0 | v50 | v100 | v500 | v999 |
| node-1 (alive) | **1000** | v0 | v50 | v100 | v500 | v999 |
| node-2 (proxy killed) | **1000** | v0 | v50 | v100 | v500 | v999 |

Every phase-1 write present on every engine, **including the node whose
proxy was SIGKILLed**. Convergence is exact (1000 / 1000 / 1000). This is
the same property the morning's 200/200 result demonstrated, now at 5× the
write volume — confirmatory, but real.

### What's not confirmed (the open issue)

The chaos client hung indefinitely on `SET chaos:k1000` (the first phase-2
write after the hold). After ~3 minutes, `ss -tnp` on the survivors showed:

- **Node 1** (predecessor of the dead node 2): NO outbound TCP socket to
  10.0.0.134 at all — neither `ESTABLISHED` nor `SYN-SENT`. The connector
  loop tore down the dead socket but is not visibly retrying.
- **Node 0** (successor of the dead node 2 via the ring): `ESTAB
  10.0.0.162:37650 → 10.0.2.121:7000 Recv-Q=184` — the outbound to node 1
  still up, with 184 bytes of unread data (likely a TLS-layer alert; the
  connector doesn't read from this socket).
- Both proxies alive (`pgrep -fa trains-valkey` returns the same PIDs as at
  launch); `wait_w` state on the chaos client (blocked on socket).

The binary clearly contains PR-RD-6 (`apply_ring_socket_opts`,
`set_tcp_user_timeout` symbol + the fallback WARN string both present via
`strings /opt/trains/trains-valkey`), but `TCP_USER_TIMEOUT` evidently is
not making the connector's failure detector strike. Three candidate root
causes, ordered by likelihood:

1. **The connector's `select!` between `wire_rx` and `retarget_rx` does
   not include the TCP/TLS stream itself.** When the connection is dead
   and *no new wire message arrives*, the connector idles. Writes only
   happen when wire_rx delivers a new message; if upstream (node 0)
   already stopped issuing trains because the chaos client is blocked
   waiting for the previous round-trip, the connector has nothing to write
   and so never observes the dead socket. This is consistent with the
   `Recv-Q=184` and the lack of SYN-SENT.
2. **`set_tcp_user_timeout` may have returned an error on AL2023's
   kernel** and the WARN-fallback path silently kept the default 15 min
   retransmit budget. The strings include
   `set_tcp_user_timeout failed; peer-death detection falls back to TCP
   defaults` — we couldn't confirm whether it ran (tracing buffer not
   flushed to disk at investigation time).
3. **The `unreachable_tx.try_send(addr)` is a one-shot per streak
   (`notified = true` set unconditionally, including after a failed
   `try_send`).** Unlikely to matter at this scale (channel cap=8), but
   structurally fragile.

The phase-1 writes succeeded because they were *applied to all three
engines before the kill landed*; the kill landed during the tail of phase
1 or in the early hold. The chaos client then slept for the hold and
attempted phase 2 against a broken ring.

## Conclusion — what this means for the paper

The headline *zero acked-write loss + survivor convergence* claim is now
twice-validated at EC2 scale (this run + the morning's 200/200). What
remains genuinely unproven is the original RD-4 promise: that the
*replication continues* through a masked crash on real hardware. PR-RD-6
closes the kernel-level window that's necessary, but not sufficient — the
application-level failure detector still doesn't fire promptly when
upstream traffic is also halted (a coupled failure mode).

The fix candidate (PR-RD-9, sketch):

- Have the connector's idle `select!` ALSO `select!` on a periodic
  health-check tick that issues an empty WRITE to the TLS stream when
  there's been no traffic for >1 s. The write either succeeds (heartbeat)
  or fails (peer dead → connector reconnect path → unreachable signal).
- And/or: read from the TLS stream (with a 1 s timeout) on the connector
  side to detect peer-close, in addition to the listener side.

Either way it's a 1-day operator session to design + test + chaos-retest.
That is the actual next step before the paper can claim "writes continue
through the masked window."

## Artifacts

- This REPORT.md (no acked.json / per-engine PartialReport — the chaos
  client hung mid-write so the JSON output paths were never reached;
  evidence is the direct `DBSIZE` + sample-GET output above).
- The diagnostic transcripts (`ss -tnp`, `/tmp/trains-valkey.out` partial
  flushes, `pgrep`, `strings`) captured in the session log are the
  primary evidence for the open-issue analysis.

## Cost

- 3 × t4g.small × ~25 min ≈ $0.02
- S3 + SSM + CloudFormation: trivial
- **Total < $0.05.** Teardown ran immediately; zero resources outstanding.
