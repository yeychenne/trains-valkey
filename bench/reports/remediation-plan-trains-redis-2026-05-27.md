# Remediation Plan — trains-valkey (companion to threat-model-2026-05-27)

> **Sibling doc:** `threat-model-trains-valkey-2026-05-27.md` — read first for context, threat IDs, and the bypass analysis behind each mitigation.
> **Scope of this doc.** Every threat rated **High** plus the **Medium** threats with a feasible same-day mitigation. **Low** threats and large-effort items are tracked at the bottom.

## Buckets at a glance

### Ship this week (trivial + small, H rating)

- **R-01** (T-tr-10) — ~~Reject `NaN`/`±Inf`/overflow in `effect::resolve`~~ **Already implemented** on `main@c80d72f`. PR-SEC-A adds 6 regression tests.
- **R-02** (T-tr-09 + T-tr-19) — Listener semaphore + per-IP cap (small)
- **R-03** (T-tr-11 + T-tr-15) — S3 bucket hardening: **partially shipped in PR-SEC-D** (versioning + access logging). Full version (role-split + Object Lock) needs v1.1 because Object Lock conflicts with `auto_delete_objects=True`.
- **R-04** (T-tr-08 + T-tr-16) — ~~Redact `argv` from `tracing` output~~ **Mostly already mitigated by omission**; one residual + recommendation captured below. PR-SEC-C: doc-only.
- **R-05** (T-tr-20b + T-tr-21) — ~~`bincode` frame-size cap on `WireMsg`~~ **Already implemented** on `main@c80d72f` (`MAX_FRAME_LEN`, default 16 MiB, configurable). PR-SEC-A adds 3 regression tests.

### Plan for v1.1 (medium effort, H or feasible-M)

- **R-06** (T-tr-01 + T-tr-18 + T-tr-18b) — mTLS on RESP client ↔ proxy (TB-A)
- **R-07** (T-tr-14 + T-tr-14b + T-tr-22) — Move Valkey to UNIX domain socket with `0600` perms + `requirepass`
- **R-08** (T-tr-11 + T-tr-15) — Sign `bin/trains` with `rsign2` (minisign); ring-node SSM verifies before exec
- **R-09** (T-tr-02 + T-tr-07 + T-tr-15b) — Append-only audit log of `(origin, request_id, ts, blake3(argv))`
- **R-10** (T-tr-17b) — Cap `Replica::applied_ops` by per-origin watermark + recent-set — **SHIPPED 2026-06-12** (PR-RED-1)
- **R-11** (T-tr-05 + T-tr-20) — View-change frame authorisation via TLS exporter nonce (depends on OPEN-Q-3)

### Track only (Accept, Transfer, or large effort)

- **R-12** (T-tr-03) — Transferred to AWS IAM/MFA/CloudTrail
- **R-13** (T-tr-13) — Coordinator IAM allowlist by instance-id (small, but operator-owned)
- **R-14** (T-tr-04, T-tr-09b, T-tr-12, T-tr-17, T-tr-23) — Accept (out of bench scope)
- **R-15** Long-term key revocation list (OPEN-Q-1) — large, needs design

---

## Ship this week

### R-01 — Reject non-finite floats in effect resolution (T-tr-10) — **ALREADY MITIGATED**

- **Threat one-liner.** A malformed `INCRBYFLOAT NaN` poisons every replica with `SET k NaN`.
- **Actual state on `main@c80d72f` (verified 2026-05-27 PR-SEC-A).**
  1. `parse_f64` in `crates/trains-valkey/src/effect.rs` rejects non-finite values *at parse time* via `v.is_finite().then_some(v)`. NaN, ±Inf, ±infinity → `None` → `Resolution::Immediate(Reply::Error("ERR value is not a valid float"))`. Never broadcasts.
  2. `resolve_incrbyfloat` and `resolve_hincrbyfloat` check `next.is_finite()` *after* the add, catching overflow (e.g. cur=1e308 + incr=1e308 → +Inf). On failure: `Resolution::Immediate(Reply::Error("ERR increment would produce NaN or Infinity"))`. Never broadcasts.
- **PR-SEC-A net new.** 6 regression tests in `crates/trains-valkey/src/effect.rs::tests` covering: NaN increment, ±Inf increment (5 spellings), overflow at resolve, HINCRBYFLOAT NaN + overflow, and a white-box test on `parse_f64` itself.
- **LOC.** 0 source, ~90 of test (regression coverage).
- **Acceptance.** ✅ all 6 new tests pass; existing 8 effect tests pass; full `cargo test --workspace` still green.
- **Effort.** trivial (regression coverage only).
- **Ship in.** v1.0 (already shipped — coverage gap closed).

### R-02 — Listener backpressure: global semaphore + per-IP cap (T-tr-09, T-tr-19)

- **Threat one-liner.** Slowloris and SPOP-on-empty-set storms exhaust `cmd_tx` and the accept backlog.
- **Steps.**
  1. In `crates/trains-valkey/src/proxy.rs::listener_loop`, add `let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONNS));` (default `MAX_CONNS=512`).
  2. Maintain `Arc<Mutex<HashMap<IpAddr, u32>>>` with `PER_IP_CAP=32`.
  3. Before each `tokio::spawn(client_loop(...))`, try-acquire a semaphore permit AND increment the per-IP counter; on failure, write `-ERR busy` and close.
  4. Decrement on drop (use a small `ConnGuard` newtype).
- **LOC.** ~50 in `proxy.rs` plus a `ConnGuard` helper.
- **Acceptance.** Integration test `slow_clients_dont_starve_fast_clients` — open 600 idle connections, then verify a 601st client gets `-ERR busy` AND existing healthy clients keep getting `+OK`.
- **Effort.** small (2-3 h).
- **Ship in.** v1.0.

### R-03 — S3 bucket policy hardening (T-tr-11, T-tr-15) — **PARTIAL (PR-SEC-D)**

- **Threat one-liner.** Anyone with `s3:PutObject` can replace `bin/trains` between coordinator upload and ring-node fetch.
- **What PR-SEC-D shipped (v1.0).** Edited `scripts/bench-aws/trains_bench/stacks/compute.py`:
  1. `versioned=True` on the bench bucket. A poisoned `bin/trains` swap is now **recoverable** by version-id within the 7-day non-current-version retention window. Pre-PR-SEC-D, an overwrite was unrecoverable.
  2. New companion `BenchAccessLogs` bucket; `server_access_logs_bucket=` on the bench bucket routes every `GET`/`PUT`/`DELETE` into it for forensics. 90-day lifecycle. BlockPublicAccess.BLOCK_ALL on the log bucket too.
  3. `noncurrent_version_expiration=7 days` lifecycle rule on the bench bucket to cap versioning cost.
  4. `cdk synth` clean; ready for the next `cdk deploy` to take effect.
- **What's still owed to v1.1.**
  - **Object Lock with Governance retention** on the `bin/*` prefix: blocks deletion even by the bucket owner. Skipped today because Object Lock retention is incompatible with `auto_delete_objects=True` (the bench's "every teardown destroys everything" pattern). Lifting this requires a teardown-script rewrite that explicitly bypasses governance (operator-owned).
  - **Role-based bucket policy** denying `PutObject` on `bin/*` except a dedicated coordinator role. Today all 3 instances share `BenchInstanceRole`, so a same-role policy would be a no-op. The actual fix is to split into `CoordinatorRole` (binary upload) + `NodeRole` (binary download only), then layer the deny policy. ~80 LOC of CDK; tracked as a v1.1 follow-up.
  - **Signed binary distribution (R-08)** — the *correct* defense against the binary-swap threat. The bucket policy is defense in depth; signing is the primary mitigation. Already on v1.1 list.
- **LOC.** +35 in `compute.py`.
- **Acceptance.** ✅ `cdk synth` produces valid CloudFormation for both stacks. ✅ The new `BenchAccessLogs` bucket appears in the synth output with a `LoggingConfiguration` referencing it from the bench bucket. Manual deploy verification deferred to the operator's next bench-sweep run.
- **Effort.** small (~1 h for the partial).
- **Ship in.** v1.0 partial (PR-SEC-D); v1.1 for the role split + Object Lock; v1.1 for R-08 signed binaries (the real fix).

### R-04 — Redact `argv` from logs by default (T-tr-08, T-tr-16) — **MOSTLY ALREADY MITIGATED**

- **Threat one-liner.** `tracing` output and per-node JSON expose every key+value of replicated writes.
- **Actual state on `main@c80d72f` (verified 2026-05-27 PR-SEC-C).**
  1. **T-tr-08 (tracing argv leak): mitigated by omission.** A full grep of `crates/trains-valkey/src/` and `crates/trains-net/src/` for `tracing::(info|debug|trace|warn|error)` calls that touch `argv`, `cmd.`, `reply`, or `payload` returns ZERO matches that emit RESP command content. The only payload-related logs are `tracing::warn!(error = %e, "skipping undecodable delivered payload")` in `proxy.rs:592` and `replica.rs:238`, which emit just the codec error string. No client RESP payload ever reaches a tracing event.
  2. **T-tr-16 (per-node REPORT JSON): mitigated.** The public-facing per-node JSON (`report-node-{0,1,2}.json` in every published `bench/results/ec2-2026-05-26*` directory) carries only summary stats: `engine_label`, `acked_total`, `missing_keys`, `dbsize`. **No raw values.** Verified on the morning EC2 run + E1-v2 + E2 + E4 artifacts.
  3. **T-tr-16 residual: the transient `acked.json` carries `[[key, value]]` pairs.** Used by `verify-local` to compare each acked write against the engine's stored value. Lives only in the private bench S3 bucket (`Project=trains-bench` tag, coordinator-role-only read) and never propagates further. Bench uses synthetic data (`chaos:kN → vN`), so the practical leak is nil. For **production-like use** with non-synthetic workloads, the recommended pattern is to switch verify to a hash-equality check (compare `blake3(engine_value)` to `blake3(acked_value)`); the chaos client emits the hash in JSON, never the raw value. Tracked as a v1.1 follow-up.
- **PR-SEC-C net new.** Doc-only. No code change — the work is already done by omission. The recommendation for production-like runs is documented here and in the threat model.
- **LOC.** 0 source.
- **Acceptance.** ✅ `rg "argv" crates/trains-valkey/src/ crates/trains-net/src/ | rg "tracing::"` returns no payload-emitting matches. ✅ All committed `bench/results/*/report-node-*.json` files contain only summary fields.
- **Effort.** trivial (verification + doc).
- **Ship in.** v1.0 (already shipped — doc gap closed). v1.1 carries the optional hash-mode verify for production use.

### R-05 — Bincode frame-size cap (T-tr-20b, T-tr-21) — **ALREADY MITIGATED**

- **Threat one-liner.** An over-large `WireMsg` allocates unbounded memory before any framing check.
- **Actual state on `main@c80d72f` (verified 2026-05-27 PR-SEC-A).** `crates/trains-net/src/codec.rs` defines `MAX_FRAME_LEN` (emitted by `build.rs` from `TRAINS_MAX_FRAME_LEN_MB`, default **16 MiB**) and checks `len > MAX_FRAME_LEN` *before allocating the body buffer* in both `read_train` and `read_msg`. The check rejects oversize headers as `CodecError::FrameTooLarge(len)`, so an attacker who frames a 4-byte length prefix above the cap is rejected after reading exactly 4 bytes.
- **PR-SEC-A net new.** 3 regression tests in `crates/trains-net/src/codec.rs::tests`:
  - `oversize_train_frame_is_rejected_before_allocation` — `MAX_FRAME_LEN + 1` header → `CodecError::FrameTooLarge`.
  - `oversize_wire_msg_frame_is_rejected_before_allocation` — same for `WireMsg`.
  - `exactly_max_frame_len_is_accepted_at_header_check` — boundary: a header at exactly `MAX_FRAME_LEN` must NOT trigger `FrameTooLarge` (it errors later on body `UnexpectedEof`).
- **Note on the TM's suggested cap.** The TM recommended 1 MiB. The actual default is 16 MiB, which is intentional — the codec carries trains carrying multiple payloads. The cap is operator-tunable per deployment (see `crates/trains-net/build.rs`).
- **LOC.** 0 source, ~50 of test.
- **Acceptance.** ✅ all 3 new tests pass; 4 existing codec tests pass.
- **Effort.** trivial (regression coverage only).
- **Ship in.** v1.0 (already shipped — coverage gap closed).

---

## Plan for v1.1

### R-06 — mTLS on RESP client ↔ proxy boundary (T-tr-01, T-tr-18, T-tr-18b) — **SHIPPED 2026-06-12 (PR-RED-3)**

> **Shipped 2026-06-12.** `ProxyConfig.client_tls: Option<ClientTlsConfig>` +
> `build_client_acceptor` in `proxy.rs`; `listener_loop` runs a `tokio_rustls`
> server handshake (pinned client cert via the reused
> `trains-net::PinnedFingerprintVerifier`, which already impls
> `ClientCertVerifier`) before any RESP byte; `client_loop` made generic over
> `AsyncRead + AsyncWrite + Unpin`. CLI: `--client-identity`,
> `--allowed-client-spki` (repeatable), `--no-client-tls`. Tests:
> `tests/proxy_client_mtls.rs` (right-SPKI acked, wrong-SPKI rejected, plaintext
> rejected). **Two deviations from the sketch below:** (1) **default is mTLS
> OFF** with a loud plaintext startup warning (the sketch wanted default-on, but
> the bench chaos driver speaks plaintext and defaulting-on would break it +
> the E5 harness; turning it on is one flag pair). (2) **The chaos driver's
> sync TLS client is deferred to PR-RED-6** (demos/chaos through the ring),
> where mTLS actually gets enabled for a run — `chaos.rs` is synchronous and a
> `rustls::StreamOwned` client doesn't belong in a focused security PR. No
> `trains-net` change was needed.

- **Threat one-liner.** TB-A is plaintext today; any local actor can speak RESP as any "client".
- **Steps.**
  1. Extend `ProxyConfig` with an optional `client_tls: Option<ClientTlsConfig>` carrying `NodeIdentity` + `Vec<SpkiFingerprint>` for allowed clients.
  2. In `listener_loop`, if `client_tls.is_some()`, wrap accepted `TcpStream` in `tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg))` configured with `PinnedFingerprintVerifier` (already in `trains-net`).
  3. Update `client_loop` signature to accept `AsyncRead + AsyncWrite` (it already does via generics, but verify).
  4. CLI: add `--client-tls-cert`, `--client-tls-key`, `--allowed-client-spki <hex>` (repeatable).
  5. Document a fallback `--no-client-tls` for the bench harness, defaulting to TLS in production.
- **LOC.** ~120 across `proxy.rs`, `trains-net/src/tls.rs`, and the CLI wiring in `trains-cli`.
- **Acceptance.** New integration test `resp_client_with_wrong_spki_is_rejected` + `resp_client_with_right_spki_acked`. Existing tests run with `--no-client-tls` flag.
- **Effort.** medium (~1 d).
- **Ship in.** v1.1.

### R-07 — Valkey on UNIX domain socket + `requirepass` + ACL (T-tr-14, T-tr-14b, T-tr-22) — **SHIPPED 2026-06-12 (PR-RED-4)**

> **Shipped 2026-06-12.** Backend gains a UDS transport: `RedisBackend::connect_uds`
> / `connect_uds_auth` over `std::os::unix::net::UnixStream` (a `Stream` enum
> delegates `Read`/`Write` so `apply`/`query`/snapshot are unchanged). CLI:
> `--backend unix:///path/to/valkey.sock` + `--backend-password-file` (keeps the
> secret out of `ps`/`/proc`; takes precedence over `--backend-password`). The
> EC2 bootstrap (`scripts/redis-chaos/node-bootstrap.sh`) launches Valkey with
> `port 0` + `unixsocket` + `unixsocketperm 700` + `requirepass` when
> `VALKEY_UDS` is set, and `launch-node.sh` then points the proxy at it with a
> 0600 password file. Test: `tests/redis_backend.rs::redis_backend_uds_with_requirepass`
> (right-password works · no-password denied NOAUTH · wrong-password fails AUTH).
> Single-user `requirepass` is used (the threat model's ACL note is satisfied by
> the engine being single-client); per-user ACLs are out of scope at this trust
> level. **Deviation:** UDS mode is opt-in via `VALKEY_UDS` (legacy loopback-TCP
> stays the default) so existing chaos runs and the cross-node verifier are
> unaffected; the hardened path is one env var. Also fixed a latent bug: the
> bootstrap hardcoded `--bind 127.0.0.1`, ignoring its own `VALKEY_BIND` var.

- **Threat one-liner.** Loopback Valkey is reachable by every process on the host with no auth.
- **Steps.**
  1. CDK UserData: write `/etc/valkey/valkey.conf` with
     ```
     port 0
     unixsocket /var/run/trains/valkey.sock
     unixsocketperm 600
     requirepass <ephemeral-secret-injected-via-SSM-parameter>
     ```
     Note: the password lives in an SSM SecureString parameter, fetched at boot by UserData.
  2. Create `/var/run/trains` owned by the `trains` user (the proxy's UID).
  3. Edit `crates/trains-valkey/src/backend.rs` (or wherever `RedisBackend` connects) to take a `RedisEndpoint::Uds(PathBuf)` variant and use `tokio::net::UnixStream::connect(path)`.
  4. CLI: `--backend unix:///var/run/trains/valkey.sock?password=…` (or `--backend-password-file`).
- **LOC.** ~80 of Rust (mostly in `backend.rs` + CLI parsing) + ~30 of UserData shell.
- **Acceptance.** `ss -tlnp | grep 6379` returns nothing on the host; proxy still passes `tests/redis_backend.rs`. Plus a regression test that an unauthenticated `redis-cli -s /var/run/trains/valkey.sock PING` is denied.
- **Effort.** medium (~1 d).
- **Ship in.** v1.1.

### R-08 — Signed binary distribution (T-tr-11, T-tr-15)

- **Threat one-liner.** S3 binary substitution between coordinator upload and ring-node fetch.
- **Steps.**
  1. Add `rsign2` as a `[dev-dependencies]` of `trains-cli` OR as a separate `scripts/bench-aws/sign-binary` Cargo workspace member (operator's call — workspace member is cleaner).
  2. Coordinator UserData: generate or load a keypair (private key in SSM SecureString, public key baked into CDK constant), then after `cargo build --release` run `rsign2 sign target/release/trains` and `aws s3 cp` both `trains` and `trains.minisig`.
  3. Ring-node SSM `AWS-RunShellScript`: download both files, run `rsign2 verify trains` against the public key (constant string in the script), then `chmod +x trains` only on success.
- **LOC.** ~30 of CDK constant + script wiring; no Rust core changes.
- **Acceptance.** Replace `trains.minisig` with a junk file in S3, re-run bench, verify ring-node SSM commands fail with non-zero exit and `Verification failed`. Restore good signature, verify success.
- **Effort.** medium (~½ d including the operator-side key bootstrap).
- **Ship in.** v1.1.

### R-09 — Append-only audit log (T-tr-02, T-tr-07, T-tr-15b)

- **Threat one-liner.** No persisted record of who/what/when for any acked write.
- **Steps.**
  1. Introduce `crates/trains-valkey/src/audit.rs` with `AuditLog::open(path: &Path)` returning a `BufWriter<File>` wrapped in `Mutex`.
  2. After each `(origin, request_id)` apply in the driver loop, write a JSON line: `{ts_ns, origin, request_id, cmd, argv_blake3, view_id}`.
  3. Use `tracing-appender::rolling::daily` for rotation (already a transitive dep via `tracing-subscriber`).
  4. Optionally ship the file to CloudWatch Logs via the SSM VPC endpoint (out-of-scope hook for the operator).
- **LOC.** ~80 in `audit.rs`, ~10 in `proxy.rs`.
- **Acceptance.** Run the chaos workload; `wc -l audit.log` ≥ acked-write count; every `request_id` in the log is unique per `origin`.
- **Effort.** medium (~½ d).
- **Ship in.** v1.1.

### R-10 — Bound `Replica::applied_ops` with per-origin watermark (T-tr-17b) — **SHIPPED 2026-06-12 (PR-RED-1)**

> **Shipped 2026-06-12 (PR-RED-1):** `OriginDedup { watermark, recent: BTreeSet }` per origin (`WriteDedup`), replacing the flat set in both `Replica` and the proxy's `DriverState`; snapshot format bumped (v2) so state-transfer size is independent of op count; 6 new tests incl. a 1M-op soak. Deviation from the sketch below: no entry-dropping cap — `recent` stays tiny by construction (per-origin FIFO ids + total order); crossing a 4096-entry sanity bound logs a loud warning instead of silently dropping (dedup correctness is never traded for memory).

- **Threat one-liner.** Unbounded dedup set OOMs the proxy on long-running workloads.
- **Steps.**
  1. Replace `BTreeSet<(ProcId, u64)>` with `HashMap<ProcId, OriginDedup>` where `OriginDedup { watermark: u64, recent: VecDeque<u64> }` and `recent.capacity() = 65_536`.
  2. On apply: if `request_id <= watermark`, drop; else if `recent.contains(&request_id)`, drop; else apply and push to `recent` (popping the oldest when `recent.len() >= recent.capacity()`, then bumping `watermark` to that popped id).
  3. Property test: dedup behaviour identical to old unbounded set for any reasonable workload (< 65 536 in-flight per origin).
- **LOC.** ~120 in `replica.rs` + ~80 in property tests.
- **Acceptance.** Existing `dedup_*` tests pass unchanged. New `dedup_bounded_under_steady_load` test simulates 200 k writes per origin and asserts memory stays bounded.
- **Effort.** medium (~1 d).
- **Ship in.** v1.1.

### R-11 — View-change frame authorisation via TLS exporter nonce (T-tr-05, T-tr-20)

- **Threat one-liner.** A peer with stolen SPKI-pinned identity can replay/forge view-change frames.
- **Steps.**
  1. Confirm OPEN-Q-3 with the rustls API: `tokio_rustls::TlsStream::get_ref().1.export_keying_material(out, b"trains-vc", None)`.
  2. Embed a 32-byte `exporter_nonce` plus monotonic `view_seq: u64` in `WireMsg::ViewChange`; receiver verifies the nonce matches its own TLS session export, rejects replays where `view_seq <= last_seen`.
  3. Add `verify_view_msg` unit tests with replay and forged scenarios.
- **LOC.** ~80 in `trains-net/src/wire.rs` + view-change handler in `proxy.rs`.
- **Acceptance.** Forged `WireMsg::ViewChange` with stale `view_seq` is rejected; legitimate view change still completes the EC2 chaos run in ≤ 45 s.
- **Effort.** medium (~1-2 d, contingent on OPEN-Q-3).
- **Ship in.** v1.1 (or v1.2 if exporter access requires upstream rustls work).

---

## Track only

### R-12 — Transfer T-tr-03 to AWS IAM + MFA + CloudTrail

- **Action.** Operator enables hardware MFA on the AWS root + IAM users with `cdk deploy` rights; CloudTrail data events on the results bucket; SCPs to block role assumption from outside the operator's source IPs. No code change.

### R-13 — Coordinator `ssm:SendCommand` allowlist (T-tr-13)

- **Threat one-liner.** Tag-filtered SSM scope picks up unrelated EC2 instances if anyone can write the `Project=trains-bench` tag.
- **Action.** Replace the tag-filter `Resource` in the coordinator role with explicit instance ARNs populated by CDK at synth time.
- **Effort.** small but operator-owned (CDK edit + role re-deploy).

### R-14 — Accepted Lows and out-of-scope

- **T-tr-04** repudiation of operator-issued SSM commands (CloudTrail is sufficient evidence for the bench's needs).
- **T-tr-09b** Valkey OOM under malicious large-payload writes (production sizing review owns this).
- **T-tr-12** S3 bucket deletion (single-region single-account risk; multi-region replication is a future cost item).
- **T-tr-17** `/proc/<pid>/mem` write to corrupt `applied_ops` (requires host root; out of scope for a host-trusted threat model).
- **T-tr-23** SSM quota exhaustion (transferred to AWS support quota increase).

### R-15 — Long-term key revocation (OPEN-Q-1)

- **Status.** Large effort, blocking design needed: choose CRL vs OCSP vs in-band view-change `ABORT-NODE` semantics. Currently `PinnedFingerprintVerifier` accepts a static `Vec<SpkiFingerprint>` with no revocation surface.
- **Next step.** Operator decision on rotation cadence and revocation channel before any implementation; capture decision in `bench/reports/keymgmt-design-202X-XX-XX.md`.
- **Effort.** large (multi-week including operator runbook, HSM custody, customer comms surface). Not v1.0/v1.1.
