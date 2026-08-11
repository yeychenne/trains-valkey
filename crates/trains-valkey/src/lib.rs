//! trains-valkey — a RESP write-interception proxy that replicates Redis over
//! the TRAINS uniform total-order broadcast ring (state-machine replication).
//!
//! # Why
//! Agent Orchestrator is single-machine today: its coordination fast-path
//! (distributed locks, topology-run dispatch/queue state, run/session status,
//! HITL gate flags) lives in Redis. Replicating that Redis consistently across
//! machines — without a leader-based consensus stack and without losing acked
//! writes on failover — makes AO multi-machine. TRAINS supplies *uniform* total
//! order; Redis's single-threaded, deterministic command model makes it the
//! easiest state-machine-replication target (Schneider 1990).
//!
//! # How (the SMR loop)
//! ```text
//!   client write ─► classify ─► WriteOp ─► oBroadcast (TRAINS, TotalOrder)
//!                                               │
//!                  every node's deliver ◄───────┘
//!                            │
//!              apply to local store (in total order)   ── originating node also
//!   client read ─► local store (no broadcast)             wakes the client
//! ```
//! A write any node delivered is delivered by all survivors (uniform delivery),
//! so a crash is *masked* by the reconfiguration layer instead of triggering a
//! lossy Sentinel failover.
//!
//! # Scope
//! RD-1 (done): deterministic command interception + broadcast + apply, proven
//! by a 3-node convergence test. RD-2 (done): effect replication — the origin
//! resolves non-deterministic *mutations* (`SPOP`→`SREM`, `INCRBYFLOAT`→`SET`,
//! `HINCRBYFLOAT`→`HSET`) to a deterministic effect before broadcast (see
//! [`effect`]); non-deterministic *reads* (`SRANDMEMBER`, `RANDOMKEY`, `TIME`,
//! `SCAN`…) are answered locally with a deterministic pick and never broadcast.
//! RD-3 (done): apply-side `(origin, request_id)` dedup for at-least-once
//! delivery, and replica **state transfer** — [`replica::ReplicaSnapshot`] pairs
//! the `trains-core` protocol [`StateSnapshot`](trains_core::StateSnapshot) seam
//! with the SMR application state (store keyspace + dedup set) so a rejoining
//! replica catches up the whole store, not just the live tail.
//! RD-4 (in-process done): crash masking under the proxy — the reconfiguration
//! layer (◇S failure detector + `ViewChange` token protocol) is wired into the
//! [`proxy`] driver via `ProxyConfig::ring_addrs`, so a permanent crash is
//! masked and survivors keep serving (see `tests/crash_masking.rs`).
//! RJ / V3 (done): node RE-INTEGRATION. A restarted node rejoins — first as a
//! passive replica that catches up from a survivor's snapshot + contiguous
//! delivered-effect tail and tails it ([`ProxyConfig::rejoin`], v2,
//! live-validated on EC2: E5 t1-rejoin), then — opt-in via `RejoinCfg::promote`
//! — promotes back to a full ACKING member through the re-admit view change
//! (v3, restoring N-redundancy; `core::readmit_node` + the survivor `handle_vc`
//! re-admit path + `tests/proxy_tls.rs::promoted_rejoiner_becomes_full_acking_member`).
//! Grounded in the TLC-verified `ReAdmit` spec action; see
//! `docs/PLAN-v3-proxy-promotion-2026-06-16.md` and trains-rust
//! `docs/WHITEPAPER-rejoin-and-readmission-2026-06-16.md`.
//! Out of scope here (later / operator-gated):
//! - **RD-3 follow-up**: `MULTI`/`EXEC`/Lua atomic effect bundles (deferred —
//!   need a `WriteOp` multi-command refactor + proxy transaction state, and Lua
//!   needs an embedded interpreter that conflicts with this repo's lean-deps).
//! - **RD-4 EC2 chaos**: only the operator-gated EC2 `fis-kill` at-scale run
//!   remains. The real `redis-server`/Valkey backend is done — [`backend::RedisBackend`]
//!   implements [`store::RedisStore`] over a loopback RESP connection and is
//!   validated against a live engine (`tests/redis_backend.rs`); pick it on the
//!   binary with `--backend redis://HOST:PORT`. See
//!   `bench/reports/trains-valkey-ec2-backend-research-2026-05-25.md`.
//!
//! # Layout
//! - [`resp`]     — RESP2 request decoder + reply encoder.
//! - [`command`]  — parsed [`command::Command`] + the [`command::WriteOp`] wire envelope.
//! - [`classify`] — static read / write / non-deterministic command table.
//! - [`store`]    — the [`store::RedisStore`] apply/query seam + [`store::MemStore`].
//! - [`replica`]  — [`replica::Replica`]: the I/O-free intercept→broadcast→apply heart.
//! - [`delivered_log`] — bounded delivered-effect tail for rejoin catch-up (PR-RJ-2b).
//! - [`proxy`]    — the async RESP TCP server wired to the TLS ring transport.

pub mod backend;
pub mod chaos;
pub mod classify;
pub mod command;
pub mod delivered_log;
pub mod effect;
pub mod proxy;
pub mod replica;
pub mod resp;
pub mod store;

#[cfg(feature = "arctic-proxy")]
pub mod arctic;

pub use backend::RedisBackend;
pub use chaos::{run_load, verify, verify_one, AckedWrites, PartialReport, VerifyReport};
pub use classify::{classify, Class};
pub use command::{Command, WriteOp};
pub use delivered_log::{DeliveredEntry, DeliveredLog, DEFAULT_CAP};
pub use effect::{resolve, Resolution};
pub use proxy::{run_proxy_node, run_proxy_node_with_reader, ProxyConfig, ProxyHandle, ReadRouter};
pub use replica::{
    apply_delivered_op_parts, build_state_transfer_lazy, Applied, ClientOutcome, OriginDedup,
    Replica, ReplicaSnapshot, Stepped, WriteDedup, SNAPSHOT_VERSION,
};
pub use resp::{Reply, RespDecoder, RespError};
pub use store::{MemStore, RedisStore, SnapshotError, StoreEntry};

#[cfg(feature = "arctic-proxy")]
pub use arctic::{OrderedArcticStore, SharedArcticReader};
