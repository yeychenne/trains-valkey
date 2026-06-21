//! `Replica` — the I/O-free heart of the write-interception proxy.
//!
//! It composes the verified [`trains_core::TrainsNode`] kernel with a local
//! [`RedisStore`], implementing state-machine replication:
//!
//! * **reads** are answered from the local store (no broadcast);
//! * **deterministic writes** are wrapped in a [`WriteOp`] and `oBroadcast`ed
//!   via `core.step(LocalBroadcast(..))`;
//! * on **delivery** (the totally-ordered stream coming back off the ring),
//!   every replica decodes the `WriteOp` and applies it to its store — so all
//!   replicas converge. The *originating* replica additionally surfaces the
//!   reply so the proxy can answer the waiting client.
//!
//! Like [`trains_ao::AoTrainsNode`], this type owns no transport and does no
//! I/O: it is a pure function of the input event stream, so it slots under any
//! runtime (the async proxy in [`crate::proxy`]) and is trivially testable
//! hand-to-hand in-process (the convergence integration test).

use std::collections::{BTreeMap, BTreeSet};

use trains_core::{DeliveryMode, Input, Output, ProcId, StateSnapshot, Train, TrainsNode};

use crate::classify::{classify, Class};
use crate::command::{Command, WriteOp};
use crate::delivered_log::{DeliveredEntry, DeliveredLog};
use crate::effect::{self, Resolution};
use crate::resp::Reply;
use crate::store::{RedisStore, SnapshotError};

/// What to do with a client command after the replica has handled it.
#[derive(Debug)]
pub enum ClientOutcome {
    /// Answer the client immediately with this reply (reads, and rejected
    /// non-deterministic/unsupported writes).
    Immediate(Reply),
    /// A deterministic write was broadcast onto the ring. The reply is not
    /// known yet — it arrives later, when this op is delivered back (match it
    /// by `request_id` in the [`Stepped::applied`] of a subsequent step). The
    /// `forward` trains, if any, must be routed to the successor.
    Broadcast {
        request_id: u64,
        forward: Vec<Train>,
    },
}

/// A write that was delivered (in total order) and applied to the local store.
#[derive(Debug, Clone)]
pub struct Applied {
    /// Node that originated the write.
    pub origin: ProcId,
    /// The origin's request id (matches a prior [`ClientOutcome::Broadcast`]).
    pub request_id: u64,
    /// The reply produced by applying the command locally.
    pub reply: Reply,
}

/// The result of feeding a train/tick into the replica.
#[derive(Debug, Default)]
pub struct Stepped {
    /// Trains to forward to the successor on the ring.
    pub forward: Vec<Train>,
    /// Writes applied to the local store this step, in delivery order.
    pub applied: Vec<Applied>,
}

/// Sanity bound on an origin's out-of-order window (see [`OriginDedup`]).
/// With per-origin FIFO id issue + total-order delivery the window stays tiny;
/// crossing this bound means an origin is skipping request_ids (a bug) and is
/// logged loudly. Entries are NEVER dropped to enforce it — correctness first.
const RECENT_SANITY_BOUND: usize = 4096;

/// Per-origin write-dedup state (PR-RED-1 / R-10, threat T-tr-17b): a
/// contiguous watermark plus a small out-of-order window, replacing the
/// previous flat `BTreeSet<(ProcId, u64)>` that grew by one entry per write
/// forever (OOM on multi-hour load, ever-fatter snapshots).
///
/// Invariant: a `request_id` has been applied **iff**
/// `id < watermark || recent.contains(&id)`. `watermark` counts the contiguous
/// applied prefix `0..watermark` (every origin assigns ids from a monotonically
/// increasing counter starting at 0 — `next_rid`); `recent` holds applied ids
/// `>= watermark` awaiting their gap to fill. The dedup decision is exactly
/// equivalent to membership in the old unbounded set, and stays deterministic
/// across replicas because TRAINS hands every replica the same total order.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OriginDedup {
    /// All request_ids in `0..watermark` have been applied.
    watermark: u64,
    /// Applied request_ids `>= watermark` (out-of-order window).
    recent: BTreeSet<u64>,
}

impl OriginDedup {
    /// Has this request_id already been applied?
    pub fn is_dup(&self, id: u64) -> bool {
        id < self.watermark || self.recent.contains(&id)
    }

    /// Record a freshly applied id, then sweep the watermark over any
    /// now-contiguous prefix (draining `recent`). Caller must have checked
    /// [`OriginDedup::is_dup`] first (see [`WriteDedup::first_seen`]).
    fn record(&mut self, id: u64) {
        debug_assert!(!self.is_dup(id), "record() called for an already-applied id");
        self.recent.insert(id);
        while self.recent.remove(&self.watermark) {
            self.watermark += 1;
        }
        // Do NOT cap by dropping entries: a large window means an origin is
        // skipping ids (bug) — surface it, don't mask it. (`len` grows by at
        // most 1 per call, so each multiple of the bound warns exactly once.)
        let len = self.recent.len();
        if len >= RECENT_SANITY_BOUND && len % RECENT_SANITY_BOUND == 0 {
            tracing::warn!(
                watermark = self.watermark,
                recent_len = len,
                "dedup out-of-order window unexpectedly large — an origin appears \
                 to be skipping request_ids (likely a bug); refusing to drop entries"
            );
        }
    }

    /// The contiguous applied prefix is `0..watermark`.
    pub fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Size of the out-of-order window (should stay near 0 in healthy runs).
    pub fn recent_len(&self) -> usize {
        self.recent.len()
    }
}

/// Apply-side write dedup across all origins (at-least-once / C3): bounded
/// per-origin watermark + recent-window state (PR-RED-1 / R-10).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteDedup {
    origins: BTreeMap<ProcId, OriginDedup>,
}

impl WriteDedup {
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` iff `(origin, request_id)` has not been applied before — and
    /// records it. Mirrors `BTreeSet::insert` on the old flat set: the caller
    /// applies the op exactly when this returns `true`.
    pub fn first_seen(&mut self, origin: ProcId, request_id: u64) -> bool {
        let od = self.origins.entry(origin).or_default();
        if od.is_dup(request_id) {
            return false;
        }
        od.record(request_id);
        true
    }

    /// Per-origin dedup state, if any op from `origin` was ever applied.
    pub fn origin(&self, origin: ProcId) -> Option<&OriginDedup> {
        self.origins.get(&origin)
    }
}

/// Current [`ReplicaSnapshot`] format version. 4 = adds `view` (PR-V3-3, the
/// survivor's installed view + dead set, so a rejoiner can `adopt_view` before
/// requesting re-admission); 3 = adds `delivered_index` (PR-RJ-2b, the rejoin
/// tail anchor `X`); 2 = per-origin watermark dedup (PR-RED-1); 1 was the flat
/// `applied_ops: BTreeSet<(ProcId, u64)>`.
pub const SNAPSHOT_VERSION: u32 = 4;

/// The survivor's reconfiguration view, carried in a state transfer so a
/// promoting rejoiner (PR-V3-3) can `ViewChange::adopt_view` and seed a
/// non-fenced `ReAdmitGather`. Filled by the proxy serve path (which holds the
/// `ViewChange`); `None` from the I/O-free [`Replica`] model, which has no view.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ViewInfo {
    /// The survivor's installed view id.
    pub installed_view: u64,
    /// Ring positions the survivor considers crashed (excluded).
    pub dead: Vec<ProcId>,
}

/// A transferable snapshot of a replica's full state for rejoin/state transfer
/// (PR-RD-3). Pairs the `trains-core` protocol [`StateSnapshot`] (the seam the
/// reconfiguration layer defines) with the SMR-layer application state: the
/// serialized store keyspace and the apply-side dedup state. Since PR-RED-1 the
/// dedup state is per-origin watermarks, so the snapshot's size is independent
/// of how many ops were ever applied.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicaSnapshot {
    /// Snapshot format version ([`SNAPSHOT_VERSION`]). Internal format — both
    /// ends of a state transfer run the same binary; the field exists so a
    /// mismatch is diagnosable instead of a silent bincode decode error.
    pub version: u32,
    /// Protocol state from `trains-core` (seen clocks, done keys, view fence).
    pub protocol: StateSnapshot,
    /// Serialized store keyspace ([`RedisStore::export_snapshot`]).
    pub store: Vec<u8>,
    /// Apply-side dedup state, so a re-broadcast of an op already in the
    /// snapshot is not re-applied after the joiner resumes.
    pub dedup: WriteDedup,
    /// Delivered-index `X` (PR-RJ-2b): the count of effects applied into this
    /// snapshot. A rejoiner that imports this snapshot replays the survivor's
    /// delivered-effect tail from `X` onward (the contiguous catch-up of
    /// `ADR-001`), and numbers its own first live delivery `X`. A `u64` scalar,
    /// so the snapshot stays O(1) in op count (PR-RED-1's size invariant holds).
    #[serde(default)]
    pub delivered_index: u64,
    /// The survivor's reconfiguration view (PR-V3-3), for a promoting rejoiner.
    /// `None` from the I/O-free [`Replica`]; `Some` from the proxy serve path.
    #[serde(default)]
    pub view: Option<ViewInfo>,
}

/// A TRAINS replica fronting a [`RedisStore`].
pub struct Replica<S: RedisStore> {
    id: ProcId,
    core: TrainsNode,
    store: S,
    next_rid: u64,
    /// At-least-once dedup (the C3 contract): a re-broadcast carries the
    /// original `(origin, request_id)` but a fresh payload `(sender, seq)`, so
    /// protocol-level dedup doesn't catch it. Bounded per-origin watermarks
    /// (PR-RED-1 / R-10) — memory is O(origins), not O(writes).
    dedup: WriteDedup,
    /// Bounded delivered-effect tail (PR-RJ-2b): every effect that passes dedup
    /// and is applied is appended here, so a survivor can serve a rejoining
    /// replica the contiguous catch-up `> X` after it imports a snapshot at `X`.
    delivered_log: DeliveredLog,
}

impl<S: RedisStore> Replica<S> {
    pub fn new(id: ProcId, mode: DeliveryMode, store: S) -> Self {
        Replica {
            id,
            core: TrainsNode::new(id, mode),
            store,
            next_rid: 0,
            dedup: WriteDedup::new(),
            delivered_log: DeliveredLog::default(),
        }
    }

    /// Export this replica's full state for transfer to a rejoining/new replica
    /// (PR-RD-3). Binds the protocol [`StateSnapshot`] to the application state.
    pub fn export_snapshot(&self) -> ReplicaSnapshot {
        ReplicaSnapshot {
            version: SNAPSHOT_VERSION,
            protocol: self.core.export_state(),
            store: self.store.export_snapshot(),
            dedup: self.dedup.clone(),
            delivered_index: self.delivered_log.head_index(),
            // The I/O-free model has no reconfiguration view; the proxy serve
            // path fills this from its `ViewChange` (PR-V3-3).
            view: None,
        }
    }

    /// Install a [`ReplicaSnapshot`] into this (typically fresh) replica: the
    /// protocol state via `trains-core`'s `import_state`, the store keyspace,
    /// and the dedup state — after which it resumes from the live stream
    /// consistent with the view the snapshot was taken in.
    pub fn import_snapshot(&mut self, snap: ReplicaSnapshot) -> Result<(), SnapshotError> {
        if snap.version != SNAPSHOT_VERSION {
            tracing::warn!(
                got = snap.version,
                expected = SNAPSHOT_VERSION,
                "importing a snapshot with an unexpected format version"
            );
        }
        self.core.import_state(snap.protocol);
        self.store.import_snapshot(&snap.store)?;
        self.dedup = snap.dedup;
        // Resume the delivered-effect log at X: this node has no tail of its own
        // yet, but its first subsequent live delivery must be numbered `X` so it
        // continues the survivors' count with no gap/overlap (PR-RJ-2b, ADR-001).
        self.delivered_log = DeliveredLog::resumed_at(snap.delivered_index, self.delivered_log.cap());
        Ok(())
    }

    /// Apply-side dedup state (bounded per-origin watermarks, PR-RED-1).
    pub fn dedup(&self) -> &WriteDedup {
        &self.dedup
    }

    /// Count of effects applied so far == the delivered-index `X` recorded in a
    /// snapshot. A rejoiner replays a survivor's tail from this point (PR-RJ-2b).
    pub fn delivered_index(&self) -> u64 {
        self.delivered_log.head_index()
    }

    /// The bounded delivered-effect tail, for serving a rejoiner its catch-up
    /// (PR-RJ-2b; the transport that ships it is PR-RJ-2c).
    pub fn delivered_log(&self) -> &DeliveredLog {
        &self.delivered_log
    }

    pub fn id(&self) -> ProcId {
        self.id
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Issue this node's initial train (only issuer nodes should call this).
    pub fn issue_initial(&mut self) -> Train {
        self.core.issue_initial_train()
    }

    /// Handle a parsed client command.
    ///
    /// Reads and rejected writes return [`ClientOutcome::Immediate`]; a
    /// deterministic write is broadcast and returns [`ClientOutcome::Broadcast`].
    pub fn handle(&mut self, cmd: &Command) -> ClientOutcome {
        match classify(&cmd.name) {
            Class::Read => ClientOutcome::Immediate(self.store.query(cmd)),
            // Deterministic write: broadcast the command verbatim; the origin
            // returns the apply result (client_reply = None).
            Class::Write => self.broadcast_write(cmd.argv.clone(), None),
            // Non-deterministic mutation: resolve the effect against committed
            // local state, then broadcast the deterministic rewrite carrying the
            // origin-resolved client reply (PR-RD-2).
            Class::NonDeterministic => match effect::resolve(cmd, &self.store) {
                Resolution::Immediate(reply) => ClientOutcome::Immediate(reply),
                Resolution::Broadcast { argv, client_reply } => {
                    self.broadcast_write(argv, Some(client_reply))
                }
            },
            Class::Unsupported => ClientOutcome::Immediate(Reply::error(format!(
                "ERR unknown command '{}' (not in the replication table)",
                cmd.name
            ))),
        }
    }

    /// Assign a request id, wrap `argv` (+ optional origin-resolved reply) in a
    /// [`WriteOp`], and `oBroadcast` it. Broadcasting only queues the payload —
    /// it is applied when delivered back in total order (see [`Replica::absorb`]);
    /// any trains emitted now must still be forwarded.
    fn broadcast_write(&mut self, argv: Vec<Vec<u8>>, client_reply: Option<Reply>) -> ClientOutcome {
        let request_id = self.next_rid;
        let mut op = WriteOp::new(self.id, request_id, argv);
        if let Some(r) = client_reply {
            op = op.with_client_reply(r);
        }
        let bytes = match op.encode() {
            Ok(b) => b,
            Err(e) => {
                // Note: `next_rid` is NOT consumed on failure. A permanently
                // skipped request_id would leave a gap that pins every
                // replica's dedup watermark for this origin (PR-RED-1).
                return ClientOutcome::Immediate(Reply::error(format!(
                    "ERR failed to encode write for replication: {e}"
                )));
            }
        };
        self.next_rid += 1;
        let outs = self.core.step(Input::LocalBroadcast(bytes));
        let Stepped { forward, .. } = self.absorb(outs);
        ClientOutcome::Broadcast { request_id, forward }
    }

    /// Feed an inbound train; returns trains to forward and any writes applied.
    pub fn on_train(&mut self, t: Train) -> Stepped {
        let outs = self.core.step(Input::TrainReceived(t));
        self.absorb(outs)
    }

    /// Drive a timer tick (delivery/forwarding progress without new input).
    pub fn tick(&mut self) -> Stepped {
        let outs = self.core.step(Input::Tick);
        self.absorb(outs)
    }

    /// Apply one delivered effect through the at-least-once dedup: skip if it
    /// was already applied, else apply it to the store and record it in the
    /// delivered-effect log. Returns the [`Applied`] record iff it applied.
    /// Shared by the live ring path ([`Replica::absorb`]) and the rejoin
    /// catch-up path ([`Replica::apply_state_transfer`]) so both dedup, apply,
    /// and log identically.
    fn apply_delivered_op(&mut self, op: WriteOp) -> Option<Applied> {
        let (origin, request_id) = (op.origin, op.request_id);
        apply_delivered_op_parts(op, &mut self.store, &mut self.dedup, &mut self.delivered_log)
            .map(|reply| Applied { origin, request_id, reply })
    }

    /// **Survivor side (PR-RJ-3).** Build the state transfer that closes a
    /// peer's gap from delivered-index `have` to our current head: either an
    /// incremental tail (when the peer is recent enough to sit within our
    /// retained delivered-effect log) or a full snapshot (a fresh peer with
    /// `have == 0`, or one so far behind the tail was truncated past it).
    ///
    /// Returns `(snapshot_bytes, tail_frames)`: an empty `snapshot_bytes` means
    /// "apply the tail only". The pieces are opaque bytes the caller hands to
    /// the [`trains_net`] transport. `have == 0` always yields a snapshot, so a
    /// rejoiner with a stale store gets a full keyspace replace, not a merge.
    pub fn build_state_transfer(&self, have: u64) -> (Vec<u8>, Vec<Vec<u8>>) {
        build_state_transfer_lazy(have, &self.delivered_log, || self.export_snapshot())
    }

    /// **Rejoiner side (PR-RJ-3).** Apply a fetched state transfer: import the
    /// snapshot (when present — a full keyspace replace, clearing any stale
    /// pre-downtime state), then replay the tail frames through the same
    /// at-least-once dedup as the live path. Returns the number of effects
    /// actually applied (duplicates already covered by the snapshot are no-ops).
    /// Idempotent across overlapping transfers — the consistent-cut + dedup
    /// safety of `ADR-001`.
    pub fn apply_state_transfer(
        &mut self,
        snapshot: &[u8],
        tail: &[Vec<u8>],
    ) -> Result<usize, SnapshotError> {
        if !snapshot.is_empty() {
            let (snap, _) = bincode::serde::decode_from_slice::<ReplicaSnapshot, _>(
                snapshot,
                bincode::config::standard(),
            )?;
            self.import_snapshot(snap)?;
        }
        let mut applied = 0usize;
        for frame in tail {
            let entry = DeliveredEntry::decode(frame)?;
            if self.apply_delivered_op(entry.op).is_some() {
                applied += 1;
            }
        }
        Ok(applied)
    }

    /// Translate core outputs into forwards + applied writes, applying every
    /// delivered [`WriteOp`] to the local store in delivery order.
    fn absorb(&mut self, outs: Vec<Output>) -> Stepped {
        let mut stepped = Stepped::default();
        for o in outs {
            match o {
                Output::ForwardTrain(t) => stepped.forward.push(t),
                Output::Deliver(payloads) => {
                    for p in payloads {
                        match WriteOp::decode(&p.data) {
                            Ok(op) => {
                                if let Some(applied) = self.apply_delivered_op(op) {
                                    stepped.applied.push(applied);
                                }
                            }
                            Err(e) => {
                                // A payload that isn't a WriteOp means a foreign
                                // broadcaster shares the ring. Skip it loudly
                                // rather than corrupt the store.
                                tracing::warn!(error = %e, "skipping undecodable delivered payload");
                            }
                        }
                    }
                }
                Output::DeclareCrash(victim) => {
                    // Failover masking is wired in PR-RD-4; here we only note it.
                    tracing::info!(victim, "core declared crash (handled by reconfiguration in RD-4)");
                }
            }
        }
        stepped
    }
}

/// Apply one delivered effect to raw store / dedup / delivered-log pieces:
/// at-least-once dedup → `store.apply` → append to the tail. Returns the apply
/// reply iff it applied (`None` = a deduped duplicate). Shared by [`Replica`]
/// (live ring + catch-up) and the proxy's passive rejoiner (PR-RJ-3c) so every
/// apply path dedups, applies, and logs identically — no second path to drift.
pub fn apply_delivered_op_parts<S: RedisStore>(
    op: WriteOp,
    store: &mut S,
    dedup: &mut WriteDedup,
    delivered_log: &mut DeliveredLog,
) -> Option<Reply> {
    // Dedup (at-least-once / C3): `first_seen` is false ⇒ duplicate.
    if !dedup.first_seen(op.origin, op.request_id) {
        return None;
    }
    // Apply the (already-deterministic) effect. The client reply is the
    // origin-resolved value when present (RD-2), else the apply result.
    let apply_reply = match op.command() {
        Some(cmd) => store.apply(&cmd),
        None => Reply::error("ERR delivered write op had empty argv"),
    };
    let reply = op.client_reply.clone().unwrap_or(apply_reply);
    // Record in the delivered-effect tail (PR-RJ-2b) so a survivor can serve a
    // rejoiner the contiguous catch-up after a snapshot at X.
    delivered_log.append(op);
    Some(reply)
}

/// Decide and frame the state transfer for a peer at delivered-index `have`
/// (PR-RJ-3). Shared by [`Replica::build_state_transfer`] and the proxy driver,
/// which holds the same [`DeliveredLog`] but stores its snapshot pieces in its
/// own driver state rather than a [`Replica`]. Returns `(snapshot_bytes,
/// tail_frames)` — an empty `snapshot_bytes` means "apply the tail only".
///
/// `make_snapshot` is invoked **only** on the snapshot path, so an incremental
/// poll (`have` within the retained log) never serializes the whole store.
/// `have == 0` always takes the snapshot path, so a rejoiner with a stale store
/// gets a full keyspace replace rather than a merge.
pub fn build_state_transfer_lazy(
    have: u64,
    delivered_log: &DeliveredLog,
    make_snapshot: impl FnOnce() -> ReplicaSnapshot,
) -> (Vec<u8>, Vec<Vec<u8>>) {
    let serve_incrementally =
        have > 0 && have >= delivered_log.low_water_index() && have <= delivered_log.head_index();
    if serve_incrementally {
        let tail = delivered_log.entries_from(have).iter().map(|e| e.encode()).collect();
        (Vec::new(), tail)
    } else {
        // Full reset: snapshot at our head X; the peer re-polls (have = X) for
        // anything delivered while the snapshot was in flight.
        let snap = make_snapshot();
        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard())
            .expect("ReplicaSnapshot is always serializable");
        let tail = delivered_log
            .entries_from(snap.delivered_index)
            .iter()
            .map(|e| e.encode())
            .collect();
        (bytes, tail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    fn cmd(parts: &[&str]) -> Command {
        Command::parse(parts.iter().map(|s| s.as_bytes().to_vec()).collect()).unwrap()
    }

    #[test]
    fn read_is_answered_locally_without_broadcast() {
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        match r.handle(&cmd(&["GET", "missing"])) {
            ClientOutcome::Immediate(Reply::Nil) => {}
            other => panic!("expected immediate Nil, got {other:?}"),
        }
    }

    #[test]
    fn nondeterministic_spop_empty_is_immediate_nil() {
        // RD-2: SPOP of an empty/missing set resolves immediately to nil — no
        // broadcast (nothing to remove).
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        match r.handle(&cmd(&["SPOP", "s"])) {
            ClientOutcome::Immediate(Reply::Nil) => {}
            other => panic!("expected immediate nil, got {other:?}"),
        }
    }

    #[test]
    fn nondeterministic_spop_with_members_broadcasts_effect() {
        // RD-2: with committed members, SPOP resolves to a deterministic SREM
        // effect and is broadcast (reply released on delivery).
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        r.store_mut().apply(&cmd(&["SADD", "s", "a", "b"]));
        match r.handle(&cmd(&["SPOP", "s"])) {
            ClientOutcome::Broadcast { request_id, .. } => assert_eq!(request_id, 0),
            other => panic!("expected broadcast, got {other:?}"),
        }
    }

    #[test]
    fn deterministic_write_is_broadcast_not_applied_yet() {
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        match r.handle(&cmd(&["SET", "a", "1"])) {
            ClientOutcome::Broadcast { request_id, .. } => assert_eq!(request_id, 0),
            other => panic!("expected broadcast, got {other:?}"),
        }
        // Not applied until delivered — the store is still empty.
        assert_eq!(r.store().key_count(), 0);
    }

    /// Deliver a single WriteOp to a replica (helper for dedup/transfer tests).
    fn deliver(r: &mut Replica<MemStore>, op: &WriteOp) -> Stepped {
        let payload = trains_core::Payload { sender: 0, seq: 0, data: op.encode().unwrap() };
        r.absorb(vec![Output::Deliver(vec![payload])])
    }

    #[test]
    fn rebroadcast_applies_once() {
        // RD-3 dedup: the same (origin, request_id) delivered twice applies once.
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        let op = WriteOp::new(1, 5, vec![b"INCR".to_vec(), b"c".to_vec()]);

        let first = deliver(&mut r, &op);
        assert_eq!(first.applied.len(), 1, "first delivery applies");

        let dup = deliver(&mut r, &op);
        assert!(dup.applied.is_empty(), "duplicate (origin,request_id) is skipped");

        assert_eq!(
            r.store().query(&cmd(&["GET", "c"])),
            Reply::Bulk(b"1".to_vec()),
            "INCR applied exactly once despite the re-broadcast"
        );
    }

    #[test]
    fn snapshot_transfers_store_and_dedup_set() {
        // RD-3 state transfer: export from a survivor, import into a fresh
        // replica → store + dedup set both transfer.
        let mut src = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        deliver(&mut src, &WriteOp::new(0, 0, vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]));
        let snap = src.export_snapshot();

        let mut dst = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        dst.import_snapshot(snap).unwrap();

        // Store keyspace transferred.
        assert_eq!(dst.store().query(&cmd(&["GET", "k"])), Reply::Bulk(b"v".to_vec()));

        // Dedup set transferred: re-delivering op (0,0) is skipped, so a stale
        // re-broadcast around the join boundary can't overwrite.
        let stale = WriteOp::new(0, 0, vec![b"SET".to_vec(), b"k".to_vec(), b"STALE".to_vec()]);
        let stepped = deliver(&mut dst, &stale);
        assert!(stepped.applied.is_empty(), "op (0,0) already applied per imported dedup set");
        assert_eq!(dst.store().query(&cmd(&["GET", "k"])), Reply::Bulk(b"v".to_vec()));
    }

    /// Deliver a `SET k v` op with the given origin/request_id (dedup tests).
    fn deliver_id(r: &mut Replica<MemStore>, origin: ProcId, id: u64) -> Stepped {
        let op =
            WriteOp::new(origin, id, vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]);
        deliver(r, &op)
    }

    #[test]
    fn dedup_memory_bounded() {
        // PR-RED-1 / R-10 (T-tr-17b): 100k sequential ops from one origin must
        // NOT grow the dedup structure — the watermark advances and the
        // out-of-order window stays empty, so memory is O(1) in op count
        // (previously: one BTreeSet entry per write, forever).
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        for id in 0..100_000u64 {
            let stepped = deliver_id(&mut r, 1, id);
            assert_eq!(stepped.applied.len(), 1, "op {id} must apply");
            assert!(
                r.dedup().origin(1).unwrap().recent_len() <= 1,
                "recent window must stay O(1) under sequential ids"
            );
        }
        let od = r.dedup().origin(1).unwrap();
        assert_eq!(od.watermark(), 100_000, "watermark covers the whole contiguous prefix");
        assert_eq!(od.recent_len(), 0, "no out-of-order residue after a sequential stream");
    }

    /// PR-RED-1 / R-10: 1M-op soak variant of `dedup_memory_bounded` (release
    /// acceptance run: `cargo test --release dedup_memory_bounded_soak_1m -- --ignored`).
    #[test]
    #[ignore = "soak: run explicitly with --ignored (1M ops)"]
    fn dedup_memory_bounded_soak_1m() {
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        for id in 0..1_000_000u64 {
            deliver_id(&mut r, 1, id);
        }
        let od = r.dedup().origin(1).unwrap();
        assert_eq!(od.watermark(), 1_000_000);
        assert_eq!(od.recent_len(), 0);
    }

    #[test]
    fn dedup_rejects_replay_below_watermark() {
        // PR-RED-1 / R-10: an id at/below the watermark is a duplicate even
        // though it is no longer held in any set.
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        for id in 0..10u64 {
            deliver_id(&mut r, 1, id);
        }
        assert_eq!(r.dedup().origin(1).unwrap().watermark(), 10);
        let replay = deliver_id(&mut r, 1, 3);
        assert!(replay.applied.is_empty(), "id 3 < watermark must be rejected");
        // And the replay must not have polluted the out-of-order window.
        assert_eq!(r.dedup().origin(1).unwrap().recent_len(), 0);
    }

    #[test]
    fn dedup_rejects_replay_in_recent() {
        // PR-RED-1 / R-10: an out-of-order id sitting in `recent` (above the
        // watermark) is also a duplicate on re-delivery.
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        deliver_id(&mut r, 1, 0);
        deliver_id(&mut r, 1, 1);
        deliver_id(&mut r, 1, 5); // gap: 2,3,4 not yet seen → 5 parks in `recent`
        let od = r.dedup().origin(1).unwrap();
        assert_eq!(od.watermark(), 2);
        assert_eq!(od.recent_len(), 1);

        let replay = deliver_id(&mut r, 1, 5);
        assert!(replay.applied.is_empty(), "id 5 already in recent must be rejected");
        assert_eq!(r.dedup().origin(1).unwrap().recent_len(), 1, "no duplicate entry");
    }

    #[test]
    fn dedup_out_of_order_window() {
        // PR-RED-1 / R-10: out-of-order ids park in `recent`, then the
        // watermark sweeps over them once the gap fills, draining the window.
        // (Plan's example "1,2,5,3,4 → watermark 5" adapted to the actual
        // 0-based `next_rid` counter: ids 0,1,2,5,3,4 → watermark 6.)
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        for id in [0u64, 1, 2, 5, 3, 4] {
            let stepped = deliver_id(&mut r, 1, id);
            assert_eq!(stepped.applied.len(), 1, "op {id} is novel and must apply");
        }
        let od = r.dedup().origin(1).unwrap();
        assert_eq!(od.watermark(), 6, "watermark sweeps the filled gap");
        assert_eq!(od.recent_len(), 0, "window drains once contiguous");
    }

    #[test]
    fn delivered_log_appends_applied_effects_and_skips_dups() {
        // PR-RJ-2b: each *applied* effect advances delivered_index and lands in
        // the tail; a deduped re-broadcast does neither (it isn't re-applied, so
        // it must not be re-logged — else the rejoin tail would double-count).
        let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        for id in 0..3u64 {
            deliver_id(&mut r, 1, id);
        }
        assert_eq!(r.delivered_index(), 3, "three applied effects → X=3");
        assert_eq!(r.delivered_log().head_index(), 3);
        let tail = r.delivered_log().entries_from(0);
        assert_eq!(tail.iter().map(|e| e.index).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(tail[1].op.request_id, 1, "tail carries the delivered ops in order");

        // A duplicate delivery is skipped — index and tail length unchanged.
        let dup = deliver_id(&mut r, 1, 1);
        assert!(dup.applied.is_empty(), "dup not applied");
        assert_eq!(r.delivered_index(), 3, "dup must not advance the delivered-index");
        assert_eq!(r.delivered_log().len(), 3, "dup must not be re-logged");
    }

    #[test]
    fn snapshot_carries_and_restores_delivered_index() {
        // PR-RJ-2b: X travels in the snapshot; after import the joiner resumes
        // numbering at X (empty tail, head==X) so its next live delivery is X —
        // contiguous with the survivors, no gap/overlap (ADR-001 consistent cut).
        let mut src = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        for id in 0..7u64 {
            deliver_id(&mut src, 1, id);
        }
        let snap = src.export_snapshot();
        assert_eq!(snap.version, SNAPSHOT_VERSION);
        assert_eq!(snap.delivered_index, 7);

        let mut dst = Replica::new(1, DeliveryMode::UniformTotalOrder, MemStore::new());
        dst.import_snapshot(snap).unwrap();
        assert_eq!(dst.delivered_index(), 7, "joiner resumes at X");
        assert!(dst.delivered_log().is_empty(), "joiner has no tail of its own yet");
        assert_eq!(dst.delivered_log().low_water_index(), 7);

        // Its next applied effect is numbered exactly X, continuing the count.
        let stepped = deliver_id(&mut dst, 2, 0);
        assert_eq!(stepped.applied.len(), 1);
        assert_eq!(dst.delivered_log().entries_from(7)[0].index, 7);
        assert_eq!(dst.delivered_index(), 8);
    }

    #[test]
    fn passive_rejoin_state_transfer_wipes_stale_then_tracks_live_writes() {
        // PR-RJ-3 (E5 t1-rejoin, in-process): a survivor with delivered history;
        // a rejoiner whose store is STALE (kept pre-downtime keys, like the E5
        // node whose valkey outlived its killed proxy). The rejoiner catches up
        // purely by pulling build_state_transfer → apply_state_transfer (no ring
        // membership) and must (a) converge with the stale key WIPED, then (b)
        // stay converged as writes continue, (c) idempotently.
        let mut src = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        let workload = [
            ["SET", "a", "1"],
            ["SET", "b", "2"],
            ["SADD", "s", "x"],
            ["SADD", "s", "y"],
            ["HSET", "h", "k"],
        ];
        for (i, c) in workload.into_iter().enumerate() {
            let op = WriteOp::new(1, i as u64, c.iter().map(|p| p.as_bytes().to_vec()).collect());
            deliver(&mut src, &op);
        }
        assert_eq!(src.delivered_index(), 5);

        // Rejoiner starts with a STALE key the survivor never had.
        let mut dst = Replica::new(2, DeliveryMode::UniformTotalOrder, MemStore::new());
        dst.store_mut().apply(&cmd(&["SET", "stale", "ZOMBIE"]));

        // (a) Fresh rejoin: have=0 ⇒ snapshot (full reset) + tail.
        let (snap, tail) = src.build_state_transfer(0);
        assert!(!snap.is_empty(), "have=0 must yield a snapshot (full keyspace reset)");
        dst.apply_state_transfer(&snap, &tail).unwrap();
        assert_eq!(
            dst.store().export_snapshot(),
            src.store().export_snapshot(),
            "rejoiner keyspace must match the survivor"
        );
        assert_eq!(dst.store().query(&cmd(&["GET", "stale"])), Reply::Nil, "stale key wiped");
        assert_eq!(dst.delivered_index(), 5);

        // (b) Writes continue on the survivor (ids 5..7).
        let more = [["SET", "c", "3"], ["INCR", "counter", ""], ["SADD", "s", "z"]];
        for (i, c) in more.into_iter().enumerate() {
            let argv = c.iter().filter(|p| !p.is_empty()).map(|p| p.as_bytes().to_vec()).collect();
            deliver(&mut src, &WriteOp::new(1, 5 + i as u64, argv));
        }
        // Rejoiner polls incrementally with its current delivered-index.
        let (snap2, tail2) = src.build_state_transfer(dst.delivered_index());
        assert!(snap2.is_empty(), "incremental poll (have>0) carries no snapshot");
        assert_eq!(tail2.len(), 3, "exactly the 3 new effects");
        dst.apply_state_transfer(&snap2, &tail2).unwrap();
        assert_eq!(
            dst.store().export_snapshot(),
            src.store().export_snapshot(),
            "rejoiner stays converged as writes continue"
        );
        assert_eq!(dst.delivered_index(), 8);

        // (c) Idempotent: re-applying the same tail applies nothing (dedup).
        assert_eq!(dst.apply_state_transfer(&snap2, &tail2).unwrap(), 0);
    }

    #[test]
    fn build_state_transfer_forces_snapshot_when_peer_predates_retained_tail() {
        // PR-RJ-3: if the requested `have` is older than the bounded log's
        // low-water (the tail was truncated past it), serve a full snapshot
        // rather than a non-contiguous tail.
        let mut src = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        // Tiny cap so eviction kicks in: rebuild the log bounded.
        src.delivered_log = DeliveredLog::new(4);
        for id in 0..10u64 {
            deliver(&mut src, &WriteOp::new(1, id, vec![b"SET".to_vec(), b"k".to_vec(), id.to_string().into_bytes()]));
        }
        assert!(src.delivered_log().low_water_index() > 2, "old entries evicted");
        // A peer claiming have=2 (below low-water) must get a snapshot.
        let (snap, _tail) = src.build_state_transfer(2);
        assert!(!snap.is_empty(), "peer older than the retained tail ⇒ snapshot");
    }

    #[test]
    fn snapshot_roundtrip_carries_watermarks() {
        // PR-RED-1 / R-10: (a) the snapshot carries the per-origin watermarks —
        // a replayed pre-snapshot id is still rejected after import; (b) the
        // snapshot's serialized size is independent of how many ops were applied.
        fn build(n_ops: u64) -> Replica<MemStore> {
            let mut r = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
            for id in 0..n_ops {
                deliver_id(&mut r, 1, id);
            }
            r
        }
        fn snap_bytes(r: &Replica<MemStore>) -> Vec<u8> {
            bincode::serde::encode_to_vec(r.export_snapshot(), bincode::config::standard())
                .expect("snapshot serializes")
        }

        // (b) size independence: same keyspace, 50× the op history, same bytes.
        // (1_000 and 50_000 both varint-encode to the same width, so the only
        // way the sizes differ is if dedup state scales with op count.)
        let small = build(1_000);
        let large = build(50_000);
        assert_eq!(
            snap_bytes(&small).len(),
            snap_bytes(&large).len(),
            "snapshot size must not scale with the number of applied ops"
        );

        // (a) behavior roundtrip.
        let snap = large.export_snapshot();
        let mut dst = Replica::new(0, DeliveryMode::UniformTotalOrder, MemStore::new());
        dst.import_snapshot(snap).unwrap();
        let replay = deliver_id(&mut dst, 1, 123);
        assert!(replay.applied.is_empty(), "pre-snapshot id still rejected after import");
        assert_eq!(dst.dedup().origin(1).unwrap().watermark(), 50_000);
        let fresh = deliver_id(&mut dst, 1, 50_000);
        assert_eq!(fresh.applied.len(), 1, "next fresh id applies normally");
    }
}
