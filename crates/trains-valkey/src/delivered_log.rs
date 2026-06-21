//! Durable, bounded delivered-effect log (PR-RJ-2b).
//!
//! # Why
//! Rejoin / state transfer (`docs/PLAN-pr-rj-2-readmission-2026-06-15.md`,
//! `ADR-001`) needs a survivor to hand a returning replica everything it missed
//! during downtime. A full snapshot covers the *store*, but a node that was only
//! briefly down would re-transfer the entire keyspace to recover a handful of
//! writes. The cheaper, virtually-synchronous path is: the rejoiner imports a
//! snapshot taken at **delivered-index `X`**, then replays the survivor's
//! **contiguous delivered-effect tail `> X`**. This module is that tail.
//!
//! # What it records
//! Every [`WriteOp`] that passes apply-side dedup (i.e. is actually applied to
//! the store, in total order) is appended here with a monotonically increasing
//! **delivered index**. Because TRAINS delivers the *same* totally-ordered,
//! deduplicated effect stream to every replica, the k-th applied effect is byte
//! identical on every node — so any survivor's tail from `X` is interchangeable
//! with any other's. That interchangeability is exactly what makes the
//! single-source contiguous tail in the plan gap-free.
//!
//! # Bounding (the "durable" caveat)
//! The log is a fixed-capacity ring buffer: it retains the most recent [`cap`]
//! entries and evicts the oldest, so a long-running proxy cannot OOM (the same
//! discipline as the PR-RED-1 bounded dedup). "Durable" here means *retained for
//! the run on the survivor* — a survivor stays up, so an in-memory tail suffices
//! to serve a rejoiner; on-disk persistence across a survivor restart is a later
//! concern and is intentionally out of scope for PR-RJ-2b. The bound implies a
//! **maximum coverable downtime gap**: if a rejoiner's snapshot index is older
//! than [`low_water_index`], the survivor can no longer serve a contiguous tail
//! ([`can_serve_from`] is `false`) and the caller (PR-RJ-3) must fall back to a
//! fresh full snapshot.
//!
//! [`cap`]: DeliveredLog::cap
//! [`low_water_index`]: DeliveredLog::low_water_index
//! [`can_serve_from`]: DeliveredLog::can_serve_from

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::command::WriteOp;

/// Default ring-buffer capacity (entries). A power of two for a tidy bound;
/// ~262k recent effects is far more than any realistic downtime window on the
/// AO coordination plane, while keeping the survivor's tail memory bounded.
/// Tunable per node; PR-RJ-3 may expose it via config.
pub const DEFAULT_CAP: usize = 1 << 18;

/// One entry in the delivered-effect log: the totally-ordered position
/// (`index`) and the deterministic effect applied at that position. Serialized
/// over the wire by PR-RJ-2c (snapshot-tail transport).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveredEntry {
    /// Delivered index: the k-th applied effect (0-based) has index `k`.
    pub index: u64,
    /// The deterministic effect applied at this index (identical on every
    /// replica, so tails are interchangeable across survivors).
    pub op: WriteOp,
}

impl DeliveredEntry {
    /// Serialize for the state-transfer tail (one opaque frame per entry,
    /// PR-RJ-2c transport). Always serializable (it wraps a [`WriteOp`]).
    pub fn encode(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("DeliveredEntry is always serializable")
    }

    /// Decode a tail frame produced by [`DeliveredEntry::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (e, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(e)
    }
}

/// A bounded, append-on-apply log of delivered effects, keyed by delivered
/// index, for serving a rejoining replica its missed tail.
#[derive(Debug, Clone)]
pub struct DeliveredLog {
    /// Most-recent entries, oldest at the front (ring-buffer eviction order).
    entries: VecDeque<DeliveredEntry>,
    /// Index that the *next* appended effect will receive == total effects
    /// ever applied. Never decreases; unaffected by eviction.
    head_index: u64,
    /// Maximum retained entries before the oldest is evicted (≥ 1).
    cap: usize,
}

impl Default for DeliveredLog {
    fn default() -> Self {
        Self::new(DEFAULT_CAP)
    }
}

impl DeliveredLog {
    /// A fresh, empty log with retention `cap` (clamped to ≥ 1).
    pub fn new(cap: usize) -> Self {
        DeliveredLog {
            entries: VecDeque::new(),
            head_index: 0,
            cap: cap.max(1),
        }
    }

    /// An empty log resuming at delivered-index `index` — used right after a
    /// snapshot import so the importing node's first live delivery is numbered
    /// `index`, continuing the survivors' count without a gap or overlap.
    pub fn resumed_at(index: u64, cap: usize) -> Self {
        DeliveredLog {
            entries: VecDeque::new(),
            head_index: index,
            cap: cap.max(1),
        }
    }

    /// Append an applied effect, returning the delivered index it was assigned.
    /// Evicts the oldest entry if the ring is at capacity (advancing
    /// [`DeliveredLog::low_water_index`] but never [`DeliveredLog::head_index`]
    /// backwards).
    pub fn append(&mut self, op: WriteOp) -> u64 {
        let index = self.head_index;
        self.entries.push_back(DeliveredEntry { index, op });
        self.head_index += 1;
        while self.entries.len() > self.cap {
            self.entries.pop_front();
        }
        index
    }

    /// The index the next appended effect will receive == count of effects ever
    /// applied. This is the `X` written into a snapshot ([`crate::replica::ReplicaSnapshot`]).
    pub fn head_index(&self) -> u64 {
        self.head_index
    }

    /// Index of the oldest entry still retained; equals [`head_index`] when the
    /// log is empty. A consumer whose snapshot index is `< low_water_index`
    /// cannot be served a contiguous tail.
    ///
    /// [`head_index`]: DeliveredLog::head_index
    pub fn low_water_index(&self) -> u64 {
        self.entries
            .front()
            .map(|e| e.index)
            .unwrap_or(self.head_index)
    }

    /// Number of entries currently retained.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log currently retains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Retention capacity (entries).
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Can a contiguous tail be served to a consumer sitting at delivered-index
    /// `x` (i.e. one that has applied effects `0..x`)? True iff we still retain
    /// entry `x` — or `x` is exactly the head, meaning the consumer is already
    /// current and the tail is empty. False iff `x` predates the retained window
    /// (tail truncated) or `x` is ahead of our head (consumer claims to know
    /// more than us — a caller bug).
    pub fn can_serve_from(&self, x: u64) -> bool {
        x >= self.low_water_index() && x <= self.head_index
    }

    /// The contiguous tail of entries with index `>= x`, in order. When
    /// [`can_serve_from`] is true this is exactly the effects the consumer
    /// missed; otherwise the result starts at [`low_water_index`] (a gap) and
    /// the caller must treat it as "snapshot too old". Callers should check
    /// [`can_serve_from`] first.
    ///
    /// [`can_serve_from`]: DeliveredLog::can_serve_from
    /// [`low_water_index`]: DeliveredLog::low_water_index
    pub fn entries_from(&self, x: u64) -> Vec<DeliveredEntry> {
        self.entries
            .iter()
            .filter(|e| e.index >= x)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(rid: u64) -> WriteOp {
        // A distinct deterministic effect per id (origin 1), so entries are
        // distinguishable and mirror what `Replica::absorb` would record.
        WriteOp::new(1, rid, vec![b"SET".to_vec(), b"k".to_vec(), rid.to_string().into_bytes()])
    }

    #[test]
    fn append_assigns_monotonic_indices_from_zero() {
        // PR-RJ-2b: the k-th applied effect gets index k; head == count.
        let mut log = DeliveredLog::new(1024);
        assert_eq!(log.head_index(), 0);
        assert!(log.is_empty());
        for k in 0..5u64 {
            assert_eq!(log.append(op(k)), k, "append returns the assigned index");
        }
        assert_eq!(log.head_index(), 5, "head == number of applied effects");
        assert_eq!(log.len(), 5);
        assert_eq!(log.low_water_index(), 0, "nothing evicted yet");
    }

    #[test]
    fn tail_from_returns_contiguous_suffix() {
        // PR-RJ-2b: a rejoiner at index X gets exactly effects X.. .
        let mut log = DeliveredLog::new(1024);
        for k in 0..10u64 {
            log.append(op(k));
        }
        let tail = log.entries_from(7);
        assert_eq!(tail.iter().map(|e| e.index).collect::<Vec<_>>(), vec![7, 8, 9]);
        assert_eq!(tail[0].op, op(7), "carries the effect, not just the index");
        // The whole history is serveable, and the empty tail at head is too.
        assert!(log.can_serve_from(0));
        assert!(log.can_serve_from(10), "consumer already current → empty tail");
        assert!(log.entries_from(10).is_empty());
    }

    #[test]
    fn bounded_eviction_advances_low_water_not_head() {
        // PR-RJ-2b: the ring buffer caps memory — oldest entries are evicted,
        // low_water advances, head keeps counting the full history.
        let cap = 8;
        let mut log = DeliveredLog::new(cap);
        for k in 0..20u64 {
            log.append(op(k));
        }
        assert_eq!(log.len(), cap, "retains at most cap entries");
        assert_eq!(log.head_index(), 20, "head counts every applied effect");
        assert_eq!(log.low_water_index(), 12, "oldest retained == head - cap");
        // Evicted region is no longer serveable; retained region is.
        assert!(!log.can_serve_from(11), "snapshot older than the window → fall back");
        assert!(log.can_serve_from(12), "exactly at the window edge is serveable");
        assert!(log.can_serve_from(20), "current consumer → empty tail");
        let tail = log.entries_from(15);
        assert_eq!(tail.first().unwrap().index, 15);
        assert_eq!(tail.last().unwrap().index, 19);
    }

    #[test]
    fn can_serve_rejects_indices_ahead_of_head() {
        // A consumer claiming to have applied more than us is a caller bug, not
        // a serveable tail.
        let mut log = DeliveredLog::new(16);
        log.append(op(0));
        assert!(!log.can_serve_from(99));
    }

    #[test]
    fn resumed_at_starts_head_without_entries() {
        // PR-RJ-2b: after a snapshot import at X, the log resumes numbering at X
        // so the first live delivery is X (no gap, no overlap with survivors).
        let mut log = DeliveredLog::resumed_at(500, 1024);
        assert_eq!(log.head_index(), 500);
        assert!(log.is_empty());
        assert_eq!(log.low_water_index(), 500, "empty ⇒ low_water == head");
        assert!(log.can_serve_from(500), "nothing missed yet");
        assert_eq!(log.append(op(500)), 500, "first post-import effect is numbered X");
        assert_eq!(log.head_index(), 501);
    }

    #[test]
    fn entry_encode_decode_round_trips() {
        // PR-RJ-2c/3: a tail frame survives serialization byte-for-byte.
        let e = DeliveredEntry { index: 42, op: op(7) };
        let back = DeliveredEntry::decode(&e.encode()).unwrap();
        assert_eq!(e, back);
        assert!(DeliveredEntry::decode(b"not-a-frame").is_err());
    }

    #[test]
    fn cap_is_clamped_to_at_least_one() {
        let mut log = DeliveredLog::new(0);
        assert_eq!(log.cap(), 1);
        log.append(op(0));
        log.append(op(1));
        assert_eq!(log.len(), 1, "cap 0 clamps to 1, still bounded");
        assert_eq!(log.head_index(), 2);
        assert_eq!(log.low_water_index(), 1);
    }
}
