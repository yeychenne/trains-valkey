//! PR-RD-1 acceptance: a 3-node TRAINS-replicated Redis converges.
//!
//! Drives three [`trains_valkey::Replica`]s hand-to-hand over a simulated ring
//! (the same in-process driver pattern as `trains-core`'s `ring_integration`
//! test — no TLS, fully deterministic), applies a mixed write workload issued
//! from *different* originating nodes, and asserts that after the stream
//! quiesces every replica holds the identical keyspace.
//!
//! The headline property is **convergence**: identical totally-ordered command
//! streams ⇒ identical state on every node. Concrete value assertions are
//! restricted to outcomes that are order-independent (commutative increments,
//! disjoint hash fields) or same-issuer-ordered (per-issuer clocks are
//! monotonic), since the *global* delivery order is the protocol's to choose,
//! not the injection order's.

use trains_core::{DeliveryMode, Train, RING_SIZE};
use trains_valkey::replica::ClientOutcome;
use trains_valkey::store::StoreEntry;
use trains_valkey::{Command, MemStore, RedisStore, Replica};

/// Two issuers (matches the default `NUM_TRAINS=2`); node 2 is a non-issuer
/// that must still be able to broadcast writes.
const NUM_ISSUERS: u8 = 2;

fn cmd(s: &str) -> Command {
    Command::parse(s.split_whitespace().map(|w| w.as_bytes().to_vec()).collect()).unwrap()
}

/// Drive the workload through a 3-node ring under `mode` and return the three
/// converged stores.
fn run_workload(mode: DeliveryMode, writes: &[(usize, &str)]) -> Vec<MemStore> {
    assert_eq!(RING_SIZE, 3, "this test assumes the default RING_SIZE=3 build");

    let mut replicas: Vec<Replica<MemStore>> = (0..RING_SIZE as u8)
        .map(|id| Replica::new(id, mode, MemStore::new()))
        .collect();

    // Issuers seed the ring with their initial trains.
    let mut in_flight: Vec<Option<Train>> = vec![None; RING_SIZE];
    for issuer in 0..NUM_ISSUERS as usize {
        in_flight[issuer] = Some(replicas[issuer].issue_initial());
    }

    // Inject every write at its originating node (queues the payload; it rides
    // the next train through that node). Forward trains from the broadcast are
    // discarded — the circulating in_flight trains carry the protocol forward,
    // exactly as the trains-core ring_integration test does.
    for (node, c) in writes {
        match replicas[*node].handle(&cmd(c)) {
            ClientOutcome::Broadcast { .. } => {}
            other => panic!("expected '{c}' to broadcast, got {other:?}"),
        }
    }

    // Circulate. With 2 trains over a 3-ring, a payload needs ~2 laps to reach
    // FULL_ACK; 300 hops is far more than enough for this workload to quiesce.
    for _ in 0..300 {
        let mut next: Vec<Option<Train>> = vec![None; RING_SIZE];
        for (holder, slot) in in_flight.iter_mut().enumerate() {
            if let Some(train) = slot.take() {
                let succ = (holder + 1) % RING_SIZE;
                let stepped = replicas[succ].on_train(train);
                for t in stepped.forward {
                    next[succ] = Some(t);
                }
            }
        }
        in_flight = next;
    }

    replicas.into_iter().map(|r| r.store().clone()).collect()
}

/// The shared workload: writes originate from all three nodes, including the
/// non-issuer (node 2).
fn workload() -> Vec<(usize, &'static str)> {
    vec![
        (0, "SET user:1 alice"),        // node 0 (issuer)
        (1, "INCR counter"),            // node 1 (issuer)
        (2, "INCR counter"),            // node 2 (non-issuer) — must replicate
        (0, "HSET lock:topo owner node0"),
        (1, "HSET lock:topo ts 12345"), // disjoint field → commutes with owner
        (2, "INCRBY counter 5"),        // counter total = 1+1+5 = 7 (commutes)
        (0, "DEL user:1"),              // same-issuer after SET → user:1 absent
        (2, "SET shared hello"),        // node-2-only key → deterministic value
    ]
}

fn assert_converged(stores: &[MemStore]) {
    let snap0 = stores[0].snapshot_sorted();
    for (i, s) in stores.iter().enumerate().skip(1) {
        assert_eq!(
            snap0,
            s.snapshot_sorted(),
            "node 0 and node {i} keyspaces diverged"
        );
    }
}

fn get_str(store: &MemStore, key: &str) -> Option<Vec<u8>> {
    match store.query(&cmd(&format!("GET {key}"))) {
        trains_valkey::Reply::Bulk(b) => Some(b),
        _ => None,
    }
}

#[test]
fn three_node_redis_converges_uto() {
    let stores = run_workload(DeliveryMode::UniformTotalOrder, &workload());
    assert_converged(&stores);

    // Every replica should reflect the same applied workload.
    for s in &stores {
        // Commutative increments: order doesn't matter, total is 7.
        assert_eq!(get_str(s, "counter").as_deref(), Some(&b"7"[..]), "counter");
        // Disjoint hash fields written by two different nodes both present.
        assert_eq!(
            s.query(&cmd("HGET lock:topo owner")),
            trains_valkey::Reply::Bulk(b"node0".to_vec())
        );
        assert_eq!(
            s.query(&cmd("HGET lock:topo ts")),
            trains_valkey::Reply::Bulk(b"12345".to_vec())
        );
        // Same-issuer SET-then-DEL ⇒ key removed.
        assert_eq!(s.query(&cmd("EXISTS user:1")), trains_valkey::Reply::Integer(0));
        // Non-issuer (node 2) write replicated everywhere.
        assert_eq!(get_str(s, "shared").as_deref(), Some(&b"hello"[..]), "shared");
    }
}

#[test]
fn three_node_redis_converges_total_order() {
    // In a healthy (no-crash) ring, TotalOrder reduces to UTO: live_mask =
    // FULL_ACK. Convergence must hold identically.
    let stores = run_workload(DeliveryMode::TotalOrder, &workload());
    assert_converged(&stores);
    for s in &stores {
        assert_eq!(get_str(s, "counter").as_deref(), Some(&b"7"[..]));
        assert_eq!(get_str(s, "shared").as_deref(), Some(&b"hello"[..]));
    }
}

#[test]
fn converged_state_is_nontrivial() {
    // Guard against a vacuous pass (e.g. nothing ever delivered): the workload
    // must leave a populated, identical keyspace.
    let stores = run_workload(DeliveryMode::UniformTotalOrder, &workload());
    assert!(stores[0].key_count() >= 3, "expected counter, lock:topo, shared");
    // Spot-check the hash landed as a hash on every node.
    for s in &stores {
        let snap = s.snapshot_sorted();
        let lock = snap.iter().find(|(k, _)| k == b"lock:topo");
        assert!(
            matches!(lock, Some((_, StoreEntry::Hash(_)))),
            "lock:topo should be a hash on every replica"
        );
    }
}

#[test]
fn reads_are_consistent_post_quiesce() {
    // The acceptance's "reads consistent post-quiesce": a GET of every key
    // returns the same answer at every replica.
    let stores = run_workload(DeliveryMode::UniformTotalOrder, &workload());
    let keys = ["counter", "shared", "user:1"];
    for k in keys {
        let answers: Vec<_> = stores.iter().map(|s| get_str(s, k)).collect();
        assert!(
            answers.windows(2).all(|w| w[0] == w[1]),
            "GET {k} differs across replicas: {answers:?}"
        );
    }
}
