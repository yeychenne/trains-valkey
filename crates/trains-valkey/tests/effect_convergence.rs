//! PR-RD-2 acceptance: convergence holds when the workload exercises the
//! non-deterministic *mutations* (`SPOP`, `INCRBYFLOAT`, `HINCRBYFLOAT`).
//!
//! Each is resolved at its originating node into a deterministic effect
//! (`SREM` / `SET` / `HSET`) before broadcast, so every replica applies the
//! identical effect stream and converges — the failure mode RD-2 closes is
//! replicas independently resolving the randomness/float and diverging.
//!
//! Resolution reads *committed* state, so the workload is **phased**: the
//! inputs (`SADD`, `SET`, `HSET`) are injected and driven to quiescence first,
//! then the non-deterministic mutations are injected (now seeing the committed
//! set / base values) and driven again. This mirrors the real ordering
//! constraint — you can't resolve `SPOP` against a set that hasn't arrived yet.

use trains_core::{DeliveryMode, Train, RING_SIZE};
use trains_valkey::replica::ClientOutcome;
use trains_valkey::{Command, MemStore, RedisStore, Reply, Replica};

const NUM_ISSUERS: u8 = 2;

fn cmd(s: &str) -> Command {
    Command::parse(s.split_whitespace().map(|w| w.as_bytes().to_vec()).collect()).unwrap()
}

/// A 3-node in-process ring of replicas with phased inject + drive.
struct Ring {
    replicas: Vec<Replica<MemStore>>,
    in_flight: Vec<Option<Train>>,
}

impl Ring {
    fn new(mode: DeliveryMode) -> Self {
        let mut replicas: Vec<Replica<MemStore>> = (0..RING_SIZE as u8)
            .map(|id| Replica::new(id, mode, MemStore::new()))
            .collect();
        let mut in_flight = vec![None; RING_SIZE];
        for issuer in 0..NUM_ISSUERS as usize {
            in_flight[issuer] = Some(replicas[issuer].issue_initial());
        }
        Ring { replicas, in_flight }
    }

    fn inject(&mut self, node: usize, c: &str) -> ClientOutcome {
        self.replicas[node].handle(&cmd(c))
    }

    fn run(&mut self, steps: usize) {
        let Ring { replicas, in_flight } = self;
        for _ in 0..steps {
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
            *in_flight = next;
        }
    }

    fn store(&self, node: usize) -> &MemStore {
        self.replicas[node].store()
    }

    fn assert_converged(&self) {
        let snap0 = self.store(0).snapshot_sorted();
        for i in 1..RING_SIZE {
            assert_eq!(
                snap0,
                self.store(i).snapshot_sorted(),
                "node 0 and node {i} diverged"
            );
        }
    }
}

fn run_effects(mode: DeliveryMode) {
    let mut ring = Ring::new(mode);

    // ── Phase 1: commit the inputs the resolutions will read. ──
    ring.inject(0, "SADD myset a b c d");
    ring.inject(1, "SET price 10.0");
    ring.inject(0, "HSET stats hits 100");
    ring.run(150);
    ring.assert_converged();
    assert_eq!(ring.store(0).query(&cmd("SCARD myset")), Reply::Integer(4));

    // ── Phase 2: non-deterministic mutations, resolved against committed state.
    let pop = ring.inject(2, "SPOP myset"); // node 2 is a non-issuer
    let inc = ring.inject(1, "INCRBYFLOAT price 1.5");
    let hinc = ring.inject(0, "HINCRBYFLOAT stats hits 5");
    ring.run(200);

    // Each resolved to a broadcastable effect (inputs were present).
    assert!(matches!(pop, ClientOutcome::Broadcast { .. }), "SPOP should broadcast SREM");
    assert!(matches!(inc, ClientOutcome::Broadcast { .. }), "INCRBYFLOAT should broadcast SET");
    assert!(matches!(hinc, ClientOutcome::Broadcast { .. }), "HINCRBYFLOAT should broadcast HSET");

    // Headline: every replica converged.
    ring.assert_converged();

    // The deterministic effects landed identically everywhere.
    for i in 0..RING_SIZE {
        let s = ring.store(i);
        assert_eq!(s.query(&cmd("SCARD myset")), Reply::Integer(3), "node {i}: one member popped");
        assert_eq!(
            s.query(&cmd("GET price")),
            Reply::Bulk(b"11.5".to_vec()),
            "node {i}: INCRBYFLOAT resolved to 11.5"
        );
        assert_eq!(
            s.query(&cmd("HGET stats hits")),
            Reply::Bulk(b"105".to_vec()),
            "node {i}: HINCRBYFLOAT resolved to 105"
        );
    }
}

#[test]
fn effects_converge_uto() {
    run_effects(DeliveryMode::UniformTotalOrder);
}

#[test]
fn effects_converge_total_order() {
    run_effects(DeliveryMode::TotalOrder);
}

#[test]
fn spop_pops_a_real_member_consistently() {
    // The popped member (whatever the origin chose) is actually removed on
    // every replica — no replica keeps a member another dropped.
    let mut ring = Ring::new(DeliveryMode::UniformTotalOrder);
    ring.inject(0, "SADD s x y z");
    ring.run(150);
    ring.inject(0, "SPOP s");
    ring.run(150);
    ring.assert_converged();
    // Exactly one of x/y/z is gone, the same one on every node (guaranteed by
    // assert_converged); two remain.
    assert_eq!(ring.store(0).query(&cmd("SCARD s")), Reply::Integer(2));
}
