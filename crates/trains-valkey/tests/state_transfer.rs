//! PR-RD-3 acceptance: a killed-then-restarted replica rejoins via state
//! transfer and converges to the survivors' state.
//!
//! After a workload commits on a 3-node ring, node 2 "crashes" (its store is
//! lost) and restarts empty. It state-transfers from a survivor — importing the
//! `trains-core` protocol [`StateSnapshot`] (seam) **and** the SMR application
//! state (store keyspace + dedup set) via [`ReplicaSnapshot`] — then resumes
//! from the live stream. It must (a) immediately match the survivor's keyspace,
//! and (b) stay converged as new writes flow.

use trains_core::{DeliveryMode, Train, RING_SIZE};
use trains_valkey::replica::ReplicaSnapshot;
use trains_valkey::{Command, MemStore, RedisStore, Reply, Replica};

const NUM_ISSUERS: u8 = 2;

fn cmd(s: &str) -> Command {
    Command::parse(s.split_whitespace().map(|w| w.as_bytes().to_vec()).collect()).unwrap()
}

struct Ring {
    mode: DeliveryMode,
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
        Ring { mode, replicas, in_flight }
    }

    fn inject(&mut self, node: usize, c: &str) {
        self.replicas[node].handle(&cmd(c));
    }

    fn run(&mut self, steps: usize) {
        let Ring { replicas, in_flight, .. } = self;
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

    fn snapshot(&self, node: usize) -> ReplicaSnapshot {
        self.replicas[node].export_snapshot()
    }

    /// Simulate `node` crashing and restarting empty, then state-transferring
    /// from `snap`.
    fn rejoin_from(&mut self, node: usize, snap: ReplicaSnapshot) {
        let mut fresh = Replica::new(node as u8, self.mode, MemStore::new());
        fresh.import_snapshot(snap).expect("import snapshot");
        self.replicas[node] = fresh;
    }

    fn assert_converged(&self) {
        let snap0 = self.store(0).snapshot_sorted();
        for i in 1..RING_SIZE {
            assert_eq!(snap0, self.store(i).snapshot_sorted(), "node 0 and node {i} diverged");
        }
    }
}

fn run_rejoin(mode: DeliveryMode) {
    let mut ring = Ring::new(mode);

    // ── A workload the crashed node will have missed. ──
    ring.inject(0, "SET a 1");
    ring.inject(1, "SET b 2");
    ring.inject(0, "SADD s x y z");
    ring.inject(1, "HSET cfg k v");
    ring.run(180);
    ring.assert_converged();
    let pre = ring.store(0).snapshot_sorted();
    assert!(!pre.is_empty(), "workload must populate the keyspace");

    // ── Node 2 crashes (store lost) and restarts empty, then state-transfers
    //    from survivor 0. ──
    let snap = ring.snapshot(0);
    ring.rejoin_from(2, snap);

    // (a) Immediately consistent with the survivor's keyspace.
    assert_eq!(
        ring.store(2).snapshot_sorted(),
        pre,
        "rejoiner did not import the survivor's full keyspace"
    );

    // ── (b) Live writes after rejoin — the rejoiner must keep up. ──
    ring.inject(0, "SET c 3");
    ring.inject(1, "INCR counter");
    ring.inject(0, "SADD s w"); // mutate a key that existed pre-crash
    ring.run(220);

    ring.assert_converged();
    let s2 = ring.store(2);
    assert_eq!(s2.query(&cmd("GET c")), Reply::Bulk(b"3".to_vec()));
    assert_eq!(s2.query(&cmd("GET counter")), Reply::Bulk(b"1".to_vec()));
    assert_eq!(s2.query(&cmd("SCARD s")), Reply::Integer(4), "x y z + w");
}

#[test]
fn rejoiner_converges_via_state_transfer_uto() {
    run_rejoin(DeliveryMode::UniformTotalOrder);
}

#[test]
fn rejoiner_converges_via_state_transfer_total_order() {
    run_rejoin(DeliveryMode::TotalOrder);
}
