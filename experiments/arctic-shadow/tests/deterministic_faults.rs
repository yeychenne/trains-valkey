use std::collections::{BTreeMap, VecDeque};

use trains_core::DeliveryMode;
use trains_valkey::{Command, DeliveredEntry, MemStore, RedisStore, Replica, Reply, WriteOp};
use trains_valkey_arctic_shadow::MirrorStore;

const NODES: usize = 3;

type Store = MirrorStore<MemStore>;
type Node = Replica<Store>;

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

fn command(argv: Vec<Vec<u8>>) -> Command {
    Command::parse(argv).expect("non-empty command")
}

fn new_node(id: usize) -> Node {
    Replica::new(
        id as u8,
        DeliveryMode::TotalOrder,
        MirrorStore::new(MemStore::new()),
    )
}

fn phase(
    rng: &mut DeterministicRng,
    request_ids: &mut [u64; NODES],
    per_origin: usize,
) -> Vec<WriteOp> {
    let mut queues: Vec<VecDeque<WriteOp>> = (0..NODES)
        .map(|origin| {
            (0..per_origin)
                .map(|_| {
                    let request_id = request_ids[origin];
                    request_ids[origin] += 1;
                    let key = format!("sim:key:{:02}", rng.index(24)).into_bytes();
                    let argv = if rng.index(5) == 0 {
                        vec![b"DEL".to_vec(), key]
                    } else {
                        let value = format!("v:{origin}:{request_id}:{}", rng.next()).into_bytes();
                        vec![b"SET".to_vec(), key, value]
                    };
                    WriteOp::new(origin as u8, request_id, argv)
                })
                .collect()
        })
        .collect();

    let mut ordered = Vec::with_capacity(per_origin * NODES);
    while queues.iter().any(|queue| !queue.is_empty()) {
        let available: Vec<_> = queues
            .iter()
            .enumerate()
            .filter_map(|(origin, queue)| (!queue.is_empty()).then_some(origin))
            .collect();
        let origin = available[rng.index(available.len())];
        ordered.push(queues[origin].pop_front().unwrap());
    }
    ordered
}

fn update_expected(expected: &mut BTreeMap<Vec<u8>, Vec<u8>>, op: &WriteOp) {
    match op.argv[0].as_slice() {
        b"SET" => {
            expected.insert(op.argv[1].clone(), op.argv[2].clone());
        }
        b"DEL" => {
            expected.remove(&op.argv[1]);
        }
        other => panic!("unexpected simulated command {other:?}"),
    }
}

fn deliver_phase(
    nodes: &mut [Node],
    offline: Option<usize>,
    ops: Vec<WriteOp>,
    rng: &mut DeterministicRng,
    next_index: &mut u64,
    expected: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) {
    for op in ops {
        update_expected(expected, &op);
        let frame = DeliveredEntry {
            index: *next_index,
            op,
        }
        .encode();
        *next_index += 1;
        let duplicate = rng.index(4) == 0;

        for (node_id, node) in nodes.iter_mut().enumerate() {
            if offline == Some(node_id) {
                continue;
            }
            assert_eq!(
                node.apply_state_transfer(&[], std::slice::from_ref(&frame))
                    .unwrap(),
                1,
                "first delivery must apply at node {node_id}"
            );
            if duplicate {
                assert_eq!(
                    node.apply_state_transfer(&[], std::slice::from_ref(&frame))
                        .unwrap(),
                    0,
                    "duplicate delivery changed node {node_id}"
                );
            }
        }
    }
}

fn assert_exact_state(nodes: &[Node], expected: &BTreeMap<Vec<u8>, Vec<u8>>, delivered_index: u64) {
    let expected_snapshot: Vec<_> = expected
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let dbsize = command(vec![b"DBSIZE".to_vec()]);

    for (node_id, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.delivered_index(),
            delivered_index,
            "node {node_id} has the wrong delivered index"
        );
        assert_eq!(
            node.store().query(&dbsize),
            Reply::Integer(expected.len() as i64),
            "node {node_id} has the wrong cardinality"
        );
        assert_eq!(
            node.store().shadow().snapshot_sorted(),
            expected_snapshot,
            "node {node_id} differs from the acknowledged manifest"
        );
    }
}

#[test]
fn deterministic_fault_schedules_preserve_mirrored_state_across_victim_rotation() {
    for seed in [1, 7, 19, 41, 97, 257] {
        let mut rng = DeterministicRng::new(seed);
        let mut nodes: Vec<_> = (0..NODES).map(new_node).collect();
        let mut request_ids = [0; NODES];
        let mut next_index = 0;
        let mut expected = BTreeMap::new();

        deliver_phase(
            &mut nodes,
            None,
            phase(&mut rng, &mut request_ids, 12),
            &mut rng,
            &mut next_index,
            &mut expected,
        );
        assert_exact_state(&nodes, &expected, next_index);

        for victim in [2, 0, 1] {
            let source = (victim + 1) % NODES;
            let have_before_crash = nodes[victim].delivered_index();

            deliver_phase(
                &mut nodes,
                Some(victim),
                phase(&mut rng, &mut request_ids, 15),
                &mut rng,
                &mut next_index,
                &mut expected,
            );

            // Simulate a lost catch-up response. Nothing from this poll reaches
            // the victim; the subsequent restart must remain retry-safe.
            let _dropped_transfer = nodes[source].build_state_transfer(have_before_crash);

            // Preserve a stale local image across process restart and add a key
            // that cannot exist in the survivor snapshot. Full import must wipe it.
            let stale_bytes = nodes[victim].store().export_snapshot();
            let mut stale_store = MirrorStore::new(MemStore::new());
            stale_store.import_snapshot(&stale_bytes).unwrap();
            assert_eq!(
                stale_store.apply(&command(vec![
                    b"SET".to_vec(),
                    b"sim:stale-only".to_vec(),
                    victim.to_string().into_bytes(),
                ])),
                Reply::ok()
            );
            let mut restarted = Replica::new(victim as u8, DeliveryMode::TotalOrder, stale_store);

            // Take the full replacement snapshot, then delay its installation
            // while more operations commit on the reduced view.
            let (snapshot, snapshot_tail) = nodes[source].build_state_transfer(0);
            assert!(!snapshot.is_empty());
            deliver_phase(
                &mut nodes,
                Some(victim),
                phase(&mut rng, &mut request_ids, 4),
                &mut rng,
                &mut next_index,
                &mut expected,
            );
            restarted
                .apply_state_transfer(&snapshot, &snapshot_tail)
                .unwrap();
            assert_eq!(
                restarted
                    .store()
                    .query(&command(vec![b"GET".to_vec(), b"sim:stale-only".to_vec(),])),
                Reply::Nil,
                "full snapshot did not replace stale state for victim {victim}"
            );

            // Poll from snapshot index X for the delayed tail. Applying the
            // complete response twice models an overlapping retry.
            let (incremental_snapshot, tail) =
                nodes[source].build_state_transfer(restarted.delivered_index());
            assert!(incremental_snapshot.is_empty());
            let applied = restarted.apply_state_transfer(&[], &tail).unwrap();
            assert_eq!(applied, tail.len());
            assert_eq!(restarted.apply_state_transfer(&[], &tail).unwrap(), 0);
            nodes[victim] = restarted;

            assert_exact_state(&nodes, &expected, next_index);

            deliver_phase(
                &mut nodes,
                None,
                phase(&mut rng, &mut request_ids, 5),
                &mut rng,
                &mut next_index,
                &mut expected,
            );
            assert_exact_state(&nodes, &expected, next_index);
        }
    }
}
