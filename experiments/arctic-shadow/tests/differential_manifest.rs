use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::time::Duration;

use trains_valkey::{Command, RedisBackend, RedisStore, Reply};
use trains_valkey_arctic_shadow::MirrorStore;

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(3_037_000_493);
        self.0
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }

    fn bytes(&mut self) -> Vec<u8> {
        let len = 1 + self.index(32);
        (0..len).map(|_| self.next() as u8).collect()
    }
}

fn engine_bin() -> Option<&'static str> {
    ["valkey-server", "redis-server"].into_iter().find(|bin| {
        ProcessCommand::new(bin)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    })
}

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct Engine {
    child: Child,
    addr: SocketAddr,
}

impl Engine {
    fn spawn(bin: &str) -> Self {
        let addr = pick_addr();
        let port = addr.port().to_string();
        let child = ProcessCommand::new(bin)
            .args([
                "--port",
                &port,
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn Valkey engine");
        let engine = Self { child, addr };
        for _ in 0..100 {
            if let Ok(backend) = RedisBackend::connect(addr)
                && backend.query(&command(vec![b"PING".to_vec()])) == Reply::Simple("PONG".into())
            {
                return engine;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("Valkey engine at {addr} never became ready");
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn command(argv: Vec<Vec<u8>>) -> Command {
    Command::parse(argv).expect("non-empty command")
}

fn key(seed: u64, slot: usize) -> Vec<u8> {
    format!("fuzz:{seed:016x}:{slot:03}").into_bytes()
}

#[test]
fn seeded_command_manifest_matches_valkey_and_arctic() {
    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };

    let engine = Engine::spawn(bin);
    let mut store = MirrorStore::new(RedisBackend::connect(engine.addr).unwrap());
    let mut expected = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut acknowledged_writes = Vec::<Vec<Vec<u8>>>::new();

    for seed in [1, 3, 7, 17, 37, 101, 257, 65_537] {
        let mut rng = DeterministicRng::new(seed);
        for _ in 0..512 {
            let first = key(seed, rng.index(64));
            match rng.index(10) {
                0..=4 => {
                    let value = rng.bytes();
                    let argv = vec![b"SET".to_vec(), first.clone(), value.clone()];
                    assert_eq!(store.apply(&command(argv.clone())), Reply::ok());
                    expected.insert(first, value);
                    acknowledged_writes.push(argv);
                }
                5 => {
                    let second = key(seed, rng.index(64));
                    let mut removed = 0;
                    for candidate in [&first, &second] {
                        if expected.remove(candidate).is_some() {
                            removed += 1;
                        }
                    }
                    let argv = vec![b"DEL".to_vec(), first, second];
                    assert_eq!(store.apply(&command(argv.clone())), Reply::Integer(removed));
                    acknowledged_writes.push(argv);
                }
                6..=7 => {
                    let reply = expected
                        .get(&first)
                        .map_or(Reply::Nil, |value| Reply::Bulk(value.clone()));
                    assert_eq!(store.query(&command(vec![b"GET".to_vec(), first])), reply);
                }
                8 => {
                    let second = key(seed, rng.index(64));
                    let present = [&first, &second]
                        .into_iter()
                        .filter(|candidate| expected.contains_key(*candidate))
                        .count() as i64;
                    assert_eq!(
                        store.query(&command(vec![b"EXISTS".to_vec(), first, second])),
                        Reply::Integer(present)
                    );
                }
                9 => assert_eq!(
                    store.query(&command(vec![b"DBSIZE".to_vec()])),
                    Reply::Integer(expected.len() as i64)
                ),
                _ => unreachable!(),
            }
        }
    }

    assert!(acknowledged_writes.len() > 2_000);
    assert_eq!(
        store.query(&command(vec![b"DBSIZE".to_vec()])),
        Reply::Integer(expected.len() as i64)
    );
    let expected_snapshot: Vec<_> = expected.into_iter().collect();
    assert_eq!(store.shadow().snapshot_sorted(), expected_snapshot);
}
