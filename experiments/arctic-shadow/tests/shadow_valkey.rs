use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use trains_core::DeliveryMode;
use trains_net::{NodeIdentity, RingConfig};
use trains_valkey::proxy::{
    ProxyConfig, ProxyHandle, RejoinCfg, SnapshotServerCfg, run_proxy_node,
};
use trains_valkey::{
    Command as RCommand, DeliveredLog, RedisBackend, RedisStore, Reply, WriteDedup, WriteOp,
    apply_delivered_op_parts,
};
use trains_valkey_arctic_shadow::MirrorStore;

const RING: usize = 3;
const NUM_ISSUERS: usize = 2;
const KEYS_PER_ORIGIN: usize = 500;

fn engine_bin() -> Option<&'static str> {
    ["valkey-server", "redis-server"].into_iter().find(|bin| {
        Command::new(bin)
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
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

struct Engine {
    child: Child,
    addr: SocketAddr,
}

impl Engine {
    fn spawn(bin: &str) -> Self {
        let addr = pick_addr();
        let port = addr.port().to_string();
        let child = Command::new(bin)
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
                && backend.query(&command(&["PING"])) == Reply::Simple("PONG".into())
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

fn command(parts: &[&str]) -> RCommand {
    RCommand::parse(parts.iter().map(|part| part.as_bytes().to_vec()).collect()).unwrap()
}

fn clone_identity(identity: &NodeIdentity) -> NodeIdentity {
    NodeIdentity {
        cert_chain: identity.cert_chain.clone(),
        key: identity.key.clone_key(),
        fingerprint: identity.fingerprint,
    }
}

struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Client {
    async fn try_connect(addr: SocketAddr) -> Option<Self> {
        let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
            .await
            .ok()?
            .ok()?;
        let (reader, writer) = stream.into_split();
        Some(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    async fn connect(addr: SocketAddr) -> Self {
        Self::try_connect(addr).await.expect("connect RESP client")
    }

    async fn command(&mut self, parts: &[&str]) -> Reply {
        let mut request = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            request.extend_from_slice(part.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        self.writer
            .write_all(&request)
            .await
            .expect("write RESP request");
        self.writer.flush().await.expect("flush RESP request");
        tokio::time::timeout(Duration::from_secs(20), read_reply(&mut self.reader))
            .await
            .expect("timed out awaiting RESP reply")
            .expect("read RESP reply")
    }
}

async fn read_line(reader: &mut BufReader<OwnedReadHalf>) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).await?;
    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
        line.pop();
    }
    Ok(line)
}

fn read_reply<'a>(
    reader: &'a mut BufReader<OwnedReadHalf>,
) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Reply>> + Send + 'a>> {
    Box::pin(async move {
        let line = read_line(reader).await?;
        if line.is_empty() {
            return Ok(Reply::error("ERR empty reply line"));
        }
        let body = String::from_utf8_lossy(&line[1..]).into_owned();
        Ok(match line[0] {
            b'+' => Reply::Simple(body),
            b'-' => Reply::Error(body),
            b':' => Reply::Integer(body.parse().unwrap_or(0)),
            b'$' => {
                let len: i64 = body.parse().unwrap_or(-1);
                if len < 0 {
                    Reply::Nil
                } else {
                    let mut bytes = vec![0u8; len as usize + 2];
                    reader.read_exact(&mut bytes).await?;
                    bytes.truncate(len as usize);
                    Reply::Bulk(bytes)
                }
            }
            marker => Reply::error(format!("ERR unsupported reply marker {marker}")),
        })
    })
}

fn key(origin: usize, index: usize) -> String {
    format!("arctic:{origin}:{index:06}")
}

fn value(phase: u8, origin: usize, index: usize) -> String {
    format!("v{phase}:{origin}:{index:06}")
}

async fn load_initial(client: &mut Client, origin: usize) {
    for index in 0..KEYS_PER_ORIGIN {
        let key = key(origin, index);
        let value = value(1, origin, index);
        assert_eq!(client.command(&["SET", &key, &value]).await, Reply::ok());
    }
}

async fn mutate(client: &mut Client, origin: usize) {
    for index in (0..KEYS_PER_ORIGIN).step_by(10) {
        let key = key(origin, index);
        let value = value(2, origin, index);
        assert_eq!(client.command(&["SET", &key, &value]).await, Reply::ok());
    }
    for index in (0..KEYS_PER_ORIGIN).step_by(20) {
        let key = key(origin, index);
        assert_eq!(client.command(&["DEL", &key]).await, Reply::Integer(1));
    }
}

type Store = MirrorStore<RedisBackend>;

async fn await_barrier(handles: &[ProxyHandle<Store>], phase: &str) {
    for _ in 0..400 {
        let mut complete = true;
        for handle in handles {
            let store = handle.store.lock().expect("store mutex poisoned");
            for origin in 0..NUM_ISSUERS {
                let marker = format!("arctic:barrier:{origin}");
                if store.query(&command(&["GET", &marker]))
                    != Reply::Bulk(phase.as_bytes().to_vec())
                {
                    complete = false;
                }
            }
        }
        if complete {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("all replicas did not cross barrier {phase}");
}

async fn await_value(addr: SocketAddr, key: &str, expected: Reply) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if let Some(mut client) = Client::try_connect(addr).await {
            let result =
                tokio::time::timeout(Duration::from_secs(1), client.command(&["GET", key])).await;
            if matches!(result, Ok(ref reply) if reply == &expected) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{addr} did not reach GET {key} == {expected:?}");
}

async fn set_until_ok(addr: SocketAddr, key: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if let Some(mut client) = Client::try_connect(addr).await {
            let result =
                tokio::time::timeout(Duration::from_secs(1), client.command(&["SET", key, value]))
                    .await;
            if matches!(result, Ok(Reply::Simple(ref reply)) if reply == "OK") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("SET {key} never committed at {addr}");
}

async fn await_promotion(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if let Some(mut client) = Client::try_connect(addr).await {
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                client.command(&["SET", "lifecycle:promotion:probe", "active"]),
            )
            .await;
            if matches!(result, Ok(Reply::Simple(ref reply)) if reply == "OK") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node at {addr} never promoted into the active ring");
}

#[test]
fn arctic_point_commands_and_snapshot_match_valkey() {
    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };

    let source_engine = Engine::spawn(bin);
    let mut source = MirrorStore::new(RedisBackend::connect(source_engine.addr).unwrap());
    assert_eq!(
        source.apply(&command(&["SET", "arctic:0:000001", "one"])),
        Reply::ok()
    );
    assert_eq!(
        source.apply(&command(&["SET", "arctic:0:000002", "two"])),
        Reply::ok()
    );
    assert_eq!(
        source.query(&command(&["GET", "arctic:0:000001"])),
        Reply::Bulk(b"one".to_vec())
    );
    assert_eq!(
        source.apply(&command(&["DEL", "arctic:0:000002"])),
        Reply::Integer(1)
    );
    assert_eq!(source.query(&command(&["DBSIZE"])), Reply::Integer(1));

    let snapshot = source.export_snapshot();
    let target_engine = Engine::spawn(bin);
    let mut target = MirrorStore::new(RedisBackend::connect(target_engine.addr).unwrap());
    target.import_snapshot(&snapshot).unwrap();

    assert_eq!(
        target.query(&command(&["GET", "arctic:0:000001"])),
        Reply::Bulk(b"one".to_vec())
    );
    assert_eq!(
        target.query(&command(&["GET", "arctic:0:000002"])),
        Reply::Nil
    );
    assert_eq!(
        target.shadow().snapshot_sorted(),
        source.shadow().snapshot_sorted()
    );
}

#[test]
fn duplicate_delivery_applies_to_valkey_and_arctic_once() {
    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };

    let engine = Engine::spawn(bin);
    let mut store = MirrorStore::new(RedisBackend::connect(engine.addr).unwrap());
    let op = WriteOp::new(
        0,
        7,
        vec![
            b"SET".to_vec(),
            b"arctic:0:000007".to_vec(),
            b"seven".to_vec(),
        ],
    );
    let mut dedup = WriteDedup::new();
    let mut delivered = DeliveredLog::default();

    assert_eq!(
        apply_delivered_op_parts(op.clone(), &mut store, &mut dedup, &mut delivered),
        Some(Reply::ok())
    );
    assert_eq!(
        apply_delivered_op_parts(op, &mut store, &mut dedup, &mut delivered),
        None
    );
    assert_eq!(store.query(&command(&["DBSIZE"])), Reply::Integer(1));
    assert_eq!(delivered.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn trains_three_node_arctic_shadow_matches_valkey() {
    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };

    let engines: Vec<Engine> = (0..RING).map(|_| Engine::spawn(bin)).collect();
    let identities: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fingerprints: Vec<_> = identities
        .iter()
        .map(|identity| identity.fingerprint)
        .collect();
    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_addr()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_addr()).collect();

    let mut handles: Vec<ProxyHandle<Store>> = Vec::new();
    for (node, identity) in identities.into_iter().enumerate() {
        let store = MirrorStore::new(RedisBackend::connect(engines[node].addr).unwrap());
        let config = ProxyConfig {
            id: node as u8,
            mode: DeliveryMode::UniformTotalOrder,
            issue_initial: node < NUM_ISSUERS,
            resp_listen: resp_addrs[node],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[node],
                successor_addr: ring_addrs[(node + 1) % RING],
                pinned_peer_fingerprints: fingerprints.clone(),
            },
            ring_addrs: vec![],
            snapshot_server: None,
            rejoin: None,
        };
        handles.push(run_proxy_node(config, store).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut client0 = Client::connect(handles[0].resp_addr).await;
    let mut client1 = Client::connect(handles[1].resp_addr).await;
    tokio::join!(load_initial(&mut client0, 0), load_initial(&mut client1, 1));

    assert_eq!(
        client0
            .command(&["SET", "arctic:barrier:0", "phase1"])
            .await,
        Reply::ok()
    );
    assert_eq!(
        client1
            .command(&["SET", "arctic:barrier:1", "phase1"])
            .await,
        Reply::ok()
    );
    await_barrier(&handles, "phase1").await;

    tokio::join!(mutate(&mut client0, 0), mutate(&mut client1, 1));
    assert_eq!(
        client0
            .command(&["SET", "arctic:barrier:0", "phase2"])
            .await,
        Reply::ok()
    );
    assert_eq!(
        client1
            .command(&["SET", "arctic:barrier:1", "phase2"])
            .await,
        Reply::ok()
    );
    await_barrier(&handles, "phase2").await;

    let expected_size =
        (NUM_ISSUERS * (KEYS_PER_ORIGIN - KEYS_PER_ORIGIN / 20) + NUM_ISSUERS) as i64;
    let mut snapshots = Vec::new();
    for (node, handle) in handles.iter().enumerate() {
        let store = handle.store.lock().expect("store mutex poisoned");
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer(expected_size),
            "node {node}"
        );
        for origin in 0..NUM_ISSUERS {
            for index in 0..KEYS_PER_ORIGIN {
                let key = key(origin, index);
                let expected = if index % 20 == 0 {
                    Reply::Nil
                } else {
                    let phase = if index % 10 == 0 { 2 } else { 1 };
                    Reply::Bulk(value(phase, origin, index).into_bytes())
                };
                assert_eq!(
                    store.query(&command(&["GET", &key])),
                    expected,
                    "node {node}, key {key}"
                );
            }
        }
        snapshots.push(store.shadow().snapshot_sorted());
    }

    assert!(
        snapshots.windows(2).all(|pair| pair[0] == pair[1]),
        "Arctic replicas diverged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn passive_rejoiner_imports_mirrored_snapshot_and_tails_live_writes() {
    const PRE_KEYS_PER_ORIGIN: usize = 100;
    const TAIL_KEYS_PER_ORIGIN: usize = 50;

    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };

    let engines: Vec<Engine> = (0..RING).map(|_| Engine::spawn(bin)).collect();
    let rejoin_engine = Engine::spawn(bin);
    let identities: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fingerprints: Vec<_> = identities
        .iter()
        .map(|identity| identity.fingerprint)
        .collect();
    let snapshot_identity = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
    let snapshot_fingerprint = snapshot_identity.fingerprint;
    let fetcher = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
    let fetcher_fingerprint = fetcher.fingerprint;
    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_addr()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_addr()).collect();
    let snapshot_addr = pick_addr();
    let mut snapshot_identity = Some(snapshot_identity);

    let mut handles: Vec<ProxyHandle<Store>> = Vec::new();
    for (node, identity) in identities.into_iter().enumerate() {
        let store = MirrorStore::new(RedisBackend::connect(engines[node].addr).unwrap());
        let config = ProxyConfig {
            id: node as u8,
            mode: DeliveryMode::UniformTotalOrder,
            issue_initial: node < NUM_ISSUERS,
            resp_listen: resp_addrs[node],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[node],
                successor_addr: ring_addrs[(node + 1) % RING],
                pinned_peer_fingerprints: fingerprints.clone(),
            },
            ring_addrs: vec![],
            snapshot_server: (node == 0).then(|| SnapshotServerCfg {
                listen: snapshot_addr,
                identity: snapshot_identity.take().unwrap(),
                allowed_fetcher_fingerprints: vec![fetcher_fingerprint],
            }),
            rejoin: None,
        };
        handles.push(run_proxy_node(config, store).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut client0 = Client::connect(handles[0].resp_addr).await;
    let mut client1 = Client::connect(handles[1].resp_addr).await;
    for index in 0..PRE_KEYS_PER_ORIGIN {
        let key0 = format!("rejoin:0:{index:06}");
        let key1 = format!("rejoin:1:{index:06}");
        assert_eq!(
            client0.command(&["SET", &key0, "before"]).await,
            Reply::ok()
        );
        assert_eq!(
            client1.command(&["SET", &key1, "before"]).await,
            Reply::ok()
        );
    }
    assert_eq!(
        client0
            .command(&["SET", "rejoin:barrier:0", "phase1"])
            .await,
        Reply::ok()
    );
    assert_eq!(
        client1
            .command(&["SET", "rejoin:barrier:1", "phase1"])
            .await,
        Reply::ok()
    );
    await_value(
        handles[2].resp_addr,
        "rejoin:barrier:1",
        Reply::Bulk(b"phase1".to_vec()),
    )
    .await;

    // Model a restarted engine that retained stale local state while its Arctic
    // in-process shadow was lost with the proxy process.
    let mut stale_backend = RedisBackend::connect(rejoin_engine.addr).unwrap();
    assert_eq!(
        stale_backend.apply(&command(&["SET", "stale:key", "must-be-replaced"])),
        Reply::ok()
    );
    let rejoin_store = MirrorStore::new(stale_backend);
    let throwaway = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
    let rejoiner = run_proxy_node(
        ProxyConfig {
            id: 2,
            mode: DeliveryMode::UniformTotalOrder,
            issue_initial: false,
            resp_listen: pick_addr(),
            client_tls: None,
            ring: RingConfig {
                identity: throwaway,
                listen_addr: pick_addr(),
                successor_addr: pick_addr(),
                pinned_peer_fingerprints: vec![],
            },
            ring_addrs: vec![],
            snapshot_server: None,
            rejoin: Some(RejoinCfg {
                survivor_addrs: vec![snapshot_addr],
                fetch_identity: clone_identity(&fetcher),
                survivor_fingerprints: vec![snapshot_fingerprint],
                poll_interval: Duration::from_millis(50),
                promote: false,
            }),
        },
        rejoin_store,
    )
    .await
    .expect("spawn passive mirrored rejoiner");

    await_value(
        rejoiner.resp_addr,
        "rejoin:barrier:0",
        Reply::Bulk(b"phase1".to_vec()),
    )
    .await;
    {
        let store = rejoiner.store.lock().expect("store mutex poisoned");
        assert_eq!(store.query(&command(&["GET", "stale:key"])), Reply::Nil);
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer((NUM_ISSUERS * PRE_KEYS_PER_ORIGIN + NUM_ISSUERS) as i64)
        );
    }

    // New writes arrive after the full snapshot. The passive node must receive
    // these through incremental delivered-log tails, without another snapshot.
    for index in PRE_KEYS_PER_ORIGIN..PRE_KEYS_PER_ORIGIN + TAIL_KEYS_PER_ORIGIN {
        let key0 = format!("rejoin:0:{index:06}");
        let key1 = format!("rejoin:1:{index:06}");
        assert_eq!(client0.command(&["SET", &key0, "after"]).await, Reply::ok());
        assert_eq!(client1.command(&["SET", &key1, "after"]).await, Reply::ok());
    }
    assert_eq!(
        client0
            .command(&["SET", "rejoin:barrier:0", "phase2"])
            .await,
        Reply::ok()
    );
    assert_eq!(
        client1
            .command(&["SET", "rejoin:barrier:1", "phase2"])
            .await,
        Reply::ok()
    );
    await_value(
        rejoiner.resp_addr,
        "rejoin:barrier:1",
        Reply::Bulk(b"phase2".to_vec()),
    )
    .await;

    let expected_size =
        (NUM_ISSUERS * (PRE_KEYS_PER_ORIGIN + TAIL_KEYS_PER_ORIGIN) + NUM_ISSUERS) as i64;
    let survivor_snapshot = {
        let store = handles[0].store.lock().expect("store mutex poisoned");
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer(expected_size)
        );
        store.shadow().snapshot_sorted()
    };
    {
        let store = rejoiner.store.lock().expect("store mutex poisoned");
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer(expected_size)
        );
        assert_eq!(store.shadow().snapshot_sorted(), survivor_snapshot);
    }

    let mut passive_client = Client::connect(rejoiner.resp_addr).await;
    match passive_client
        .command(&["SET", "rejoin:forbidden", "1"])
        .await
    {
        Reply::Error(message) => assert!(message.to_lowercase().contains("rejoining")),
        reply => panic!("passive rejoiner accepted a write: {reply:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn live_crash_rejoin_promote_restores_mirrored_full_view() {
    const PRE_KEYS: usize = 20;
    const REDUCED_KEYS: usize = 20;
    const TAIL_KEYS: usize = 10;

    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };

    let engines: Vec<Engine> = (0..RING).map(|_| Engine::spawn(bin)).collect();
    let ring_identities: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let ring_fingerprints: Vec<_> = ring_identities
        .iter()
        .map(|identity| identity.fingerprint)
        .collect();
    let snapshot_identities: Vec<NodeIdentity> = (0..2)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let snapshot_fingerprints: Vec<_> = snapshot_identities
        .iter()
        .map(|identity| identity.fingerprint)
        .collect();
    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_addr()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_addr()).collect();
    let snapshot_addrs: Vec<SocketAddr> = (0..2).map(|_| pick_addr()).collect();
    let mut snapshot_identities = snapshot_identities.into_iter();

    let mut handles: Vec<ProxyHandle<Store>> = Vec::new();
    let mut acknowledged_manifest = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for node in 0..RING {
        let store = MirrorStore::new(RedisBackend::connect(engines[node].addr).unwrap());
        let config = ProxyConfig {
            id: node as u8,
            mode: DeliveryMode::TotalOrder,
            issue_initial: node < NUM_ISSUERS,
            resp_listen: resp_addrs[node],
            client_tls: None,
            ring: RingConfig {
                identity: clone_identity(&ring_identities[node]),
                listen_addr: ring_addrs[node],
                successor_addr: ring_addrs[(node + 1) % RING],
                pinned_peer_fingerprints: ring_fingerprints.clone(),
            },
            ring_addrs: ring_addrs.clone(),
            snapshot_server: (node < 2).then(|| SnapshotServerCfg {
                listen: snapshot_addrs[node],
                identity: snapshot_identities.next().unwrap(),
                allowed_fetcher_fingerprints: ring_fingerprints.clone(),
            }),
            rejoin: None,
        };
        handles.push(run_proxy_node(config, store).await.unwrap());
    }

    // The idempotent first write doubles as the healthy-ring formation probe.
    for index in 0..PRE_KEYS {
        let key = format!("lifecycle:pre:{index:04}");
        set_until_ok(handles[0].resp_addr, &key, "before").await;
        acknowledged_manifest.insert(key.into_bytes(), b"before".to_vec());
    }
    set_until_ok(handles[0].resp_addr, "lifecycle:pre:barrier", "before").await;
    acknowledged_manifest.insert(b"lifecycle:pre:barrier".to_vec(), b"before".to_vec());
    await_value(
        handles[2].resp_addr,
        "lifecycle:pre:barrier",
        Reply::Bulk(b"before".to_vec()),
    )
    .await;

    // Crash the live third member. Its Valkey process deliberately survives,
    // while its proxy and Arctic in-process state are destroyed.
    let victim = handles.pop().unwrap();
    victim.crash().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    victim.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;

    handles[0].confirm_crash(2).await;
    for index in 0..REDUCED_KEYS {
        let key = format!("lifecycle:reduced:{index:04}");
        set_until_ok(handles[0].resp_addr, &key, "during").await;
        acknowledged_manifest.insert(key.into_bytes(), b"during".to_vec());
    }
    set_until_ok(handles[0].resp_addr, "lifecycle:reduced:barrier", "during").await;
    acknowledged_manifest.insert(b"lifecycle:reduced:barrier".to_vec(), b"during".to_vec());
    await_value(
        handles[1].resp_addr,
        "lifecycle:reduced:barrier",
        Reply::Bulk(b"during".to_vec()),
    )
    .await;

    // Restart logical node 2 on the same stale engine and ring address. The new
    // MirrorStore has an empty Arctic shadow and must replace both sides from the
    // survivor's full snapshot before promotion.
    let restarted_store = MirrorStore::new(RedisBackend::connect(engines[2].addr).unwrap());
    let rejoiner = run_proxy_node(
        ProxyConfig {
            id: 2,
            mode: DeliveryMode::TotalOrder,
            issue_initial: false,
            resp_listen: pick_addr(),
            client_tls: None,
            ring: RingConfig {
                identity: clone_identity(&ring_identities[2]),
                listen_addr: ring_addrs[2],
                successor_addr: ring_addrs[0],
                pinned_peer_fingerprints: ring_fingerprints.clone(),
            },
            ring_addrs: ring_addrs.clone(),
            snapshot_server: None,
            rejoin: Some(RejoinCfg {
                survivor_addrs: snapshot_addrs.clone(),
                fetch_identity: clone_identity(&ring_identities[2]),
                survivor_fingerprints: snapshot_fingerprints.clone(),
                poll_interval: Duration::from_millis(50),
                promote: true,
            }),
        },
        restarted_store,
    )
    .await
    .expect("restart mirrored node 2 as a promoting rejoiner");

    // Writes continue during catch-up and therefore must arrive through the
    // delivered-log tail after the full mirrored snapshot.
    for index in 0..TAIL_KEYS {
        let key = format!("lifecycle:tail:{index:04}");
        set_until_ok(handles[0].resp_addr, &key, "tail").await;
        acknowledged_manifest.insert(key.into_bytes(), b"tail".to_vec());
    }
    set_until_ok(handles[0].resp_addr, "lifecycle:tail:barrier", "tail").await;
    acknowledged_manifest.insert(b"lifecycle:tail:barrier".to_vec(), b"tail".to_vec());
    await_value(
        rejoiner.resp_addr,
        "lifecycle:tail:barrier",
        Reply::Bulk(b"tail".to_vec()),
    )
    .await;

    // A passive node rejects the probe. Once it returns OK, its active driver
    // has started and its node-originated write has completed through the ring.
    await_promotion(rejoiner.resp_addr).await;
    acknowledged_manifest.insert(b"lifecycle:promotion:probe".to_vec(), b"active".to_vec());
    await_value(
        handles[0].resp_addr,
        "lifecycle:promotion:probe",
        Reply::Bulk(b"active".to_vec()),
    )
    .await;
    set_until_ok(
        handles[0].resp_addr,
        "lifecycle:post:survivor",
        "readmitted",
    )
    .await;
    acknowledged_manifest.insert(b"lifecycle:post:survivor".to_vec(), b"readmitted".to_vec());
    await_value(
        rejoiner.resp_addr,
        "lifecycle:post:survivor",
        Reply::Bulk(b"readmitted".to_vec()),
    )
    .await;
    set_until_ok(handles[0].resp_addr, "lifecycle:final:barrier", "done").await;
    acknowledged_manifest.insert(b"lifecycle:final:barrier".to_vec(), b"done".to_vec());
    await_value(
        rejoiner.resp_addr,
        "lifecycle:final:barrier",
        Reply::Bulk(b"done".to_vec()),
    )
    .await;

    assert_eq!(acknowledged_manifest.len(), 56);
    let expected_size = acknowledged_manifest.len() as i64;
    let survivor_snapshot = {
        let store = handles[0].store.lock().expect("store mutex poisoned");
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer(expected_size)
        );
        store.shadow().snapshot_sorted()
    };
    let expected_snapshot: Vec<_> = acknowledged_manifest.into_iter().collect();
    assert_eq!(
        survivor_snapshot, expected_snapshot,
        "survivor state differs from the acknowledged-operation manifest"
    );
    for (node, handle) in handles.iter().enumerate().skip(1) {
        let store = handle.store.lock().expect("store mutex poisoned");
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer(expected_size),
            "survivor node {node}"
        );
        assert_eq!(store.shadow().snapshot_sorted(), survivor_snapshot);
    }
    {
        let store = rejoiner.store.lock().expect("store mutex poisoned");
        assert_eq!(
            store.query(&command(&["DBSIZE"])),
            Reply::Integer(expected_size)
        );
        assert_eq!(store.shadow().snapshot_sorted(), survivor_snapshot);
    }
}
