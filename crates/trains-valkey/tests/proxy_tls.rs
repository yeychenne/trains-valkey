//! End-to-end PR-RD-1: a real RESP client driving a 3-node TLS ring of proxies.
//!
//! This is the "skeleton works end to end" demonstration that complements the
//! deterministic in-process `convergence` test: three [`run_proxy_node`]
//! instances form a real TLS ring (the same transport the reconfiguration work
//! is validated on), each exposing a RESP port. A TCP client speaks RESP to one
//! node; writes replicate around the ring and are observable as reads on the
//! other nodes. It also exercises the proxy's command-classification paths
//! (local reads, rejected non-deterministic writes, unsupported commands) over
//! the wire.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use trains_core::DeliveryMode;
use trains_net::{fetch_state, NodeIdentity, RingConfig, SpkiFingerprint};
use trains_valkey::proxy::{run_proxy_node, ProxyConfig, ProxyHandle, SnapshotServerCfg};
use trains_valkey::{Command, MemStore, RedisStore, Replica, Reply};

const RING: usize = 3;
const NUM_ISSUERS: usize = 2;

fn pick_port() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A minimal async RESP client for the test.
struct Client {
    r: BufReader<OwnedReadHalf>,
    w: OwnedWriteHalf,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).await.expect("connect RESP port");
        let (rd, wr) = stream.into_split();
        Client { r: BufReader::new(rd), w: wr }
    }

    /// Send a command and read exactly one reply.
    async fn cmd(&mut self, parts: &[&str]) -> Reply {
        let mut req = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        self.w.write_all(&req).await.expect("write request");
        self.w.flush().await.expect("flush request");
        tokio::time::timeout(Duration::from_secs(20), read_reply(&mut self.r))
            .await
            .expect("timed out awaiting reply")
            .expect("read reply")
    }
}

async fn read_line(r: &mut BufReader<OwnedReadHalf>) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line).await?;
    while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(line)
}

/// Parse one RESP2 reply off the wire (enough types for these assertions).
fn read_reply<'a>(
    r: &'a mut BufReader<OwnedReadHalf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Reply>> + Send + 'a>> {
    Box::pin(async move {
        let line = read_line(r).await?;
        if line.is_empty() {
            return Ok(Reply::error("ERR empty reply line"));
        }
        let body = String::from_utf8_lossy(&line[1..]).into_owned();
        Ok(match line[0] {
            b'+' => Reply::Simple(body),
            b'-' => Reply::Error(body),
            b':' => Reply::Integer(body.trim().parse().unwrap_or(0)),
            b'$' => {
                let n: i64 = body.trim().parse().unwrap_or(-1);
                if n < 0 {
                    Reply::Nil
                } else {
                    let mut buf = vec![0u8; n as usize + 2]; // payload + CRLF
                    r.read_exact(&mut buf).await?;
                    buf.truncate(n as usize);
                    Reply::Bulk(buf)
                }
            }
            b'*' => {
                let n: i64 = body.trim().parse().unwrap_or(-1);
                if n < 0 {
                    Reply::NilArray
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(read_reply(r).await?);
                    }
                    Reply::Array(items)
                }
            }
            other => Reply::error(format!("ERR unknown reply marker {other}")),
        })
    })
}

/// Bring up a 3-node TLS ring of proxy nodes; return their handles + RESP addrs.
async fn spawn_ring(mode: DeliveryMode) -> (Vec<ProxyHandle<MemStore>>, Vec<SocketAddr>) {
    let ids: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fps: Vec<_> = ids.iter().map(|i| i.fingerprint).collect();

    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();

    let mut handles = Vec::new();
    let mut bound_resp = Vec::new();
    for (i, identity) in ids.into_iter().enumerate() {
        let cfg = ProxyConfig {
            id: i as u8,
            mode,
            issue_initial: i < NUM_ISSUERS,
            resp_listen: resp_addrs[i],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[i],
                successor_addr: ring_addrs[(i + 1) % RING],
                pinned_peer_fingerprints: fps.clone(),
            },
            ring_addrs: vec![], // reconfiguration disabled for these RD-1/2 tests
            snapshot_server: None,
            rejoin: None,
        };
        let h = run_proxy_node(cfg, MemStore::new()).await.expect("spawn proxy node");
        bound_resp.push(h.resp_addr);
        handles.push(h);
    }
    (handles, bound_resp)
}

/// Poll a key on `addr` until it reads `expected` (or fail after the budget):
/// replicas may apply a write microseconds after the originator's client reply.
async fn await_get(addr: SocketAddr, key: &str, expected: &[u8]) {
    let mut client = Client::connect(addr).await;
    for _ in 0..100 {
        if let Reply::Bulk(b) = client.cmd(&["GET", key]).await {
            if b == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("replica at {addr} never converged GET {key} == {:?}", String::from_utf8_lossy(expected));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_replicate_across_tls_ring() {
    let _ = tracing_subscriber::fmt::try_init();
    let (_handles, resp) = spawn_ring(DeliveryMode::UniformTotalOrder).await;

    // Drive writes at node 0.
    let mut c0 = Client::connect(resp[0]).await;
    assert_eq!(c0.cmd(&["SET", "a", "1"]).await, Reply::Simple("OK".into()));
    assert_eq!(c0.cmd(&["INCR", "c"]).await, Reply::Integer(1));
    assert_eq!(c0.cmd(&["HSET", "h", "f", "v"]).await, Reply::Integer(1));

    // Drive a write at node 1 too (the second issuer).
    let mut c1 = Client::connect(resp[1]).await;
    assert_eq!(c1.cmd(&["SET", "b", "2"]).await, Reply::Simple("OK".into()));

    // All keys must be observable on every node post-quiesce.
    for &addr in &resp {
        await_get(addr, "a", b"1").await;
        await_get(addr, "b", b"2").await;
        await_get(addr, "c", b"1").await;
    }

    // Hash replicated too.
    let mut c2 = Client::connect(resp[2]).await;
    assert_eq!(c2.cmd(&["HGET", "h", "f"]).await, Reply::Bulk(b"v".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classification_paths_over_the_wire() {
    let _ = tracing_subscriber::fmt::try_init();
    let (_handles, resp) = spawn_ring(DeliveryMode::UniformTotalOrder).await;
    let mut c = Client::connect(resp[0]).await;

    // Local read of a missing key — answered immediately, no ring round-trip.
    assert_eq!(c.cmd(&["GET", "nope"]).await, Reply::Nil);

    // Non-deterministic mutation on an empty set resolves immediately to nil
    // (RD-2: nothing to pop ⇒ no broadcast).
    assert_eq!(c.cmd(&["SPOP", "s"]).await, Reply::Nil);

    // Unsupported command rejected.
    match c.cmd(&["WUTANG", "clan"]).await {
        Reply::Error(e) => assert!(e.to_uppercase().contains("UNKNOWN"), "got: {e}"),
        other => panic!("expected unknown-command error, got {other:?}"),
    }

    // PING is answered locally.
    assert_eq!(c.cmd(&["PING"]).await, Reply::Simple("PONG".into()));
}

/// Poll a command on `addr` until its reply equals `expected` (replicas apply
/// an effect microseconds after the originator's client reply).
async fn await_reply(addr: SocketAddr, parts: &[&str], expected: Reply) {
    let mut client = Client::connect(addr).await;
    for _ in 0..100 {
        if client.cmd(parts).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("replica at {addr} never converged {parts:?} == {expected:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn effect_replication_over_tls() {
    let _ = tracing_subscriber::fmt::try_init();
    let (_handles, resp) = spawn_ring(DeliveryMode::UniformTotalOrder).await;
    let mut c0 = Client::connect(resp[0]).await;

    // Build a set + a float base (these replies return after local commit).
    assert_eq!(c0.cmd(&["SADD", "myset", "a", "b", "c"]).await, Reply::Integer(3));
    assert_eq!(c0.cmd(&["INCRBYFLOAT", "price", "2.5"]).await, Reply::Bulk(b"2.5".to_vec()));

    // SPOP returns one of the concrete members the origin resolved + removed.
    let popped = c0.cmd(&["SPOP", "myset"]).await;
    let popped = match popped {
        Reply::Bulk(b) => b,
        other => panic!("expected a popped member, got {other:?}"),
    };
    assert!(
        [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].contains(&popped),
        "popped member must be a real member, got {:?}",
        String::from_utf8_lossy(&popped)
    );

    // Every replica converges on the resolved effects: 2 members left, price 2.5.
    for &addr in &resp {
        await_reply(addr, &["SCARD", "myset"], Reply::Integer(2)).await;
        await_reply(addr, &["GET", "price"], Reply::Bulk(b"2.5".to_vec())).await;
        // The member popped at node 0 is gone on every replica.
        await_reply(
            addr,
            &["SISMEMBER", "myset", &String::from_utf8_lossy(&popped)],
            Reply::Integer(0),
        )
        .await;
    }
}

// ── PR-RJ-3b: the state-transfer SERVER side, end-to-end over real TLS ────────

fn pcmd(s: &str) -> Command {
    Command::parse(s.split_whitespace().map(|w| w.as_bytes().to_vec()).collect()).unwrap()
}

fn clone_identity(id: &NodeIdentity) -> NodeIdentity {
    NodeIdentity { cert_chain: id.cert_chain.clone(), key: id.key.clone_key(), fingerprint: id.fingerprint }
}

/// A 3-node TLS ring where every node also runs a state-transfer server. Returns
/// the handles, RESP addrs, snapshot-server addrs, a `fetcher` identity allowed
/// on every server, and each server's pinned fingerprint.
async fn spawn_ring_with_snap(
    mode: DeliveryMode,
) -> (
    Vec<ProxyHandle<MemStore>>,
    Vec<SocketAddr>,
    Vec<SocketAddr>,
    NodeIdentity,
    Vec<SpkiFingerprint>,
) {
    let ids: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fps: Vec<_> = ids.iter().map(|i| i.fingerprint).collect();
    let snap_ids: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let snap_fps: Vec<_> = snap_ids.iter().map(|i| i.fingerprint).collect();
    let fetcher = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
    let fetcher_fp = fetcher.fingerprint;

    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let snap_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();

    let mut handles = Vec::new();
    let mut bound_resp = Vec::new();
    let mut snap_ids = snap_ids.into_iter();
    for (i, identity) in ids.into_iter().enumerate() {
        let cfg = ProxyConfig {
            id: i as u8,
            mode,
            issue_initial: i < NUM_ISSUERS,
            resp_listen: resp_addrs[i],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[i],
                successor_addr: ring_addrs[(i + 1) % RING],
                pinned_peer_fingerprints: fps.clone(),
            },
            ring_addrs: vec![],
            snapshot_server: Some(SnapshotServerCfg {
                listen: snap_addrs[i],
                identity: snap_ids.next().unwrap(),
                allowed_fetcher_fingerprints: vec![fetcher_fp],
            }),
            rejoin: None,
        };
        let h = run_proxy_node(cfg, MemStore::new()).await.expect("spawn proxy node");
        bound_resp.push(h.resp_addr);
        handles.push(h);
    }
    (handles, bound_resp, snap_addrs, fetcher, snap_fps)
}

/// A node serves a rejoining peer a snapshot+tail that reproduces its keyspace,
/// and incremental tails thereafter — the survivor side of v2 rejoin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_state_transfer_to_a_fetching_peer() {
    let _ = tracing_subscriber::fmt::try_init();
    let (_handles, resp, snap, fetcher, snap_fps) =
        spawn_ring_with_snap(DeliveryMode::UniformTotalOrder).await;
    // Let the snapshot servers bind their listeners.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Drive writes at node 0; wait until they've replicated (ring delivered).
    let mut c0 = Client::connect(resp[0]).await;
    c0.cmd(&["SET", "a", "1"]).await;
    c0.cmd(&["SET", "b", "2"]).await;
    c0.cmd(&["SADD", "s", "x"]).await;
    await_get(resp[1], "a", b"1").await;

    // Fetch a full transfer (have=0) from node 0's server, retrying the connect
    // until the listener is up.
    let xfer = {
        let mut last = None;
        for _ in 0..30 {
            match fetch_state(snap[0], &fetcher, &[snap_fps[0]], 0).await {
                Ok(x) => {
                    last = Some(x);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        last.expect("state-transfer server never answered")
    };
    assert!(!xfer.snapshot.is_empty(), "have=0 must yield a snapshot");

    // Apply into a fresh rejoiner Replica → it reproduces the served keyspace.
    let mut rejoiner = Replica::new(2, DeliveryMode::UniformTotalOrder, MemStore::new());
    rejoiner.apply_state_transfer(&xfer.snapshot, &xfer.tail).expect("apply transfer");
    assert_eq!(rejoiner.store().query(&pcmd("GET a")), Reply::Bulk(b"1".to_vec()));
    assert_eq!(rejoiner.store().query(&pcmd("GET b")), Reply::Bulk(b"2".to_vec()));
    assert_eq!(rejoiner.store().query(&pcmd("SCARD s")), Reply::Integer(1));
    let have = rejoiner.delivered_index();

    // A post-fetch write must reach the rejoiner via an INCREMENTAL tail (no
    // snapshot), proving the have-index polling path end-to-end.
    c0.cmd(&["SET", "c", "3"]).await;
    await_get(resp[1], "c", b"3").await;
    let mut caught = false;
    for _ in 0..50 {
        let inc = fetch_state(snap[0], &fetcher, &[snap_fps[0]], have).await.expect("incremental fetch");
        assert!(inc.snapshot.is_empty(), "have>0 must carry no snapshot");
        rejoiner.apply_state_transfer(&inc.snapshot, &inc.tail).expect("apply incremental");
        if rejoiner.store().query(&pcmd("GET c")) == Reply::Bulk(b"3".to_vec()) {
            caught = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(caught, "rejoiner caught the post-fetch write via the incremental tail");
}

/// PR-RJ-3c (the E5 t1-rejoin shape, in-process over TLS): a passive node starts
/// OFF the ring, catches up from a survivor's state-transfer server, and tracks
/// continued writes — converging to the survivors' keyspace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passive_rejoiner_converges_off_ring() {
    let _ = tracing_subscriber::fmt::try_init();
    let (_handles, resp, snap, fetcher, snap_fps) =
        spawn_ring_with_snap(DeliveryMode::UniformTotalOrder).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Survivors take a workload; confirm it replicated on the ring.
    let mut c0 = Client::connect(resp[0]).await;
    c0.cmd(&["SET", "a", "1"]).await;
    c0.cmd(&["SET", "b", "2"]).await;
    c0.cmd(&["SADD", "s", "x"]).await;
    await_get(resp[1], "b", b"2").await;

    // Start a passive rejoiner OFF the ring, pointed at node 0's transfer server.
    // Its ring config is a throwaway — passive mode never spawns the transport.
    let throwaway = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
    let rejoiner = run_proxy_node(
        ProxyConfig {
            id: 2,
            mode: DeliveryMode::UniformTotalOrder,
            issue_initial: false,
            resp_listen: pick_port(),
            client_tls: None,
            ring: RingConfig {
                identity: throwaway,
                listen_addr: pick_port(),
                successor_addr: pick_port(),
                pinned_peer_fingerprints: vec![],
            },
            ring_addrs: vec![],
            snapshot_server: None,
            rejoin: Some(trains_valkey::proxy::RejoinCfg {
                survivor_addrs: vec![snap[0]],
                fetch_identity: clone_identity(&fetcher),
                survivor_fingerprints: vec![snap_fps[0]],
                poll_interval: Duration::from_millis(150),
                promote: false, // this test exercises the v2 passive standby
            }),
        },
        MemStore::new(),
    )
    .await
    .expect("spawn passive rejoiner");

    // The rejoiner converges to the survivor's pre-existing keyspace by pulling
    // snapshot + tail (no ring membership).
    await_get(rejoiner.resp_addr, "a", b"1").await;
    await_get(rejoiner.resp_addr, "b", b"2").await;
    await_reply(rejoiner.resp_addr, &["SCARD", "s"], Reply::Integer(1)).await;

    // Writes continue on the survivors; the passive replica must keep up via its
    // incremental tail polling.
    c0.cmd(&["SET", "c", "3"]).await;
    c0.cmd(&["SADD", "s", "y"]).await;
    await_get(rejoiner.resp_addr, "c", b"3").await;
    await_reply(rejoiner.resp_addr, &["SCARD", "s"], Reply::Integer(2)).await;

    // A write sent to the passive replica is rejected (read-only standby).
    let mut rc = Client::connect(rejoiner.resp_addr).await;
    match rc.cmd(&["SET", "x", "1"]).await {
        Reply::Error(e) => assert!(e.to_lowercase().contains("rejoining"), "got: {e}"),
        other => panic!("passive replica must reject writes, got {other:?}"),
    }
}

/// PR-V3-3c: a passive rejoiner, once caught up, PROMOTES to a full acking ring
/// member via the re-admit view change — proven by a write made AFTER promotion
/// reaching it over the ring (it has stopped polling), and by it then accepting
/// writes (no longer a read-only standby).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-transport, timing-sensitive (ring re-formation); run via --ignored or the live bench"]
async fn promoted_rejoiner_becomes_full_acking_member() {
    let _ = tracing_subscriber::fmt::try_init();

    // Three ring identities (node 2 is the rejoiner) + a snapshot identity per
    // survivor. The snapshot servers admit the RING fingerprints, so node 2
    // fetches with its own ring identity.
    let ring_ids: Vec<NodeIdentity> =
        (0..RING).map(|_| NodeIdentity::generate(vec!["localhost".into()]).unwrap()).collect();
    let ring_fps: Vec<_> = ring_ids.iter().map(|i| i.fingerprint).collect();
    let snap_ids: Vec<NodeIdentity> =
        (0..2).map(|_| NodeIdentity::generate(vec!["localhost".into()]).unwrap()).collect();
    let snap_fps: Vec<_> = snap_ids.iter().map(|i| i.fingerprint).collect();

    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let snap_addrs: Vec<SocketAddr> = (0..2).map(|_| pick_port()).collect();

    // Spawn the two survivors (0, 1); node 2 is never started (it's the rejoiner).
    let mut survivors = Vec::new();
    let mut snap_ids_it = snap_ids.into_iter();
    let mut ring_ids_it = ring_ids.iter();
    for i in 0..2usize {
        let identity = clone_identity(ring_ids_it.next().unwrap());
        let cfg = ProxyConfig {
            id: i as u8,
            mode: DeliveryMode::TotalOrder,
            issue_initial: i < NUM_ISSUERS,
            resp_listen: resp_addrs[i],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[i],
                successor_addr: ring_addrs[(i + 1) % RING],
                pinned_peer_fingerprints: ring_fps.clone(),
            },
            ring_addrs: ring_addrs.clone(), // reconfiguration ENABLED
            snapshot_server: Some(SnapshotServerCfg {
                listen: snap_addrs[i],
                identity: snap_ids_it.next().unwrap(),
                allowed_fetcher_fingerprints: ring_fps.clone(),
            }),
            rejoin: None,
        };
        survivors.push(run_proxy_node(cfg, MemStore::new()).await.expect("spawn survivor"));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Exclude the never-started node 2 on both survivors → a working 2-node ring.
    survivors[0].confirm_crash(2).await;
    survivors[1].confirm_crash(2).await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Workload on the survivor ring.
    let mut c0 = Client::connect(survivors[0].resp_addr).await;
    c0.cmd(&["SET", "a", "1"]).await;
    c0.cmd(&["SET", "b", "2"]).await;
    await_get(survivors[1].resp_addr, "a", b"1").await;

    // Bring node 2 up as a PROMOTING rejoiner: catch up from survivor 0, then
    // re-admit to the acking view.
    let rejoiner = run_proxy_node(
        ProxyConfig {
            id: 2,
            mode: DeliveryMode::TotalOrder,
            issue_initial: false,
            resp_listen: resp_addrs[2],
            client_tls: None,
            ring: RingConfig {
                identity: clone_identity(&ring_ids[2]),
                listen_addr: ring_addrs[2],
                successor_addr: ring_addrs[0], // succ(2) = 0
                pinned_peer_fingerprints: ring_fps.clone(),
            },
            ring_addrs: ring_addrs.clone(),
            snapshot_server: None,
            rejoin: Some(trains_valkey::proxy::RejoinCfg {
                survivor_addrs: vec![snap_addrs[0]],
                fetch_identity: clone_identity(&ring_ids[2]),
                survivor_fingerprints: vec![snap_fps[0]],
                poll_interval: Duration::from_millis(150),
                promote: true,
            }),
        },
        MemStore::new(),
    )
    .await
    .expect("spawn promoting rejoiner");

    // It first catches up (passive) to the survivors' keyspace.
    await_get(rejoiner.resp_addr, "a", b"1").await;
    await_get(rejoiner.resp_addr, "b", b"2").await;

    // Wait for PROMOTION. A passive standby rejects writes ("rejoining"); a
    // promoted member accepts them. Poll write-acceptance — this is the barrier
    // between the passive and active phases. (Propagation of a write *originated*
    // on a re-admitted NON-ISSUER — loading pending onto a passing train — is a
    // separate follow-up tracked in docs/PLAN-v3-proxy-promotion-2026-06-16.md;
    // here acceptance is the promotion signal.)
    let mut rc = Client::connect(rejoiner.resp_addr).await;
    let mut promoted = false;
    for _ in 0..100 {
        if let Reply::Simple(_) = rc.cmd(&["SET", "probe", "1"]).await {
            promoted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(promoted, "rejoiner never promoted (kept rejecting writes as a passive standby)");

    // Now a FULL ACKING member. A write made after promotion (it's off the
    // passive poll loop, in the driver loop) reaches it over the RE-FORMED ring —
    // AND reaches the other survivor. In TotalOrder mode the post-re-admit live
    // set is {0,1,2}, so that delivery REQUIRED node 2's ack: it is back in the
    // acking quorum (N-redundancy restored), not just a passive reader.
    c0.cmd(&["SET", "post", "promoted"]).await;
    await_get(rejoiner.resp_addr, "post", b"promoted").await;
    await_get(survivors[1].resp_addr, "post", b"promoted").await;
}
