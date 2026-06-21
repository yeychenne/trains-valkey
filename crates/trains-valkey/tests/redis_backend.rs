//! PR-RD-4 real-backend validation against a live engine, run locally.
//!
//! These tests spawn a real `valkey-server` / `redis-server` (whichever is on
//! PATH) on an ephemeral loopback port and exercise [`RedisBackend`] against it
//! — directly, and through a 3-node TRAINS ring where each node applies the
//! delivered stream to its own engine. They **skip** (return early) when no
//! engine binary is installed, so CI without one stays green; they run for real
//! on a dev box with Valkey/Redis installed.

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use trains_valkey::store::RedisStore;
use trains_valkey::{Command as RCommand, RedisBackend, Reply};

/// Find an installed engine binary, preferring Valkey (BSD) over Redis.
fn engine_bin() -> Option<&'static str> {
    ["valkey-server", "redis-server"].into_iter().find(|bin| {
        Command::new(bin)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    })
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

/// A spawned engine; killed on drop.
struct Engine {
    child: Child,
    addr: SocketAddr,
}

impl Engine {
    fn spawn(bin: &str) -> Engine {
        let port = free_port();
        let child = Command::new(bin)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--save",
                "", // no RDB persistence
                "--appendonly",
                "no",
                "--protected-mode",
                "no", // loopback-only ephemeral test instance
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn engine");
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        // Own the child immediately so it is killed/waited even if readiness
        // fails (Engine::drop), rather than leaking the process.
        let engine = Engine { child, addr };
        for _ in 0..100 {
            if let Ok(be) = RedisBackend::connect(addr) {
                if be.query(&cmd(&["PING"])) == Reply::Simple("PONG".into()) {
                    return engine;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("engine at {addr} never became ready");
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A UDS-only engine (`port 0` + `unixsocket` + `requirepass`) — the R-07
/// hardened deployment shape. Killed on drop; the socket file is removed.
struct UdsEngine {
    child: Child,
    sock: std::path::PathBuf,
}

impl UdsEngine {
    const PASSWORD: &'static str = "r07-secret";

    fn spawn(bin: &str) -> UdsEngine {
        // A short, unique socket path under the temp dir (UDS paths are length
        // limited — keep it well under ~104 bytes).
        let sock = std::env::temp_dir().join(format!("trains-r07-{}.sock", free_port()));
        let child = Command::new(bin)
            .args([
                "--port",
                "0", // no TCP at all
                "--unixsocket",
                sock.to_str().unwrap(),
                "--unixsocketperm",
                "700",
                "--requirepass",
                Self::PASSWORD,
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn UDS engine");
        let engine = UdsEngine { child, sock: sock.clone() };
        for _ in 0..100 {
            if let Ok(be) = RedisBackend::connect_uds_auth(&sock, Some(Self::PASSWORD)) {
                if be.query(&cmd(&["PING"])) == Reply::Simple("PONG".into()) {
                    return engine;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("UDS engine at {} never became ready", sock.display());
    }
}

impl Drop for UdsEngine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.sock);
    }
}

fn cmd(parts: &[&str]) -> RCommand {
    RCommand::parse(parts.iter().map(|s| s.as_bytes().to_vec()).collect()).unwrap()
}

#[test]
fn redis_backend_apply_query_snapshot() {
    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };
    let e1 = Engine::spawn(bin);
    let mut be = RedisBackend::connect(e1.addr).unwrap();

    // Deterministic commands the proxy would deliver as effects.
    assert_eq!(be.apply(&cmd(&["SET", "a", "1"])), Reply::ok());
    assert_eq!(be.apply(&cmd(&["INCR", "a"])), Reply::Integer(2));
    assert_eq!(be.query(&cmd(&["GET", "a"])), Reply::Bulk(b"2".to_vec()));
    assert_eq!(be.apply(&cmd(&["SADD", "s", "x", "y", "z"])), Reply::Integer(3));
    assert_eq!(be.apply(&cmd(&["SREM", "s", "y"])), Reply::Integer(1)); // SPOP effect
    assert_eq!(be.apply(&cmd(&["HSET", "h", "f", "v"])), Reply::Integer(1));

    // Snapshot → import into a fresh engine → identical observable state.
    let snap = be.export_snapshot();
    let e2 = Engine::spawn(bin);
    let mut be2 = RedisBackend::connect(e2.addr).unwrap();
    be2.import_snapshot(&snap).unwrap();

    assert_eq!(be2.query(&cmd(&["GET", "a"])), Reply::Bulk(b"2".to_vec()));
    assert_eq!(be2.query(&cmd(&["SCARD", "s"])), Reply::Integer(2));
    assert_eq!(be2.query(&cmd(&["SISMEMBER", "s", "x"])), Reply::Integer(1));
    assert_eq!(be2.query(&cmd(&["SISMEMBER", "s", "y"])), Reply::Integer(0));
    assert_eq!(be2.query(&cmd(&["HGET", "h", "f"])), Reply::Bulk(b"v".to_vec()));
}

#[test]
fn redis_backend_uds_with_requirepass() {
    let Some(bin) = engine_bin() else {
        eprintln!("SKIP: no valkey-server/redis-server on PATH");
        return;
    };
    let e = UdsEngine::spawn(bin);

    // Right password over the UDS: apply/query work.
    let mut be = RedisBackend::connect_uds_auth(&e.sock, Some(UdsEngine::PASSWORD)).unwrap();
    assert_eq!(be.apply(&cmd(&["SET", "a", "1"])), Reply::ok());
    assert_eq!(be.query(&cmd(&["GET", "a"])), Reply::Bulk(b"1".to_vec()));

    // No password: the engine has `requirepass`, so commands are denied
    // (NOAUTH) — proving the socket is not an unauthenticated open door.
    let unauth = RedisBackend::connect_uds(&e.sock).unwrap();
    match unauth.query(&cmd(&["GET", "a"])) {
        Reply::Error(msg) => assert!(
            msg.to_uppercase().contains("NOAUTH") || msg.to_uppercase().contains("AUTH"),
            "expected a NOAUTH-style error, got {msg:?}"
        ),
        other => panic!("unauthenticated UDS query must be denied, got {other:?}"),
    }

    // Wrong password: connect_uds_auth surfaces the AUTH failure.
    let bad = RedisBackend::connect_uds_auth(&e.sock, Some("wrong"));
    assert!(bad.is_err(), "wrong password must fail the AUTH handshake");
}

// ── 3-node ring, each node backed by a real engine ───────────────────────────

mod ring {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use trains_core::DeliveryMode;
    use trains_net::{NodeIdentity, RingConfig};
    use trains_valkey::proxy::{run_proxy_node, ProxyConfig, ProxyHandle};

    const RING: usize = 3;
    const NUM_ISSUERS: usize = 2;

    fn pick() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    }

    /// Send one RESP command to a proxy and read the (small, scalar) reply line.
    async fn proxy_set(addr: SocketAddr, parts: &[&str]) {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let mut req = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        s.write_all(&req).await.unwrap();
        s.flush().await.unwrap();
        let mut buf = [0u8; 64];
        let _ = tokio::time::timeout(Duration::from_secs(20), s.read(&mut buf))
            .await
            .expect("proxy reply timeout")
            .expect("proxy reply read");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn three_node_ring_replicates_to_real_engines() {
        let Some(bin) = engine_bin() else {
            eprintln!("SKIP: no valkey-server/redis-server on PATH");
            return;
        };

        // One engine per node.
        let engines: Vec<Engine> = (0..RING).map(|_| Engine::spawn(bin)).collect();

        let ids: Vec<NodeIdentity> = (0..RING)
            .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
            .collect();
        let fps: Vec<_> = ids.iter().map(|i| i.fingerprint).collect();
        let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick()).collect();
        let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick()).collect();

        let mut handles: Vec<ProxyHandle<RedisBackend>> = Vec::new();
        for (i, identity) in ids.into_iter().enumerate() {
            let backend = RedisBackend::connect(engines[i].addr).unwrap();
            let cfg = ProxyConfig {
                id: i as u8,
                mode: DeliveryMode::UniformTotalOrder,
                issue_initial: i < NUM_ISSUERS,
                resp_listen: resp_addrs[i],
                client_tls: None,
                ring: RingConfig {
                    identity,
                    listen_addr: ring_addrs[i],
                    successor_addr: ring_addrs[(i + 1) % RING],
                    pinned_peer_fingerprints: fps.clone(),
                },
                ring_addrs: vec![], // reconfiguration off for this convergence test
                snapshot_server: None,
                rejoin: None,
            };
            handles.push(run_proxy_node(cfg, backend).await.unwrap());
        }

        tokio::time::sleep(Duration::from_millis(700)).await;

        // Writes through node 0 (incl. an SPOP effect path) and node 1.
        proxy_set(resp_addrs[0], &["SET", "user:1", "alice"]).await;
        proxy_set(resp_addrs[1], &["SET", "user:2", "bob"]).await;
        proxy_set(resp_addrs[0], &["SADD", "team", "a", "b", "c"]).await;
        proxy_set(resp_addrs[0], &["INCR", "count"]).await;

        // Assert every node's REAL engine converged (read engines directly).
        for (i, e) in engines.iter().enumerate() {
            let be = RedisBackend::connect(e.addr).unwrap();
            await_engine(&be, &["GET", "user:1"], Reply::Bulk(b"alice".to_vec()), i).await;
            await_engine(&be, &["GET", "user:2"], Reply::Bulk(b"bob".to_vec()), i).await;
            await_engine(&be, &["SCARD", "team"], Reply::Integer(3), i).await;
            await_engine(&be, &["GET", "count"], Reply::Bulk(b"1".to_vec()), i).await;
        }

        drop(handles);
    }

    async fn await_engine(be: &RedisBackend, parts: &[&str], expected: Reply, node: usize) {
        for _ in 0..100 {
            if be.query(&cmd(parts)) == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node {node} engine never reached {parts:?} == {expected:?}");
    }

    /// G5: the chaos workload + no-acked-write-loss verifier against a healthy
    /// 3-node real-Valkey ring (the masked-crash variant is the EC2 run; here we
    /// validate the driver's load + verify logic end-to-end).
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn chaos_driver_load_and_verify_no_loss() {
        use trains_valkey::chaos::run_load;

        let Some(bin) = engine_bin() else {
            eprintln!("SKIP: no valkey-server/redis-server on PATH");
            return;
        };
        let engines: Vec<Engine> = (0..RING).map(|_| Engine::spawn(bin)).collect();
        let engine_addrs: Vec<SocketAddr> = engines.iter().map(|e| e.addr).collect();

        let ids: Vec<NodeIdentity> = (0..RING)
            .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
            .collect();
        let fps: Vec<_> = ids.iter().map(|i| i.fingerprint).collect();
        let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick()).collect();
        let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick()).collect();

        let mut handles = Vec::new();
        for (i, identity) in ids.into_iter().enumerate() {
            let backend = RedisBackend::connect(engines[i].addr).unwrap();
            let cfg = ProxyConfig {
                id: i as u8,
                mode: DeliveryMode::UniformTotalOrder,
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
                snapshot_server: None,
                rejoin: None,
            };
            handles.push(run_proxy_node(cfg, backend).await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(700)).await;

        // Drive the load (sync) off the async runtime; verify against all engines.
        let node0 = resp_addrs[0];
        let acked = tokio::task::spawn_blocking(move || run_load(node0, "chaos:k", 0..40, None))
            .await
            .unwrap()
            .expect("run_load");
        assert_eq!(acked.len(), 40, "all writes acked on a healthy ring");

        // Allow replication to quiesce, then verify no loss + convergence.
        let report = loop_verify(&engine_addrs, &acked).await;
        assert!(report.ok(), "chaos verify failed: {report:?}");
        assert_eq!(report.acked_loss.len(), 0);
        assert!(report.dbsizes.iter().all(|&d| d == 40), "each engine holds 40 keys: {report:?}");

        drop(handles);
    }

    async fn loop_verify(
        engines: &[SocketAddr],
        acked: &trains_valkey::chaos::AckedWrites,
    ) -> trains_valkey::chaos::VerifyReport {
        for _ in 0..50 {
            let engines = engines.to_vec();
            let acked = acked.clone();
            let report = tokio::task::spawn_blocking(move || {
                trains_valkey::chaos::verify(&engines, None, &acked)
            })
            .await
            .unwrap()
            .expect("verify");
            if report.ok() {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("ring never converged within budget");
    }
}
