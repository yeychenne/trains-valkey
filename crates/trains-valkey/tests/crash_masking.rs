//! PR-RD-4 (in-process): a permanent crash is **masked** under the proxy.
//!
//! Three TLS proxy nodes form a reconfiguration-enabled ring. After some writes
//! replicate, a non-issuer node is crashed (its ring transport aborted). The
//! coordinator is notified; the [`ViewChange`](trains_recovery::view_change) token
//! circulates over `vc_outbox`/`vc_inbox` (the other survivor learns the crash
//! from the token), the victim is excluded and the ring re-formed — and
//! **post-crash writes still commit on the survivors**, with no acked-write
//! loss. This is the in-process analogue of `trains-cli`'s
//! `reconfig_wire_integration`, exercised end-to-end through the RESP proxy.
//!
//! Out of scope here (operator-gated, needs a live `redis-server` + EC2): the
//! real-backend `fis-kill` chaos run (rest of PR-RD-4).

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use trains_core::DeliveryMode;
use trains_net::{NodeIdentity, RingConfig};
use trains_valkey::proxy::{run_proxy_node, ProxyConfig, ProxyHandle};
use trains_valkey::{MemStore, Reply};

const RING: usize = 3;
const NUM_ISSUERS: usize = 2;

fn pick_port() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

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
                    let mut buf = vec![0u8; n as usize + 2];
                    r.read_exact(&mut buf).await?;
                    buf.truncate(n as usize);
                    Reply::Bulk(buf)
                }
            }
            other => Reply::error(format!("ERR unexpected reply marker {other}")),
        })
    })
}

/// Bring up a reconfiguration-enabled 3-node TLS ring of proxies.
async fn spawn_reconfig_ring() -> (Vec<ProxyHandle<MemStore>>, Vec<SocketAddr>) {
    let ids: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fps: Vec<_> = ids.iter().map(|i| i.fingerprint).collect();
    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();

    let mut handles = Vec::new();
    let mut bound = Vec::new();
    for (i, identity) in ids.into_iter().enumerate() {
        let cfg = ProxyConfig {
            id: i as u8,
            mode: DeliveryMode::TotalOrder, // crash-masking mode
            issue_initial: i < NUM_ISSUERS,
            resp_listen: resp_addrs[i],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[i],
                successor_addr: ring_addrs[(i + 1) % RING],
                pinned_peer_fingerprints: fps.clone(),
            },
            ring_addrs: ring_addrs.clone(), // reconfiguration ENABLED
            snapshot_server: None,
            rejoin: None,
        };
        let h = run_proxy_node(cfg, MemStore::new()).await.expect("spawn proxy");
        bound.push(h.resp_addr);
        handles.push(h);
    }
    (handles, bound)
}

async fn await_reply(addr: SocketAddr, parts: &[&str], expected: Reply) {
    let mut client = Client::connect(addr).await;
    for _ in 0..200 {
        if client.cmd(parts).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node at {addr} never reached {parts:?} == {expected:?}");
}

/// Retry an idempotent `SET` until it commits (returns `+OK`), giving the view
/// change time to settle even under heavy parallel-test CPU contention. `SET`
/// is idempotent, so retrying a cancelled-but-applied attempt is safe — unlike
/// `INCR`, which is why this test uses only `SET`s on the masked path.
async fn set_until_ok(addr: SocketAddr, key: &str, val: &str) {
    for _ in 0..120 {
        let mut c = Client::connect(addr).await;
        let got = tokio::time::timeout(Duration::from_secs(3), c.cmd(&["SET", key, val])).await;
        if matches!(got, Ok(Reply::Simple(ref s)) if s == "OK") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("SET {key} never committed at {addr} — crash not masked within budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn permanent_crash_is_masked_under_the_proxy() {
    let _ = tracing_subscriber::fmt::try_init();
    let (handles, resp) = spawn_reconfig_ring().await;

    // Let the ring form.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── Pre-crash writes at node 0; confirm they replicate to node 1. ──
    set_until_ok(resp[0], "k1", "v1").await;
    set_until_ok(resp[0], "k2", "v2").await;
    await_reply(resp[1], &["GET", "k1"], Reply::Bulk(b"v1".to_vec())).await;

    // ── Crash node 2 (a non-issuer): abort its ring transport. ──
    let victim = 2u8;
    handles[victim as usize].crash().await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Notify the coordinator (lowest-id survivor = node 0). Node 1 learns the
    // crash from the circulating Gather token and excludes the victim too.
    handles[0].confirm_crash(victim).await;
    // Let the view change circulate (gather → plan → install) + reissue.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // ── Post-crash writes MUST still commit on the survivors (masked crash). ──
    // A committed write is itself evidence of masking: it could only return +OK
    // by being delivered/acked across the re-formed surviving live set {0,1}.
    // Idempotent SETs (retryable under contention; INCR would not be).
    set_until_ok(resp[0], "k3", "v3").await;
    set_until_ok(resp[0], "k4", "v4").await;

    // Both survivors converge and hold all writes (pre- and post-crash).
    for &addr in &[resp[0], resp[1]] {
        await_reply(addr, &["GET", "k1"], Reply::Bulk(b"v1".to_vec())).await;
        await_reply(addr, &["GET", "k2"], Reply::Bulk(b"v2".to_vec())).await;
        await_reply(addr, &["GET", "k3"], Reply::Bulk(b"v3".to_vec())).await;
        await_reply(addr, &["GET", "k4"], Reply::Bulk(b"v4".to_vec())).await;
    }

    // Keep the (crashed) victim handle alive until the end so Drop doesn't race.
    drop(handles);
}
