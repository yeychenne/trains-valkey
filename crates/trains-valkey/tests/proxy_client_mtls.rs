//! R-06: mutual TLS on the RESP client ↔ proxy boundary.
//!
//! These tests bring up a real 3-node TLS ring of proxies whose RESP boundary
//! is mTLS-protected (each node pins an allow-list of client SPKI fingerprints,
//! reusing `trains-net`'s `PinnedFingerprintVerifier`). They assert:
//!   * a client whose cert fingerprint is allow-listed completes the handshake
//!     and its `SET` replicates (acked `+OK`);
//!   * a client whose fingerprint is NOT allow-listed is rejected at the TLS
//!     handshake (no RESP bytes are ever served to it);
//!   * a plaintext client cannot speak RESP to a TLS-enabled boundary.
//!
//! Complements `proxy_tls.rs` (which pins the *ring* boundary, not the client
//! boundary).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use trains_core::DeliveryMode;
use trains_net::{NodeIdentity, PinnedFingerprintVerifier, RingConfig, SpkiFingerprint};
use trains_valkey::proxy::{run_proxy_node, ClientTlsConfig, ProxyConfig, ProxyHandle};
use trains_valkey::MemStore;

const RING: usize = 3;
const NUM_ISSUERS: usize = 2;

/// Install the rustls `ring` crypto provider (idempotent) and init tracing.
/// The test client builds its own rustls `ClientConfig`, which needs the
/// process-level provider; the proxy installs it internally, but the client
/// config is built first.
fn init() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = tracing_subscriber::fmt::try_init();
}

fn pick_port() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn encode(parts: &[&str]) -> Vec<u8> {
    let mut req = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        req.extend_from_slice(p.as_bytes());
        req.extend_from_slice(b"\r\n");
    }
    req
}

/// `NodeIdentity` isn't `Clone`; rebuild one from its DER parts (cert + key).
fn clone_identity(id: &NodeIdentity) -> NodeIdentity {
    NodeIdentity {
        cert_chain: id.cert_chain.clone(),
        key: id.key.clone_key(),
        fingerprint: id.fingerprint,
    }
}

/// Build a TLS connector that presents `client` and pins the server to
/// `server_fp` (reusing the SPKI verifier — it verifies in both directions).
fn connector_for(client: &NodeIdentity, server_fp: SpkiFingerprint) -> TlsConnector {
    let verifier = Arc::new(PinnedFingerprintVerifier::new(vec![server_fp]));
    let cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(client.cert_chain.clone(), client.key.clone_key())
        .expect("client auth cert");
    TlsConnector::from(Arc::new(cfg))
}

async fn tls_connect(
    connector: &TlsConnector,
    addr: SocketAddr,
) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let tcp = TcpStream::connect(addr).await?;
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    connector.connect(server_name, tcp).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_client_with_right_spki_is_acked() {
    init();
    let client = NodeIdentity::generate(vec!["client".to_string()]).unwrap();
    let client_fp = client.fingerprint;

    let (_handles, resp, node0_fp) = spawn_mtls_ring(vec![client_fp]).await;

    let connector = connector_for(&client, node0_fp);
    let mut tls = tls_connect(&connector, resp[0]).await.expect("handshake should succeed");

    tls.write_all(&encode(&["SET", "k", "v"])).await.unwrap();
    tls.flush().await.unwrap();

    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(20), tls.read(&mut buf))
        .await
        .expect("reply timed out")
        .expect("read reply");
    let reply = String::from_utf8_lossy(&buf[..n]);
    assert!(reply.starts_with("+OK"), "expected +OK, got {reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_client_with_wrong_spki_is_rejected() {
    init();
    let allowed = NodeIdentity::generate(vec!["client".to_string()]).unwrap();
    let rogue = NodeIdentity::generate(vec!["rogue".to_string()]).unwrap();

    // Only `allowed`'s fingerprint is on the allow-list; the rogue client is not.
    let (_handles, resp, node0_fp) = spawn_mtls_ring(vec![allowed.fingerprint]).await;

    let connector = connector_for(&rogue, node0_fp);
    // The server's PinnedFingerprintVerifier rejects the rogue client cert, so
    // the handshake (or the first read after it) must fail — never an +OK.
    let outcome = async {
        let mut tls = tls_connect(&connector, resp[0]).await?;
        tls.write_all(&encode(&["SET", "k", "v"])).await?;
        tls.flush().await?;
        let mut buf = vec![0u8; 64];
        let n = tls.read(&mut buf).await?;
        Ok::<_, std::io::Error>(String::from_utf8_lossy(&buf[..n]).into_owned())
    }
    .await;

    match outcome {
        Err(_) => { /* handshake/read failed as required */ }
        Ok(s) => assert!(
            !s.starts_with("+OK"),
            "rogue client must not get an acked write, got {s:?}"
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plaintext_client_rejected_when_tls_on() {
    init();
    let client = NodeIdentity::generate(vec!["client".to_string()]).unwrap();
    let (_handles, resp, _node0_fp) = spawn_mtls_ring(vec![client.fingerprint]).await;

    // A plaintext RESP client against a TLS-enabled boundary: the proxy treats
    // the bytes as a (bad) TLS ClientHello and drops the connection. We must
    // never see a RESP reply.
    let mut plain = TcpStream::connect(resp[0]).await.unwrap();
    plain.write_all(&encode(&["SET", "k", "v"])).await.unwrap();
    plain.flush().await.unwrap();

    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), plain.read(&mut buf))
        .await
        .map(|r| r.unwrap_or(0))
        .unwrap_or(0);
    let reply = String::from_utf8_lossy(&buf[..n]);
    assert!(
        !reply.starts_with("+OK"),
        "plaintext client must not get an acked write, got {reply:?}"
    );
}

/// Like [`spawn_mtls_ring`] but also returns node 0's server SPKI fingerprint so
/// the test client can pin it.
async fn spawn_mtls_ring(
    allowed_client_fps: Vec<SpkiFingerprint>,
) -> (Vec<ProxyHandle<MemStore>>, Vec<SocketAddr>, SpkiFingerprint) {
    let ids: Vec<NodeIdentity> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let node0_fp = ids[0].fingerprint;
    let peer_fps: Vec<_> = ids.iter().map(|i| i.fingerprint).collect();

    let ring_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();
    let resp_addrs: Vec<SocketAddr> = (0..RING).map(|_| pick_port()).collect();

    let mut handles = Vec::new();
    let mut bound_resp = Vec::new();
    for (i, identity) in ids.into_iter().enumerate() {
        let client_tls = Some(ClientTlsConfig {
            identity: clone_identity(&identity),
            allowed_client_fingerprints: allowed_client_fps.clone(),
        });
        let cfg = ProxyConfig {
            id: i as u8,
            mode: DeliveryMode::UniformTotalOrder,
            issue_initial: i < NUM_ISSUERS,
            resp_listen: resp_addrs[i],
            client_tls,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[i],
                successor_addr: ring_addrs[(i + 1) % RING],
                pinned_peer_fingerprints: peer_fps.clone(),
            },
            ring_addrs: vec![],
            snapshot_server: None,
            rejoin: None,
        };
        let h = run_proxy_node(cfg, MemStore::new()).await.expect("spawn proxy node");
        bound_resp.push(h.resp_addr);
        handles.push(h);
    }
    (handles, bound_resp, node0_fp)
}
