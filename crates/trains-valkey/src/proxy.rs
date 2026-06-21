//! Async RESP proxy server: the runnable front door of a TRAINS-replicated
//! Redis node.
//!
//! This is the I/O glue around [`crate::replica`]'s logic. Each node:
//!   * accepts RESP client connections on a TCP port;
//!   * answers **reads** from the local store (no ring traffic);
//!   * forwards **writes** to a single *driver* task that owns the `trains-core`
//!     kernel, resolves non-deterministic commands to deterministic effects
//!     (PR-RD-2), `oBroadcast`s them, and — when delivered back in total order —
//!     applies them to the local store (deduped by `(origin, request_id)`,
//!     PR-RD-3) and wakes the originating client. Non-origin nodes apply
//!     silently.
//!
//! ## Reconfiguration / crash masking (PR-RD-4)
//! When the full ring topology is supplied (`ProxyConfig::ring_addrs`), the
//! driver also runs the distributed view change — mirroring `trains-cli`'s
//! `node::run`: the ◇S [`FailureDetector`] confirms a crash (clock-gap hints or
//! a successor-unreachable signal), the node excludes the victim (freeze +
//! `confirm_crash` + retarget past it), and the [`ViewChange`] token protocol
//! circulates Gather/Install frames over `vc_outbox`/`vc_inbox` to recover and
//! reissue — *masking* the crash so survivors keep serving with no acked-write
//! loss. Without `ring_addrs` the proxy keeps the simple RD-1..3 behaviour
//! (no reconfiguration). The real `redis-server` backend behind the
//! [`crate::store::RedisStore`] seam and the EC2 `fis-kill` chaos run are the
//! remaining RD-4 items (operator-gated).

use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use trains_recovery::failure_detector::FailureDetector;
use trains_recovery::view_change::{VcAction, ViewChange};
use trains_core::{
    DeliveryMode, Input, Output, ProcId, StateSnapshot, Train, TrainsNode, NUM_TRAINS, RING_SIZE,
};
use trains_net::{
    fetch_state, serve_snapshots, NodeIdentity, PinnedFingerprintVerifier, RingConfig,
    RingTransport, SnapshotRequest, SpkiFingerprint, StateTransfer, ViewChangeMsg,
};

use crate::classify::{classify, Class};
use crate::command::{Command, WriteOp};
use crate::delivered_log::{DeliveredEntry, DeliveredLog};
use crate::effect::{self, Resolution};
use crate::replica::{
    apply_delivered_op_parts, build_state_transfer_lazy, ReplicaSnapshot, ViewInfo, WriteDedup,
    SNAPSHOT_VERSION,
};
use crate::resp::{Reply, RespDecoder};
use crate::store::{RedisStore, SnapshotError};

const CMD_CHANNEL_CAP: usize = 256;
const READ_CHUNK: usize = 8 * 1024;
/// Clock-gap hints required before a crash is confirmed (◇S detector) — matches
/// `trains-cli`'s production binary.
const STRIKE_THRESHOLD: u32 = 3;
/// A successor disconnect is strong evidence — confirm immediately.
const DISCONNECT_WEIGHT: u32 = STRIKE_THRESHOLD;

// ─── Listener backpressure (R-02, PR-SEC-B) ─────────────────────────────────
//
// Two complementary caps on the RESP listener, both enforced *before* a
// client_loop task is spawned:
//
//   * MAX_CLIENT_CONNS — global semaphore. Bounds the total work-in-flight on
//     this node, regardless of source. Picked to be 4× the typical
//     `CMD_CHANNEL_CAP` so that even all-connections-slow can't fill the
//     command queue beyond what one driver iteration can drain.
//   * PER_IP_CONN_CAP — per-IP counter. Stops a single noisy client from
//     consuming all of MAX_CLIENT_CONNS. 32 lets the integration tests open a
//     reasonable concurrent fan-out from 127.0.0.1 without tripping the cap.
//
// Connections that fail either check get a short RESP error and are closed
// immediately — no client_loop task is spawned for them. Successful admits
// hold a [`ConnGuard`] whose `Drop` releases the permit AND decrements the
// per-IP counter, so the caps recover cleanly when clients disconnect or
// client_loop returns.

/// Maximum concurrent RESP client connections per node.
pub const MAX_CLIENT_CONNS: usize = 512;

/// Maximum concurrent RESP client connections from a single source IP.
pub const PER_IP_CONN_CAP: u32 = 32;

/// RESP error wire-bytes sent to a rejected connection before close.
const BUSY_REPLY: &[u8] = b"-ERR busy: server connection cap reached\r\n";

/// Per-IP counter map plus a `tokio::sync::Semaphore` for the global cap.
/// Cheap to clone (both fields are `Arc<...>`).
#[derive(Clone)]
struct ConnGate {
    sem: Arc<Semaphore>,
    per_ip: Arc<Mutex<HashMap<IpAddr, u32>>>,
}

impl ConnGate {
    fn new(max_conns: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(max_conns)),
            per_ip: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Try to admit a connection from `ip`. Returns `None` if either the
    /// global semaphore is full OR the per-IP cap is reached. The returned
    /// guard MUST be held for the lifetime of the client_loop task — its
    /// `Drop` releases both holds.
    fn try_admit(&self, ip: IpAddr) -> Option<ConnGuard> {
        let permit = self.sem.clone().try_acquire_owned().ok()?;
        let mut m = self.per_ip.lock().expect("per-IP map poisoned");
        let entry = m.entry(ip).or_insert(0);
        if *entry >= PER_IP_CONN_CAP {
            return None; // permit drops, semaphore releases on its own
        }
        *entry += 1;
        Some(ConnGuard {
            _permit: permit,
            ip,
            per_ip: self.per_ip.clone(),
        })
    }

    #[cfg(test)]
    fn observed_for(&self, ip: IpAddr) -> u32 {
        *self.per_ip.lock().unwrap().get(&ip).unwrap_or(&0)
    }
}

/// RAII guard returned by [`ConnGate::try_admit`]. Releases the semaphore
/// permit and decrements the per-IP counter on drop. The permit is held
/// inside the guard solely for its `Drop`; do not access it directly.
struct ConnGuard {
    _permit: OwnedSemaphorePermit,
    ip: IpAddr,
    per_ip: Arc<Mutex<HashMap<IpAddr, u32>>>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut m = self.per_ip.lock().expect("per-IP map poisoned");
        if let Some(c) = m.get_mut(&self.ip) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                m.remove(&self.ip);
            }
        }
    }
}

/// Mutual-TLS configuration for the RESP **client** boundary (R-06, T-tr-01 /
/// T-tr-18 / T-tr-18b).
///
/// When present on [`ProxyConfig::client_tls`], the proxy wraps every accepted
/// RESP connection in a rustls server handshake that requires the client to
/// present a certificate whose SPKI fingerprint is in `allowed_client_fingerprints`
/// — reusing the same [`PinnedFingerprintVerifier`] the ring transport uses for
/// peer pinning. Without it the RESP boundary is plaintext (any local actor can
/// speak RESP as any client); the binary warns loudly at startup in that case.
pub struct ClientTlsConfig {
    /// This proxy's server identity presented to RESP clients.
    pub identity: NodeIdentity,
    /// SPKI fingerprints of the client certificates allowed to connect.
    pub allowed_client_fingerprints: Vec<SpkiFingerprint>,
}

/// Configuration for one replicated-Redis proxy node.
pub struct ProxyConfig {
    /// Ring node id (`0..RING_SIZE`).
    pub id: ProcId,
    /// Delivery mode. Defaults to [`DeliveryMode::TotalOrder`] (equivalent to
    /// UTO in a healthy ring, and the mode crash masking needs).
    pub mode: DeliveryMode,
    /// Issue an initial train at startup (set on issuer nodes only).
    pub issue_initial: bool,
    /// Address to accept RESP client connections on (port 0 ⇒ ephemeral).
    pub resp_listen: SocketAddr,
    /// Optional mutual TLS on the RESP client boundary (R-06). `None` ⇒
    /// plaintext RESP (loopback-bench default; a loud startup warning fires).
    pub client_tls: Option<ClientTlsConfig>,
    /// TLS ring transport configuration.
    pub ring: RingConfig,
    /// Full ring topology (addresses indexed by node id) for reconfiguration.
    /// When non-empty (length == ring size) the proxy masks a crashed node via
    /// the distributed view change. Empty ⇒ reconfiguration disabled (RD-1..3
    /// behaviour).
    pub ring_addrs: Vec<SocketAddr>,
    /// Optional state-transfer server (PR-RJ-3b): when set, this node serves
    /// `fetch_state` requests (snapshot + delivered-effect tail) to rejoining
    /// peers over pinned mutual TLS. `None` ⇒ no server (RD-1..4 behaviour).
    pub snapshot_server: Option<SnapshotServerCfg>,
    /// Optional passive-rejoin mode (PR-RJ-3c): when set, this node does NOT join
    /// the ring. It catches up from a survivor's state-transfer server and tails
    /// it to stay current (a read-only passive replica — the v2 rejoin path). The
    /// ring transport and driver are not started; client reads are served from
    /// the catching-up store and client writes are rejected. `None` ⇒ normal ring
    /// participation.
    pub rejoin: Option<RejoinCfg>,
}

/// Passive-rejoin settings (PR-RJ-3c). A restarted node uses these to catch up
/// off-ring from one or more survivors and stay current by polling.
pub struct RejoinCfg {
    /// Survivors' state-transfer server addresses, tried in order each round.
    pub survivor_addrs: Vec<SocketAddr>,
    /// This rejoiner's TLS identity (must be allow-listed on the survivors'
    /// `SnapshotServerCfg::allowed_fetcher_fingerprints`).
    pub fetch_identity: NodeIdentity,
    /// Pinned SPKI fingerprints of the survivors' state-transfer servers.
    pub survivor_fingerprints: Vec<SpkiFingerprint>,
    /// How often to poll a survivor for a fresh delivered-effect tail.
    pub poll_interval: std::time::Duration,
    /// v3 promotion (PR-V3-3c): once caught up, rejoin the ring as a FULL acking
    /// member via the re-admit view change (restoring N-redundancy). `false`
    /// (the default for the live-validated v2 path) keeps it a read-only passive
    /// standby forever — promotion is strictly opt-in so v2 is untouched.
    pub promote: bool,
}

/// State-transfer server settings (PR-RJ-3b). A returning replica fetches a
/// snapshot + tail from this server to catch up (the rejoiner side is PR-RJ-3c).
pub struct SnapshotServerCfg {
    /// Address to serve state transfers on (its own port, distinct from RESP and
    /// the ring). Use a concrete port so peers can reach it.
    pub listen: SocketAddr,
    /// This server's TLS identity presented to fetching peers.
    pub identity: NodeIdentity,
    /// SPKI fingerprints of the peers allowed to fetch state (the ring members
    /// that may rejoin through this node).
    pub allowed_fetcher_fingerprints: Vec<SpkiFingerprint>,
}

/// Build a rustls [`TlsAcceptor`] that presents `identity` and requires a pinned
/// client certificate. Mirrors the ring transport's server-side config
/// (`trains-net::transport`) so the client boundary is pinned exactly like the
/// peer boundary.
fn build_client_acceptor(cfg: &ClientTlsConfig) -> anyhow::Result<TlsAcceptor> {
    let verifier = Arc::new(PinnedFingerprintVerifier::new(
        cfg.allowed_client_fingerprints.clone(),
    ));
    let server_cfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            cfg.identity.cert_chain.clone(),
            cfg.identity.key.clone_key(),
        )
        .map_err(|e| anyhow::anyhow!("building client-boundary TLS server config: {e}"))?;
    Ok(TlsAcceptor::from(Arc::new(server_cfg)))
}

/// A request from a client connection to the driver: broadcast this write and
/// send the resulting reply back through `reply`.
struct WriteRequest {
    argv: Vec<Vec<u8>>,
    reply: oneshot::Sender<Reply>,
}

/// What the driver does with a [`WriteRequest`] after classification/resolution.
enum WriteAction {
    /// Answer the client now (no broadcast): empty pop, type error, non-write.
    Immediate(Reply),
    /// Broadcast this effect; `Some(reply)` is the origin-resolved client reply
    /// (PR-RD-2), `None` means return the apply result (deterministic write).
    Broadcast(Vec<Vec<u8>>, Option<Reply>),
}

/// Out-of-band control to the driver (test/operator hooks for PR-RD-4).
enum DriverCtrl {
    /// Simulate a permanent crash: abort the ring transport and stop the driver.
    Crash,
    /// Operator/failure-detector notification that `victim` has crashed — begin
    /// the view change (the production path also reaches this via the ◇S
    /// detector on clock gaps / successor-unreachable).
    ConfirmCrash(ProcId),
}

/// Handle to a running proxy node. Dropping it aborts the node's tasks.
pub struct ProxyHandle<S: RedisStore> {
    /// The actually-bound RESP listen address (useful when `resp_listen` used
    /// port 0).
    pub resp_addr: SocketAddr,
    /// The shared local store (read-locked by client reads, write-locked by the
    /// driver's apply). Exposed so tests can assert converged state directly.
    pub store: Arc<Mutex<S>>,
    ctrl_tx: mpsc::Sender<DriverCtrl>,
    tasks: Vec<JoinHandle<()>>,
}

impl<S: RedisStore> ProxyHandle<S> {
    /// Simulate a permanent crash of this node: the driver aborts its ring
    /// transport (so the predecessor sees it unreachable) and stops. Used by
    /// crash-masking tests / operator drills.
    pub async fn crash(&self) {
        let _ = self.ctrl_tx.send(DriverCtrl::Crash).await;
    }

    /// Notify this node that `victim` has crashed — it begins the view change.
    /// (The production path also reaches this automatically via the failure
    /// detector; this hook lets a test/operator trigger it deterministically.)
    pub async fn confirm_crash(&self, victim: ProcId) {
        let _ = self.ctrl_tx.send(DriverCtrl::ConfirmCrash(victim)).await;
    }

    /// Abort the node's background tasks (listener + driver).
    pub fn shutdown(self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl<S: RedisStore> Drop for ProxyHandle<S> {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// Start a proxy node: spawn the ring transport, the RESP listener, and the
/// driver task. Returns once the node is accepting connections.
pub async fn run_proxy_node<S>(cfg: ProxyConfig, store: S) -> anyhow::Result<ProxyHandle<S>>
where
    S: RedisStore + Send + 'static,
{
    if !cfg.ring_addrs.is_empty() && cfg.ring_addrs.len() != RING_SIZE {
        anyhow::bail!(
            "ring_addrs length {} != RING_SIZE {}",
            cfg.ring_addrs.len(),
            RING_SIZE
        );
    }

    // Build the client-boundary TLS acceptor before binding anything, so a bad
    // identity/cert fails startup rather than the first connection (R-06).
    let acceptor = match &cfg.client_tls {
        Some(c) => Some(build_client_acceptor(c)?),
        None => None,
    };

    let listener = TcpListener::bind(cfg.resp_listen).await?;
    let resp_addr = listener.local_addr()?;

    let store = Arc::new(Mutex::new(store));
    let (cmd_tx, cmd_rx) = mpsc::channel::<WriteRequest>(CMD_CHANNEL_CAP);
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<DriverCtrl>(8);

    let gate = ConnGate::new(MAX_CLIENT_CONNS);
    let listener_task = tokio::spawn(listener_loop(listener, acceptor, store.clone(), cmd_tx, gate));

    let mut tasks = vec![listener_task];

    // Passive rejoin (PR-RJ-3c): catch up off-ring from a survivor and tail it.
    // No ring transport, no driver, no state-transfer server while passive — the
    // node is a read-only standby. With `rejoin.promote` (PR-V3-3c) it then joins
    // the ring as a full acking member; without it, it stays passive forever (v2).
    if let Some(rejoin) = cfg.rejoin {
        let (ring, did, mode, ring_addrs) = (cfg.ring, cfg.id, cfg.mode, cfg.ring_addrs.clone());
        let store2 = store.clone();
        tasks.push(tokio::spawn(async move {
            let mut cmd_rx = cmd_rx;
            // `None` ⇒ the command channel closed (shutdown); nothing to do.
            if let Some(seed) = passive_catch_up(store2.clone(), &rejoin, &mut cmd_rx).await {
                match RingTransport::spawn(ring).await {
                    Ok(transport) => {
                        tracing::info!(id = did, "promoted — joining the ring as a full acking member");
                        driver_loop(
                            DriverCfg { id: did, mode, issue_initial: false, ring_addrs },
                            transport, store2, cmd_rx, ctrl_rx, None, Some(seed),
                        )
                        .await;
                    }
                    Err(e) => {
                        // v2 safety net: a failed promotion reverts to the proven
                        // passive standby rather than dropping the node.
                        tracing::error!(error = %e, "promotion transport spawn failed; staying passive");
                        let mut rejoin = rejoin;
                        rejoin.promote = false;
                        let _ = passive_catch_up(store2, &rejoin, &mut cmd_rx).await;
                    }
                }
            }
        }));
        return Ok(ProxyHandle { resp_addr, store, ctrl_tx, tasks });
    }

    let transport = RingTransport::spawn(cfg.ring).await?;

    // State-transfer server (PR-RJ-3b): serve snapshot + tail to rejoining peers.
    // The driver services each request from its live state via `snap_req_rx`.
    let snap_req_rx = match cfg.snapshot_server {
        Some(s) => {
            let (tx, rx) = mpsc::channel::<SnapshotRequest>(8);
            tasks.push(tokio::spawn(async move {
                if let Err(e) =
                    serve_snapshots(s.listen, s.identity, s.allowed_fetcher_fingerprints, tx).await
                {
                    tracing::warn!(error = %e, "state-transfer server stopped");
                }
            }));
            Some(rx)
        }
        None => None,
    };

    let driver_task = tokio::spawn(driver_loop(
        DriverCfg {
            id: cfg.id,
            mode: cfg.mode,
            issue_initial: cfg.issue_initial,
            ring_addrs: cfg.ring_addrs,
        },
        transport,
        store.clone(),
        cmd_rx,
        ctrl_rx,
        snap_req_rx,
        None, // not a promotion — a normal cold-start ring member
    ));
    tasks.push(driver_task);

    Ok(ProxyHandle {
        resp_addr,
        store,
        ctrl_tx,
        tasks,
    })
}

/// Accept RESP client connections; one task per connection.
///
/// Backpressure (R-02 / PR-SEC-B): before spawning the per-connection task,
/// the listener tries to admit the connection through [`ConnGate`]. Failure
/// (global cap or per-IP cap reached) drops the connection with a short
/// `-ERR busy` reply. Successful admit returns a [`ConnGuard`] that the
/// spawned task owns and drops on exit.
///
/// Client mTLS (R-06): when `acceptor` is `Some`, each admitted connection is
/// run through a rustls server handshake (pinned client cert) before any RESP
/// bytes are read; a handshake failure drops the connection. When `None`, the
/// connection is served in plaintext.
async fn listener_loop<S: RedisStore + Send + 'static>(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    store: Arc<Mutex<S>>,
    cmd_tx: mpsc::Sender<WriteRequest>,
    gate: ConnGate,
) {
    loop {
        match listener.accept().await {
            Ok((mut sock, peer)) => {
                let guard = match gate.try_admit(peer.ip()) {
                    Some(g) => g,
                    None => {
                        tracing::debug!(%peer, "RESP connection refused — cap reached");
                        // Plaintext busy reply: pre-TLS, a refused client simply
                        // sees a handshake/connection failure, which is fine.
                        let _ = sock.write_all(BUSY_REPLY).await;
                        let _ = sock.shutdown().await;
                        continue;
                    }
                };
                tracing::debug!(%peer, "RESP client connected");
                let store = store.clone();
                let cmd_tx = cmd_tx.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    match acceptor {
                        Some(acc) => match acc.accept(sock).await {
                            Ok(tls) => client_loop(tls, store, cmd_tx).await,
                            Err(e) => {
                                tracing::debug!(%peer, error = %e, "RESP client TLS handshake failed");
                            }
                        },
                        None => client_loop(sock, store, cmd_tx).await,
                    }
                    drop(guard); // explicit: release permit + per-IP slot
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "RESP accept failed");
                return;
            }
        }
    }
}

/// Serve one RESP client connection.
///
/// Generic over the byte stream so the same loop serves a plaintext
/// [`TcpStream`] or a rustls-wrapped TLS stream (R-06) with no duplication.
async fn client_loop<S, IO>(
    mut sock: IO,
    store: Arc<Mutex<S>>,
    cmd_tx: mpsc::Sender<WriteRequest>,
) where
    S: RedisStore + Send + 'static,
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut decoder = RespDecoder::new();
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        let n = match sock.read(&mut buf).await {
            Ok(0) => return, // client closed
            Ok(n) => n,
            Err(e) => {
                tracing::debug!(error = %e, "RESP read error; closing connection");
                return;
            }
        };
        decoder.feed(&buf[..n]);

        loop {
            let argv = match decoder.next_command() {
                Ok(Some(argv)) => argv,
                Ok(None) => break, // need more bytes
                Err(e) => {
                    let _ = sock
                        .write_all(&Reply::error(format!("ERR Protocol error: {e}")).to_bytes())
                        .await;
                    return; // protocol desync — drop the connection
                }
            };
            if argv.is_empty() {
                continue;
            }
            let cmd = match Command::parse(argv) {
                Some(c) => c,
                None => continue,
            };

            let reply = match classify(&cmd.name) {
                Class::Read => {
                    let g = store.lock().expect("store mutex poisoned");
                    g.query(&cmd)
                }
                // Both deterministic writes and non-deterministic mutations go to
                // the driver: it owns the kernel, and non-deterministic effect
                // resolution must read committed state serialized with applies.
                Class::Write | Class::NonDeterministic => {
                    let (tx, rx) = oneshot::channel();
                    let req = WriteRequest { argv: cmd.argv.clone(), reply: tx };
                    if cmd_tx.send(req).await.is_err() {
                        Reply::error("ERR replication driver unavailable")
                    } else {
                        rx.await
                            .unwrap_or_else(|_| Reply::error("ERR replication request dropped"))
                    }
                }
                Class::Unsupported => Reply::error(format!(
                    "ERR unknown command '{}' (not in the replication table)",
                    cmd.name
                )),
            };

            if sock.write_all(&reply.to_bytes()).await.is_err() {
                return;
            }
        }
    }
}

/// Static configuration handed to the driver task.
struct DriverCfg {
    id: ProcId,
    mode: DeliveryMode,
    issue_initial: bool,
    ring_addrs: Vec<SocketAddr>,
}

/// Mutable per-node state threaded through the driver helpers.
struct DriverState<S: RedisStore> {
    core: TrainsNode,
    store: Arc<Mutex<S>>,
    pending: HashMap<u64, oneshot::Sender<Reply>>,
    next_rid: u64,
    /// Apply-side dedup (at-least-once / C3): a re-broadcast applies at most
    /// once. Bounded per-origin watermarks (PR-RED-1 / R-10) — memory is
    /// O(origins), not O(writes), so the long-running proxy cannot OOM here.
    applied_ops: WriteDedup,
    /// Bounded delivered-effect tail (PR-RJ-2b): records every applied effect so
    /// this (survivor) node can serve a rejoiner the contiguous catch-up `> X`.
    /// Append-only on the dedup-pass apply path; the serve/fetch wiring is RJ-3.
    delivered_log: DeliveredLog,
    /// Reconfiguration (PR-RD-4).
    detector: FailureDetector,
    vc: ViewChange,
    dead: BTreeSet<usize>,
    succ: usize,
}

/// The single per-node driver: owns the `trains-core` kernel, broadcasts client
/// writes, applies the delivered stream, and (when reconfiguration is enabled)
/// masks a crashed node via the distributed view change.
async fn driver_loop<S: RedisStore + Send + 'static>(
    cfg: DriverCfg,
    mut transport: RingTransport,
    store: Arc<Mutex<S>>,
    mut cmd_rx: mpsc::Receiver<WriteRequest>,
    mut ctrl_rx: mpsc::Receiver<DriverCtrl>,
    mut snap_req_rx: Option<mpsc::Receiver<SnapshotRequest>>,
    promote: Option<PromoteSeed>,
) {
    let id = cfg.id;
    let reconfig = !cfg.ring_addrs.is_empty();
    let n = if reconfig { cfg.ring_addrs.len() } else { RING_SIZE };
    let addrs = cfg.ring_addrs;

    let mut st = DriverState {
        core: TrainsNode::new(id, cfg.mode),
        store,
        pending: HashMap::new(),
        next_rid: 0,
        applied_ops: WriteDedup::new(),
        delivered_log: DeliveredLog::default(),
        detector: FailureDetector::new(STRIKE_THRESHOLD, DISCONNECT_WEIGHT),
        vc: ViewChange::new(id, n),
        dead: BTreeSet::new(),
        succ: (id as usize + 1) % n,
    };

    // v3 promotion (PR-V3-3c): seed the active driver from the caught-up passive
    // state so it is consistent with the (already-replaced) store, then adopt the
    // survivors' view. The survivors' protocol state has US in `crashed` (they
    // excluded us), so `readmit_node(self)` un-crashes us from our own view.
    let promoting = promote.is_some();
    if let Some(seed) = promote {
        st.applied_ops = seed.dedup;
        st.delivered_log = seed.delivered_log;
        st.core.import_state(seed.protocol);
        st.core.readmit_node(id);
        // Our view = the survivors' dead set, minus ourselves (we are live).
        let dead_others: BTreeSet<usize> =
            seed.view.dead.iter().copied().filter(|&d| d != id).map(|d| d as usize).collect();
        st.vc.adopt_view(
            seed.view.installed_view,
            seed.view.dead.iter().copied().filter(|&d| d != id),
        );
        st.succ = next_alive(id as usize, n, &dead_others);
        st.dead = dead_others;
    }

    if promoting {
        // Request re-admission to the acking view: the ReAdmitGather circulates,
        // the coordinator computes a plan, the ReAdmitInstall comes back and our
        // `on_readmit_install` Applies it (reissuing our slot). From then on
        // trains traverse us and our ack is required — a full member.
        let report = st.core.recovery_report();
        let acts = st.vc.on_request_readmit(report);
        tracing::info!(?id, "requesting re-admission to the acking view");
        execute(acts, id, &mut st, &transport).await;
    } else if cfg.issue_initial {
        let t = st.core.issue_initial_train();
        tracing::info!(?id, clock = t.clock, "issuing initial train");
        let _ = transport.outbox.send(t).await;
    }

    let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));

    loop {
        tokio::select! {
            maybe_req = cmd_rx.recv() => {
                match maybe_req {
                    Some(req) => {
                        handle_write_request(req, id, &mut st, &transport).await;
                    }
                    None => {
                        tracing::info!(?id, "command channel closed; driver exiting");
                        break;
                    }
                }
            }
            maybe_train = transport.inbox.recv() => {
                match maybe_train {
                    Some(t) => {
                        st.detector.note_alive(t.issuer);
                        let outs = st.core.step(Input::TrainReceived(t));
                        let victims = pump_outputs(outs, id, &st.store, &mut st.pending,
                                                   &mut st.applied_ops, &mut st.delivered_log, &transport.outbox).await;
                        process_hints(victims, id, n, reconfig, &mut st, &transport, &addrs).await;
                    }
                    None => {
                        tracing::info!(?id, "ring inbox closed; driver exiting");
                        break;
                    }
                }
            }
            Some(msg) = transport.vc_inbox.recv(), if reconfig => {
                handle_vc(msg, id, n, &mut st, &transport, &addrs).await;
            }
            Some(addr) = transport.unreachable_rx.recv(), if reconfig => {
                if let Some(victim) = addrs.iter().position(|a| *a == addr) {
                    if !st.dead.contains(&victim) {
                        if let Some(confirmed) = st.detector.record_disconnect(victim as ProcId) {
                            tracing::warn!(?id, confirmed, "successor unreachable → view change");
                            handle_crash_confirmed(confirmed, id, n, &mut st, &transport, &addrs).await;
                        }
                    }
                }
            }
            _ = tick.tick() => {
                let outs = st.core.step(Input::Tick);
                let victims = pump_outputs(outs, id, &st.store, &mut st.pending,
                                           &mut st.applied_ops, &mut st.delivered_log, &transport.outbox).await;
                process_hints(victims, id, n, reconfig, &mut st, &transport, &addrs).await;
            }
            // State-transfer request from a rejoining peer (PR-RJ-3b). Build the
            // transfer from our live state and answer. `pending()` parks this arm
            // forever when no server is configured, so it never fires then.
            maybe_req = recv_opt(&mut snap_req_rx) => {
                if let Some(req) = maybe_req {
                    serve_state_transfer(req, &st);
                }
            }
            Some(c) = ctrl_rx.recv() => {
                match c {
                    DriverCtrl::Crash => {
                        tracing::warn!(?id, "CRASH (simulated) — aborting ring transport");
                        transport.abort();
                        break;
                    }
                    DriverCtrl::ConfirmCrash(victim) if reconfig => {
                        handle_crash_confirmed(victim, id, n, &mut st, &transport, &addrs).await;
                    }
                    DriverCtrl::ConfirmCrash(_) => {}
                }
            }
        }
    }
}

/// Classify/resolve a client write and broadcast it (or answer immediately).
async fn handle_write_request<S: RedisStore>(
    req: WriteRequest,
    id: ProcId,
    st: &mut DriverState<S>,
    transport: &RingTransport,
) {
    // Decide the action without consuming req.reply yet.
    let action = match Command::parse(req.argv.clone()) {
        Some(cmd) => match classify(&cmd.name) {
            Class::Write => WriteAction::Broadcast(req.argv.clone(), None),
            Class::NonDeterministic => {
                let resolution = {
                    let g = st.store.lock().expect("store mutex poisoned");
                    effect::resolve(&cmd, &*g)
                };
                match resolution {
                    Resolution::Immediate(r) => WriteAction::Immediate(r),
                    Resolution::Broadcast { argv, client_reply } => {
                        WriteAction::Broadcast(argv, Some(client_reply))
                    }
                }
            }
            _ => WriteAction::Immediate(Reply::error("ERR command is not replicable as a write")),
        },
        None => WriteAction::Immediate(Reply::error("ERR empty command")),
    };

    match action {
        WriteAction::Immediate(reply) => {
            let _ = req.reply.send(reply);
        }
        WriteAction::Broadcast(argv, client_reply) => {
            let request_id = st.next_rid;
            let mut op = WriteOp::new(id, request_id, argv);
            if let Some(r) = client_reply {
                op = op.with_client_reply(r);
            }
            match op.encode() {
                Ok(bytes) => {
                    // Consume the id only on success: a permanently skipped
                    // request_id would leave a gap that pins every replica's
                    // dedup watermark for this origin (PR-RED-1).
                    st.next_rid += 1;
                    st.pending.insert(request_id, req.reply);
                    let outs = st.core.step(Input::LocalBroadcast(bytes));
                    // A LocalBroadcast won't declare crashes; ignore any victims.
                    let _ = pump_outputs(outs, id, &st.store, &mut st.pending,
                                         &mut st.applied_ops, &mut st.delivered_log, &transport.outbox).await;
                }
                Err(e) => {
                    let _ = req.reply.send(Reply::error(format!(
                        "ERR failed to encode write for replication: {e}"
                    )));
                }
            }
        }
    }
}

/// Route core outputs: forward trains, apply delivered writes to the store
/// (deduped; waking the originating client), and collect any `DeclareCrash`
/// victims for the failure detector.
async fn pump_outputs<S: RedisStore>(
    outs: Vec<Output>,
    id: ProcId,
    store: &Arc<Mutex<S>>,
    pending: &mut HashMap<u64, oneshot::Sender<Reply>>,
    applied_ops: &mut WriteDedup,
    delivered_log: &mut DeliveredLog,
    outbox: &mpsc::Sender<Train>,
) -> Vec<ProcId> {
    let mut crash_hints = Vec::new();
    for o in outs {
        match o {
            Output::ForwardTrain(t) => {
                let _ = outbox.send(t).await;
            }
            Output::Deliver(payloads) => {
                for p in payloads {
                    let op = match WriteOp::decode(&p.data) {
                        Ok(op) => op,
                        Err(e) => {
                            tracing::warn!(error = %e, "skipping undecodable delivered payload");
                            continue;
                        }
                    };
                    // Dedup (at-least-once / C3): skip an op already applied.
                    if !applied_ops.first_seen(op.origin, op.request_id) {
                        continue;
                    }
                    // Apply the deterministic effect on every replica. Lock scope
                    // ends with this match arm — never held across await.
                    let apply_reply = match op.command() {
                        Some(cmd) => store.lock().expect("store mutex poisoned").apply(&cmd),
                        None => Reply::error("ERR delivered write op had empty argv"),
                    };
                    // Record the applied effect in the delivered-effect tail
                    // (PR-RJ-2b) — the survivor's catch-up source for a rejoiner.
                    delivered_log.append(op.clone());
                    if op.origin == id {
                        if let Some(tx) = pending.remove(&op.request_id) {
                            let reply = op.client_reply.clone().unwrap_or(apply_reply);
                            let _ = tx.send(reply);
                        }
                    }
                }
            }
            Output::DeclareCrash(victim) => crash_hints.push(victim),
        }
    }
    crash_hints
}

/// Await the next state-transfer request, or park forever when no server is
/// configured (so the `select!` arm is inert). Disjoint from the rest of `st`.
async fn recv_opt(rx: &mut Option<mpsc::Receiver<SnapshotRequest>>) -> Option<SnapshotRequest> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Answer a rejoining peer's state-transfer request from our live driver state
/// (PR-RJ-3b): an incremental delivered-effect tail when the peer is recent
/// enough, else a full snapshot — decided by [`build_state_transfer_lazy`], which
/// only serializes the snapshot on the snapshot path.
fn serve_state_transfer<S: RedisStore>(req: SnapshotRequest, st: &DriverState<S>) {
    let have = req.have();
    let (snapshot, tail) = build_state_transfer_lazy(have, &st.delivered_log, || ReplicaSnapshot {
        version: SNAPSHOT_VERSION,
        protocol: st.core.export_state(),
        store: st.store.lock().expect("store mutex poisoned").export_snapshot(),
        dedup: st.applied_ops.clone(),
        delivered_index: st.delivered_log.head_index(),
        // Carry our reconfiguration view so a promoting rejoiner can adopt it
        // and seed a non-fenced ReAdmitGather (PR-V3-3).
        view: Some(ViewInfo {
            installed_view: st.vc.installed_view(),
            dead: st.dead.iter().map(|&d| d as ProcId).collect(),
        }),
    });
    tracing::debug!(have, snapshot = snapshot.len(), tail = tail.len(), "serving state transfer");
    req.reply(StateTransfer { snapshot, tail });
}

/// Passive-rejoin catch-up loop (PR-RJ-3c): the off-ring half of v2 rejoin.
///
/// The node owns its own dedup + delivered-effect log (it is not on the ring, so
/// it never broadcasts). Each round it asks a survivor for the state transfer
/// that closes the gap from its current delivered-index: the first round (`have
/// == 0`) imports a full snapshot — a keyspace *replace* that wipes any stale
/// pre-downtime state — and subsequent rounds apply only the incremental tail,
/// so the replica tracks live writes. Client reads are served from the shared
/// store (by the listener); client writes are rejected (read-only standby).
/// State captured during passive catch-up to seed the active [`DriverState`]
/// when promoting to a full acking member (PR-V3-3c). The store is already
/// updated in place (via the shared `Arc<Mutex>`); these are the pieces the
/// active driver must adopt to be consistent with it.
struct PromoteSeed {
    dedup: WriteDedup,
    delivered_log: DeliveredLog,
    /// The survivor's core state (clocks / done-keys / crashed mask) at the
    /// promotion snapshot — the driver imports it, then `readmit_node`s itself.
    protocol: StateSnapshot,
    /// The survivor's reconfiguration view, to `adopt_view` before re-admitting.
    view: ViewInfo,
}

/// Consecutive idle catch-up rounds (incremental fetch found nothing new)
/// before a promote-enabled rejoiner concludes it is caught up and promotes.
const PROMOTE_IDLE_ROUNDS: u32 = 2;

/// Passive catch-up (PR-RJ-3c) with optional v3 promotion (PR-V3-3c).
///
/// Off the ring, the node owns its own dedup + delivered-effect log and pulls
/// the gap-closing transfer from a survivor each round; client reads are served
/// from the shared store, writes rejected (read-only standby). Returns:
/// - `Some(seed)` when `cfg.promote` and the node has caught up — the caller
///   spawns the ring transport and hands the seed to the active driver; or
/// - `None` when the command channel closes (shutdown) — a non-promoting node
///   simply loops here forever (the proven v2 behaviour, untouched).
async fn passive_catch_up<S: RedisStore + Send + 'static>(
    store: Arc<Mutex<S>>,
    cfg: &RejoinCfg,
    cmd_rx: &mut mpsc::Receiver<WriteRequest>,
) -> Option<PromoteSeed> {
    let mut dedup = WriteDedup::new();
    let mut log = DeliveredLog::default();
    let mut idle = 0u32;
    let mut tick = tokio::time::interval(cfg.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let have = log.head_index();
                let mut served = false;
                let mut progress = false;
                for addr in &cfg.survivor_addrs {
                    match fetch_state(*addr, &cfg.fetch_identity, &cfg.survivor_fingerprints, have).await {
                        Ok(xfer) => match apply_passive_transfer(&xfer, &store, &mut dedup, &mut log) {
                            Ok(n) => {
                                progress = n > 0 || !xfer.snapshot.is_empty();
                                if progress {
                                    tracing::debug!(%addr, applied = n, head = log.head_index(),
                                        "passive catch-up applied");
                                }
                                served = true;
                                break;
                            }
                            Err(e) => tracing::warn!(%addr, error = %e, "applying state transfer failed"),
                        },
                        Err(e) => tracing::debug!(%addr, error = %e, "fetch_state failed; trying next survivor"),
                    }
                }
                // Promotion (opt-in): once caught up (served a survivor but it had
                // nothing new for a couple of rounds), take a final consistent
                // snapshot and hand off to the active driver.
                if cfg.promote && served && !progress && log.head_index() > 0 {
                    idle += 1;
                    if idle >= PROMOTE_IDLE_ROUNDS {
                        tracing::info!(head = log.head_index(),
                            "passive replica caught up — promoting to a full acking member");
                        if let Some(seed) =
                            final_snapshot_for_promotion(&store, cfg, &mut dedup, &mut log).await
                        {
                            return Some(seed);
                        }
                        idle = 0; // couldn't snapshot for promotion; keep tailing
                    }
                } else if progress {
                    idle = 0;
                }
            }
            maybe = cmd_rx.recv() => match maybe {
                Some(req) => {
                    let _ = req.reply.send(Reply::error(
                        "ERR node is rejoining — read-only passive replica",
                    ));
                }
                None => {
                    tracing::info!("command channel closed; passive catch-up exiting");
                    return None;
                }
            },
        }
    }
}

/// Take a final full (`have=0`) state transfer from a survivor and build a
/// [`PromoteSeed`] (PR-V3-3c): a consistent (protocol, store, view) base for the
/// promoted driver. The store is replaced + the tail applied here, so the seed's
/// dedup/log match it. `None` if no survivor answers with a view-carrying
/// snapshot (the caller then stays passive).
async fn final_snapshot_for_promotion<S: RedisStore>(
    store: &Arc<Mutex<S>>,
    cfg: &RejoinCfg,
    dedup: &mut WriteDedup,
    log: &mut DeliveredLog,
) -> Option<PromoteSeed> {
    for addr in &cfg.survivor_addrs {
        let Ok(xfer) = fetch_state(*addr, &cfg.fetch_identity, &cfg.survivor_fingerprints, 0).await
        else {
            continue;
        };
        if xfer.snapshot.is_empty() {
            continue;
        }
        let Ok((snap, _)) = bincode::serde::decode_from_slice::<ReplicaSnapshot, _>(
            &xfer.snapshot,
            bincode::config::standard(),
        ) else {
            continue;
        };
        let Some(view) = snap.view.clone() else {
            tracing::warn!("promotion: survivor snapshot carried no view — staying passive");
            continue;
        };
        // Replace the store + reset dedup/log, then apply the tail (the delta
        // delivered while the snapshot was in flight) — the same consistent base
        // the v2 path uses, captured here for the active driver.
        if store.lock().expect("store mutex poisoned").import_snapshot(&snap.store).is_err() {
            continue;
        }
        *dedup = snap.dedup.clone();
        *log = DeliveredLog::resumed_at(snap.delivered_index, log.cap());
        for frame in &xfer.tail {
            if let Ok(entry) = DeliveredEntry::decode(frame) {
                let mut g = store.lock().expect("store mutex poisoned");
                let _ = apply_delivered_op_parts(entry.op, &mut *g, dedup, log);
            }
        }
        return Some(PromoteSeed {
            dedup: dedup.clone(),
            delivered_log: log.clone(),
            protocol: snap.protocol,
            view,
        });
    }
    None
}

/// Apply a fetched state transfer to the passive replica's pieces (PR-RJ-3c):
/// import the snapshot when present (a full keyspace replace), then replay the
/// tail through the shared at-least-once apply. Mirrors
/// [`crate::replica::Replica::apply_state_transfer`] for the proxy's
/// `Arc<Mutex<Store>>` + standalone dedup/log. The protocol state in the
/// snapshot is intentionally ignored — a passive replica never rejoins the
/// ordering quorum (that is v3 re-admission).
fn apply_passive_transfer<S: RedisStore>(
    xfer: &StateTransfer,
    store: &Arc<Mutex<S>>,
    dedup: &mut WriteDedup,
    log: &mut DeliveredLog,
) -> Result<usize, SnapshotError> {
    if !xfer.snapshot.is_empty() {
        let (snap, _) = bincode::serde::decode_from_slice::<ReplicaSnapshot, _>(
            &xfer.snapshot,
            bincode::config::standard(),
        )?;
        store
            .lock()
            .expect("store mutex poisoned")
            .import_snapshot(&snap.store)?;
        *dedup = snap.dedup;
        *log = DeliveredLog::resumed_at(snap.delivered_index, log.cap());
    }
    let mut applied = 0;
    for frame in &xfer.tail {
        let entry = DeliveredEntry::decode(frame)?;
        let mut guard = store.lock().expect("store mutex poisoned");
        if apply_delivered_op_parts(entry.op, &mut *guard, dedup, log).is_some() {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Feed `DeclareCrash` hints through the ◇S detector; on a confirmed crash,
/// begin the view change (reconfiguration) or just confirm in the core.
async fn process_hints<S: RedisStore>(
    victims: Vec<ProcId>,
    id: ProcId,
    n: usize,
    reconfig: bool,
    st: &mut DriverState<S>,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    for victim in victims {
        if st.detector.is_confirmed(victim) {
            continue;
        }
        let Some(confirmed) = st.detector.record_gap_hint(victim) else {
            continue;
        };
        if reconfig {
            handle_crash_confirmed(confirmed, id, n, st, transport, addrs).await;
        } else {
            let _ = st.core.confirm_crash(confirmed);
        }
    }
}

/// Begin a view change for a confirmed crash: exclude the victim, snapshot a
/// fresh recovery report, and run/execute the coordinator's actions.
async fn handle_crash_confirmed<S: RedisStore>(
    confirmed: ProcId,
    id: ProcId,
    n: usize,
    st: &mut DriverState<S>,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    exclude(confirmed as usize, id, n, st, transport, addrs).await;
    let report = st.core.recovery_report();
    let actions = st.vc.on_confirm(confirmed, report);
    execute(actions, id, st, transport).await;
}

/// Handle an incoming view-change token: learn the crash, run the state machine
/// on a fresh snapshot, execute its actions.
async fn handle_vc<S: RedisStore>(
    msg: ViewChangeMsg,
    id: ProcId,
    n: usize,
    st: &mut DriverState<S>,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    // Re-admit tokens (PR-RJ-2a/V3) carry a `rejoiner`, not a `victim`. Membership
    // GROWS: re-include the rejoiner (drop from dead, un-crash in the core, retarget
    // back), then run the re-admit state machine — the mirror of the exclude path.
    if let Some(rejoiner) = msg.rejoiner() {
        readmit(rejoiner as usize, id, n, st, transport, addrs).await;
        let report = st.core.recovery_report();
        let actions = match &msg {
            ViewChangeMsg::ReAdmitGather { .. } => st.vc.on_readmit_gather(msg, report),
            ViewChangeMsg::ReAdmitInstall { .. } => st.vc.on_readmit_install(msg),
            _ => Vec::new(),
        };
        execute(actions, id, st, transport).await;
        return;
    }
    let Some(victim) = msg.victim() else {
        return;
    };
    exclude(victim as usize, id, n, st, transport, addrs).await;
    let report = st.core.recovery_report();
    let actions = match &msg {
        ViewChangeMsg::Gather { .. } => st.vc.on_gather(msg, report),
        ViewChangeMsg::Install { .. } => st.vc.on_install(msg),
        // Filtered out above (re-admit tokens have a rejoiner, not a victim).
        ViewChangeMsg::ReAdmitGather { .. } | ViewChangeMsg::ReAdmitInstall { .. } => Vec::new(),
    };
    execute(actions, id, st, transport).await;
}

/// Exclude `victim`: freeze delivery, confirm the crash, retarget past it.
async fn exclude<S: RedisStore>(
    victim: usize,
    id: ProcId,
    n: usize,
    st: &mut DriverState<S>,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    if st.dead.contains(&victim) {
        return;
    }
    st.dead.insert(victim);
    st.core.set_frozen(true);
    let _ = st.core.confirm_crash(victim as ProcId);
    let ns = next_alive(id as usize, n, &st.dead);
    if ns != st.succ {
        tracing::info!(?id, old = st.succ, new = ns, "retarget successor");
        st.succ = ns;
        transport.retarget_successor(addrs[ns]).await;
    }
}

/// Re-include `rejoiner` (the inverse of [`exclude`], v3): drop it from `dead`,
/// un-crash it in the core (so its ack is required again — a full acking member),
/// freeze for the re-admit barrier, and retarget the successor BACK through it if
/// it is now the next-alive hop. Idempotent: the membership change happens once
/// (the first re-admit token); later tokens for the same rejoiner are no-ops. The
/// `apply_recovery` driven by the `ReAdmitInstall`'s `Apply` action unfreezes.
async fn readmit<S: RedisStore>(
    rejoiner: usize,
    id: ProcId,
    n: usize,
    st: &mut DriverState<S>,
    transport: &RingTransport,
    addrs: &[SocketAddr],
) {
    if !st.dead.remove(&rejoiner) {
        return; // not excluded here / already re-admitted
    }
    st.core.set_frozen(true);
    st.core.readmit_node(rejoiner as ProcId);
    let ns = next_alive(id as usize, n, &st.dead);
    if ns != st.succ {
        tracing::info!(?id, old = st.succ, new = ns, rejoiner, "retarget successor back (re-admit)");
        st.succ = ns;
        transport.retarget_successor(addrs[ns]).await;
    }
}

/// Execute the view-change state machine's actions.
async fn execute<S: RedisStore>(
    actions: Vec<VcAction>,
    id: ProcId,
    st: &mut DriverState<S>,
    transport: &RingTransport,
) {
    for a in actions {
        match a {
            VcAction::Send(msg) => {
                let _ = transport.vc_outbox.send(msg).await;
            }
            VcAction::Apply(plan) => {
                let outs = st.core.apply_recovery(&plan);
                // Recovery delivers gap-resolved writes — apply them to the store
                // (deduped) just like any delivery.
                let _ = pump_outputs(outs, id, &st.store, &mut st.pending,
                                     &mut st.applied_ops, &mut st.delivered_log, &transport.outbox).await;
                if (id as usize) < NUM_TRAINS {
                    let t = st.core.reissue_train();
                    tracing::info!(?id, clock = t.clock, "reissue train");
                    let _ = transport.outbox.send(t).await;
                }
            }
        }
    }
}

/// Next alive node after `me` on the ring (wraps; assumes ≥1 alive).
fn next_alive(me: usize, n: usize, dead: &BTreeSet<usize>) -> usize {
    let mut j = (me + 1) % n;
    while dead.contains(&j) && j != me {
        j = (j + 1) % n;
    }
    j
}

#[cfg(test)]
mod tests {
    //! Unit tests for the listener backpressure gate (R-02, PR-SEC-B).
    //! Full end-to-end coverage of listener_loop + a real proxy is in
    //! crates/trains-valkey/tests/proxy_tls.rs; these tests isolate the gate.

    use super::*;
    use std::net::Ipv4Addr;

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn gate_admits_under_caps() {
        let gate = ConnGate::new(10);
        let g1 = gate.try_admit(ip(127, 0, 0, 1)).expect("first admit");
        let g2 = gate.try_admit(ip(127, 0, 0, 1)).expect("second admit, same IP");
        let g3 = gate.try_admit(ip(127, 0, 0, 2)).expect("third admit, different IP");
        assert_eq!(gate.observed_for(ip(127, 0, 0, 1)), 2);
        assert_eq!(gate.observed_for(ip(127, 0, 0, 2)), 1);
        drop((g1, g2, g3));
    }

    #[test]
    fn gate_rejects_when_global_semaphore_full() {
        let gate = ConnGate::new(2);
        let _a = gate.try_admit(ip(127, 0, 0, 1)).expect("admit 1");
        let _b = gate.try_admit(ip(127, 0, 0, 2)).expect("admit 2");
        assert!(gate.try_admit(ip(127, 0, 0, 3)).is_none(),
                "third admit must fail when semaphore is exhausted");
    }

    #[test]
    fn gate_rejects_when_per_ip_cap_reached() {
        let gate = ConnGate::new(1024);
        let mut guards = Vec::with_capacity(PER_IP_CONN_CAP as usize);
        for i in 0..PER_IP_CONN_CAP {
            let g = gate.try_admit(ip(10, 0, 0, 1))
                .unwrap_or_else(|| panic!("admit {i} must succeed"));
            guards.push(g);
        }
        assert!(gate.try_admit(ip(10, 0, 0, 1)).is_none(),
                "PER_IP_CONN_CAP+1 from the same IP must be rejected");
        // ...but another IP can still get in.
        assert!(gate.try_admit(ip(10, 0, 0, 2)).is_some(),
                "different IP must still be admitted when global cap has headroom");
    }

    #[test]
    fn drop_releases_both_semaphore_and_per_ip_slot() {
        let gate = ConnGate::new(2);
        // Fill global cap with a single IP.
        let g1 = gate.try_admit(ip(127, 0, 0, 1)).unwrap();
        let _g2 = gate.try_admit(ip(127, 0, 0, 1)).unwrap();
        assert!(gate.try_admit(ip(127, 0, 0, 2)).is_none(), "global cap full");
        // Drop one — both the permit and the per-IP counter should release.
        let ip1 = ip(127, 0, 0, 1);
        let observed_before = gate.observed_for(ip1);
        drop(g1);
        let observed_after = gate.observed_for(ip1);
        assert_eq!(observed_before - 1, observed_after,
                   "per-IP counter must decrement on guard drop");
        // Now a new admit from a different IP must succeed.
        assert!(gate.try_admit(ip(127, 0, 0, 2)).is_some(),
                "global cap has headroom after drop");
    }

    #[test]
    fn per_ip_entry_is_removed_when_count_hits_zero() {
        let gate = ConnGate::new(10);
        let g = gate.try_admit(ip(192, 168, 1, 1)).unwrap();
        assert_eq!(gate.observed_for(ip(192, 168, 1, 1)), 1);
        drop(g);
        // After the only guard for this IP drops, the map entry should be gone
        // (not just set to 0). Keeps the map bounded under churn.
        assert_eq!(gate.observed_for(ip(192, 168, 1, 1)), 0);
        assert!(!gate.per_ip.lock().unwrap().contains_key(&ip(192, 168, 1, 1)),
                "per-IP map entry should be removed when count reaches zero");
    }

    #[test]
    fn per_ip_cap_constant_is_below_global_cap() {
        // Sanity: a single IP can't exhaust the global semaphore.
        assert!((PER_IP_CONN_CAP as usize) < MAX_CLIENT_CONNS,
                "PER_IP_CONN_CAP must be < MAX_CLIENT_CONNS so one IP can't OOM us");
    }

    #[test]
    fn busy_reply_is_valid_resp_error_line() {
        assert!(BUSY_REPLY.starts_with(b"-"), "BUSY_REPLY must be a RESP error");
        assert!(BUSY_REPLY.ends_with(b"\r\n"), "BUSY_REPLY must be CRLF-terminated");
    }
}
