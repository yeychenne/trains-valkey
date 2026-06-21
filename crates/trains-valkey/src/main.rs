//! `trains-valkey` — run one TRAINS-replicated Redis proxy node.
//!
//! Mirrors `trains node` arg-wise, plus `--resp-listen` (the RESP client port).
//! Clients speak RESP to `--resp-listen`; the node replicates writes around the
//! TLS ring to its peers. RD-1 backs the node with an in-process key-value
//! model ([`trains_valkey::MemStore`]); the real `redis-server` backend lands in
//! PR-RD-4.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use trains_core::DeliveryMode;
use trains_net::{NodeIdentity, RingConfig, SpkiFingerprint};
use trains_valkey::proxy::{
    run_proxy_node, ClientTlsConfig, ProxyConfig, ProxyHandle, RejoinCfg, SnapshotServerCfg,
};
use trains_valkey::store::RedisStore;
use trains_valkey::{MemStore, RedisBackend};

/// Manually clone a [`NodeIdentity`] (it isn't `Clone`): the ring identity is
/// reused for the state-transfer server and the rejoin fetch identity.
fn clone_identity(id: &NodeIdentity) -> NodeIdentity {
    NodeIdentity {
        cert_chain: id.cert_chain.clone(),
        key: id.key.clone_key(),
        fingerprint: id.fingerprint,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum DeliveryModeArg {
    /// Uniform total order — ack from every node required.
    Uto,
    /// Total order within the surviving view — the crash-masking mode.
    To,
}

impl From<DeliveryModeArg> for DeliveryMode {
    fn from(m: DeliveryModeArg) -> Self {
        match m {
            DeliveryModeArg::Uto => DeliveryMode::UniformTotalOrder,
            DeliveryModeArg::To => DeliveryMode::TotalOrder,
        }
    }
}

#[derive(Parser)]
#[command(name = "trains-valkey", version, about = "TRAINS-replicated Redis proxy node")]
struct Cli {
    /// Ring node id (0..RING_SIZE).
    #[arg(long)]
    id: u8,
    /// Ring listen address (predecessor connects here).
    #[arg(long)]
    listen: SocketAddr,
    /// Successor ring address (we connect to it).
    #[arg(long)]
    successor: SocketAddr,
    /// RESP client listen address (clients connect here).
    #[arg(long)]
    resp_listen: SocketAddr,
    /// This node's TLS identity JSON (see `trains keygen`).
    #[arg(long)]
    identity: PathBuf,
    /// Pinned peer fingerprint(s), comma-separated hex (64 chars each).
    #[arg(long)]
    peer_fp: String,
    /// Issue an initial train at startup (set on issuer nodes only).
    #[arg(long)]
    issue_initial: bool,
    /// Delivery mode: `to` (default, crash-masking) or `uto` (strict).
    #[arg(long, value_enum, default_value_t = DeliveryModeArg::To)]
    delivery_mode: DeliveryModeArg,
    /// Ring topology for reconfiguration: repeat `--peer-addr <id>=<addr>` for
    /// EVERY node (including self) to enable crash masking (the distributed view
    /// change). Omit to disable reconfiguration.
    #[arg(long = "peer-addr")]
    peer_addr: Vec<String>,
    /// Client-boundary mTLS (R-06): this node's TLS identity JSON presented to
    /// RESP clients (`trains keygen`). Supplying it enables mutual TLS on the
    /// RESP port; you must then also pass `--allowed-client-spki`. Omit to serve
    /// plaintext RESP (a loud warning is printed).
    #[arg(long)]
    client_identity: Option<PathBuf>,
    /// Client-boundary mTLS (R-06): allowed client SPKI fingerprint(s) (hex).
    /// Repeat for each permitted client. Required when `--client-identity` is set.
    #[arg(long = "allowed-client-spki")]
    allowed_client_spki: Vec<String>,
    /// Explicitly acknowledge the plaintext RESP boundary and silence the
    /// startup warning. Mutually exclusive with `--client-identity`.
    #[arg(long, conflicts_with = "client_identity")]
    no_client_tls: bool,
    /// Storage backend: `mem` (default, in-process model — no engine needed),
    /// `redis://HOST:PORT` for a co-located engine over TCP, or
    /// `unix:///path/to/valkey.sock` for the hardened UNIX-domain-socket path
    /// (R-07: Valkey bound to a UDS only, no TCP).
    #[arg(long, default_value = "mem")]
    backend: String,
    /// Backend password if the engine has `requirepass` set. Prefer
    /// `--backend-password-file` in production — a password passed here is
    /// visible in `ps`/`/proc`.
    #[arg(long)]
    backend_password: Option<String>,
    /// Path to a file whose (trimmed) contents are the backend `requirepass`
    /// password (R-07). Takes precedence over `--backend-password` and keeps the
    /// secret out of the process argument list.
    #[arg(long, conflicts_with = "backend_password")]
    backend_password_file: Option<PathBuf>,
    /// State-transfer server (PR-RJ-3b): serve a snapshot + delivered-effect tail
    /// to rejoining peers on this address. The server presents this node's ring
    /// `--identity` and admits the ring peers pinned in `--peer-fp`. Survivors in
    /// the E5 rejoin scenario launch with this set. Omit to not serve.
    #[arg(long)]
    snapshot_listen: Option<SocketAddr>,
    /// Passive rejoin (PR-RJ-3c): this node is restarting after a crash — it
    /// catches up OFF the ring from a survivor's `--snapshot-listen` address
    /// (repeatable; tried in order) and tails it as a read-only passive replica.
    /// Presence enables passive mode (no ring participation). Survivors are pinned
    /// by `--peer-fp`; this node fetches with its own `--identity`.
    #[arg(long = "rejoin-from")]
    rejoin_from: Vec<SocketAddr>,
    /// Passive-rejoin poll interval (ms) — how often to pull a fresh tail.
    #[arg(long, default_value_t = 200)]
    rejoin_poll_ms: u64,
    /// v3 (PR-V3-3c): once caught up, promote from passive replica to a FULL
    /// acking ring member via the re-admit view change (restores N-redundancy).
    /// Off by default — the proven v2 passive standby is the safe baseline.
    #[arg(long)]
    rejoin_promote: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,trains_net=warn")),
        )
        .init();

    let cli = Cli::parse();

    let identity = NodeIdentity::load(&cli.identity)
        .with_context(|| format!("loading identity from {}", cli.identity.display()))?;
    let pinned = parse_fingerprints(&cli.peer_fp).context("parsing --peer-fp")?;
    let ring_addrs = parse_ring_addrs(&cli.peer_addr).context("parsing --peer-addr")?;
    let client_tls = build_client_tls(&cli)?;

    // State-transfer server (PR-RJ-3b): present the ring identity, admit the ring
    // peers. Built before `identity`/`pinned` are moved into the ring config.
    let snapshot_server = cli.snapshot_listen.map(|listen| {
        eprintln!("[{}] state-transfer server: serving on {listen}", cli.id);
        SnapshotServerCfg {
            listen,
            identity: clone_identity(&identity),
            allowed_fetcher_fingerprints: pinned.clone(),
        }
    });
    // Passive rejoin (PR-RJ-3c): when `--rejoin-from` is given, this node comes up
    // OFF the ring and catches up from the named survivors (pinned by --peer-fp).
    let rejoin = (!cli.rejoin_from.is_empty()).then(|| {
        eprintln!(
            "[{}] PASSIVE REJOIN: catching up from {} survivor(s), poll {}ms",
            cli.id,
            cli.rejoin_from.len(),
            cli.rejoin_poll_ms
        );
        RejoinCfg {
            survivor_addrs: cli.rejoin_from.clone(),
            fetch_identity: clone_identity(&identity),
            survivor_fingerprints: pinned.clone(),
            poll_interval: Duration::from_millis(cli.rejoin_poll_ms),
            promote: cli.rejoin_promote,
        }
    });

    let cfg = ProxyConfig {
        id: cli.id,
        mode: cli.delivery_mode.into(),
        issue_initial: cli.issue_initial,
        resp_listen: cli.resp_listen,
        client_tls,
        ring: RingConfig {
            identity,
            listen_addr: cli.listen,
            successor_addr: cli.successor,
            pinned_peer_fingerprints: pinned,
        },
        ring_addrs,
        snapshot_server,
        rejoin,
    };

    let backend_password = resolve_backend_password(&cli)?;

    // Choose the backend; all monomorphize the same `run_proxy_node`.
    match parse_backend(&cli.backend)? {
        Backend::Mem => {
            let handle = run_proxy_node(cfg, MemStore::new())
                .await
                .context("starting proxy node (mem backend)")?;
            serve(cli.id, handle, cli.listen, cli.successor).await;
        }
        Backend::Redis(addr) => {
            let be = RedisBackend::connect_auth(addr, backend_password.as_deref())
                .with_context(|| format!("connecting redis backend at {addr}"))?;
            let handle = run_proxy_node(cfg, be)
                .await
                .context("starting proxy node (redis backend)")?;
            serve(cli.id, handle, cli.listen, cli.successor).await;
        }
        Backend::RedisUds(path) => {
            let be = RedisBackend::connect_uds_auth(&path, backend_password.as_deref())
                .with_context(|| format!("connecting redis backend at unix://{}", path.display()))?;
            let handle = run_proxy_node(cfg, be)
                .await
                .context("starting proxy node (redis UDS backend)")?;
            serve(cli.id, handle, cli.listen, cli.successor).await;
        }
    }
    Ok(())
}

/// Resolve the backend `requirepass` password: `--backend-password-file`
/// (trimmed) takes precedence; else `--backend-password`; else none.
fn resolve_backend_password(cli: &Cli) -> Result<Option<String>> {
    if let Some(path) = &cli.backend_password_file {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading --backend-password-file {}", path.display()))?;
        return Ok(Some(raw.trim().to_string()));
    }
    Ok(cli.backend_password.clone())
}

/// Announce readiness, run until Ctrl-C, then shut down.
async fn serve<S: RedisStore + Send + 'static>(
    id: u8,
    handle: ProxyHandle<S>,
    listen: SocketAddr,
    successor: SocketAddr,
) {
    eprintln!(
        "[{id}] trains-valkey node ready — RESP on {}, ring {listen} -> {successor}",
        handle.resp_addr
    );
    tokio::signal::ctrl_c().await.ok();
    eprintln!("[{id}] shutting down");
    handle.shutdown();
}

/// Selected storage backend.
enum Backend {
    Mem,
    Redis(SocketAddr),
    RedisUds(PathBuf),
}

/// Parse `--backend`: `mem`, `redis://HOST:PORT`, or `unix:///path/to.sock`.
fn parse_backend(s: &str) -> Result<Backend> {
    if s == "mem" {
        return Ok(Backend::Mem);
    }
    if let Some(path) = s.strip_prefix("unix://") {
        if path.is_empty() {
            anyhow::bail!("--backend unix:// requires a socket path, e.g. unix:///var/run/trains/valkey.sock");
        }
        return Ok(Backend::RedisUds(PathBuf::from(path)));
    }
    let addr = s.strip_prefix("redis://").ok_or_else(|| {
        anyhow::anyhow!("--backend must be 'mem', 'redis://HOST:PORT', or 'unix:///path', got {s}")
    })?;
    let addr: SocketAddr = addr
        .parse()
        .with_context(|| format!("parsing backend address {addr}"))?;
    Ok(Backend::Redis(addr))
}

/// Build the optional client-boundary mTLS config (R-06) from the CLI.
///
/// `--client-identity` present ⇒ mutual TLS on the RESP port (requires at least
/// one `--allowed-client-spki`). Absent ⇒ plaintext RESP; warn loudly unless the
/// operator passed `--no-client-tls` to acknowledge it.
fn build_client_tls(cli: &Cli) -> Result<Option<ClientTlsConfig>> {
    match &cli.client_identity {
        Some(path) => {
            let identity = NodeIdentity::load(path)
                .with_context(|| format!("loading --client-identity from {}", path.display()))?;
            if cli.allowed_client_spki.is_empty() {
                anyhow::bail!(
                    "--client-identity set but no --allowed-client-spki given: no client \
                     could ever connect. Pass each permitted client's SPKI fingerprint."
                );
            }
            let allowed_client_fingerprints = cli
                .allowed_client_spki
                .iter()
                .map(|s| SpkiFingerprint::from_hex(s.trim()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow::anyhow!("invalid --allowed-client-spki: {e}"))?;
            eprintln!(
                "[{}] RESP client boundary: mTLS ENABLED ({} allowed client fingerprint(s))",
                cli.id,
                allowed_client_fingerprints.len()
            );
            Ok(Some(ClientTlsConfig {
                identity,
                allowed_client_fingerprints,
            }))
        }
        None => {
            if !cli.no_client_tls {
                eprintln!(
                    "[{}] WARNING: RESP client boundary is PLAINTEXT — any local actor can \
                     speak RESP as any client. Pass --client-identity + --allowed-client-spki \
                     to enable mTLS (R-06), or --no-client-tls to silence this warning.",
                    cli.id
                );
            }
            Ok(None)
        }
    }
}

/// Parse comma-separated hex SPKI fingerprints.
fn parse_fingerprints(s: &str) -> Result<Vec<SpkiFingerprint>> {
    s.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(SpkiFingerprint::from_hex)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("invalid fingerprint: {e}"))
}

/// Parse `--peer-addr <id>=<addr>` entries into addresses indexed by node id.
/// Empty input → empty vec (reconfiguration disabled). Ids must form `0..N`.
fn parse_ring_addrs(entries: &[String]) -> Result<Vec<SocketAddr>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let mut pairs: Vec<(usize, SocketAddr)> = Vec::with_capacity(entries.len());
    for e in entries {
        let (id_s, addr_s) = e
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--peer-addr must be 'ID=ADDR', got {e}"))?;
        let id: usize = id_s.trim().parse().with_context(|| format!("peer id in {e}"))?;
        let addr: SocketAddr = addr_s.trim().parse().with_context(|| format!("peer addr in {e}"))?;
        pairs.push((id, addr));
    }
    pairs.sort_by_key(|(id, _)| *id);
    for (expected, (id, _)) in pairs.iter().enumerate() {
        if *id != expected {
            anyhow::bail!("--peer-addr ids must be a contiguous 0..N range; got id {id} at position {expected}");
        }
    }
    Ok(pairs.into_iter().map(|(_, a)| a).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_mode_defaults_to_to_and_parses_uto() {
        let cli = Cli::try_parse_from([
            "trains-valkey",
            "--id", "0",
            "--listen", "127.0.0.1:9000",
            "--successor", "127.0.0.1:9001",
            "--resp-listen", "127.0.0.1:6379",
            "--identity", "/tmp/id.json",
            "--peer-fp", "aa",
        ])
        .unwrap();
        assert_eq!(cli.delivery_mode, DeliveryModeArg::To, "RD default is crash-masking TO");
        assert!(!cli.issue_initial);

        let cli = Cli::try_parse_from([
            "trains-valkey",
            "--id", "1",
            "--listen", "127.0.0.1:9000",
            "--successor", "127.0.0.1:9001",
            "--resp-listen", "127.0.0.1:6380",
            "--identity", "/tmp/id.json",
            "--peer-fp", "aa,bb",
            "--issue-initial",
            "--delivery-mode", "uto",
        ])
        .unwrap();
        assert_eq!(cli.delivery_mode, DeliveryModeArg::Uto);
        assert!(cli.issue_initial);
    }

    #[test]
    fn rejoin_and_snapshot_flags_parse() {
        // PR-RJ-3c CLI: survivor serves with --snapshot-listen; a rejoiner passes
        // --rejoin-from (repeatable) + an optional poll interval.
        let cli = Cli::try_parse_from([
            "trains-valkey",
            "--id", "2",
            "--listen", "127.0.0.1:9000",
            "--successor", "127.0.0.1:9001",
            "--resp-listen", "127.0.0.1:6379",
            "--identity", "/tmp/id.json",
            "--peer-fp", "aa,bb",
            "--snapshot-listen", "127.0.0.1:7000",
            "--rejoin-from", "127.0.0.1:7001",
            "--rejoin-from", "127.0.0.1:7002",
            "--rejoin-poll-ms", "150",
        ])
        .unwrap();
        assert_eq!(cli.snapshot_listen.unwrap().port(), 7000);
        assert_eq!(cli.rejoin_from.len(), 2);
        assert_eq!(cli.rejoin_from[1].port(), 7002);
        assert_eq!(cli.rejoin_poll_ms, 150);

        // Defaults: no server, not a rejoiner, 200ms poll.
        let bare = Cli::try_parse_from([
            "trains-valkey",
            "--id", "0",
            "--listen", "127.0.0.1:9000",
            "--successor", "127.0.0.1:9001",
            "--resp-listen", "127.0.0.1:6379",
            "--identity", "/tmp/id.json",
            "--peer-fp", "aa",
        ])
        .unwrap();
        assert!(bare.snapshot_listen.is_none());
        assert!(bare.rejoin_from.is_empty());
        assert_eq!(bare.rejoin_poll_ms, 200);
    }

    #[test]
    fn delivery_mode_arg_maps_to_core() {
        assert_eq!(DeliveryMode::from(DeliveryModeArg::Uto), DeliveryMode::UniformTotalOrder);
        assert_eq!(DeliveryMode::from(DeliveryModeArg::To), DeliveryMode::TotalOrder);
    }

    #[test]
    fn backend_parses_mem_and_redis() {
        assert!(matches!(parse_backend("mem").unwrap(), Backend::Mem));
        match parse_backend("redis://127.0.0.1:6379").unwrap() {
            Backend::Redis(a) => assert_eq!(a.port(), 6379),
            _ => panic!("expected redis backend"),
        }
        assert!(parse_backend("memcached").is_err());
        assert!(parse_backend("redis://not-an-addr").is_err());
    }

    #[test]
    fn backend_parses_unix_socket() {
        match parse_backend("unix:///var/run/trains/valkey.sock").unwrap() {
            Backend::RedisUds(p) => {
                assert_eq!(p, std::path::PathBuf::from("/var/run/trains/valkey.sock"))
            }
            _ => panic!("expected UDS backend"),
        }
        // Empty path is rejected.
        assert!(parse_backend("unix://").is_err());
    }

    #[test]
    fn ring_addrs_parse_indexed_by_id() {
        let v = parse_ring_addrs(&[
            "2=127.0.0.1:30".into(),
            "0=127.0.0.1:10".into(),
            "1=127.0.0.1:20".into(),
        ])
        .unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].port(), 10);
        assert_eq!(v[2].port(), 30);
        assert!(parse_ring_addrs(&[]).unwrap().is_empty());
        // Non-contiguous ids rejected.
        assert!(parse_ring_addrs(&["0=127.0.0.1:10".into(), "2=127.0.0.1:30".into()]).is_err());
    }
}
