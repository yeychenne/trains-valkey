//! Benchmark-only three-node proxy target for the local Arctic gate.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use serde::Serialize;
use trains_core::DeliveryMode;
use trains_net::{NodeIdentity, RingConfig};
use trains_valkey::proxy::{run_proxy_node_with_reader, ProxyConfig, ReadRouter};
use trains_valkey::{MemStore, RedisStore};

#[cfg(feature = "arctic-proxy")]
use trains_valkey::OrderedArcticStore;

const RING_SIZE: usize = 3;
const NUM_ISSUERS: usize = 2;

#[derive(Serialize)]
struct Ready<'a> {
    event: &'static str,
    backend: &'a str,
    pid: u32,
    resp_addr: SocketAddr,
    ring_size: usize,
}

fn pick_addr() -> Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

async fn serve<S, F>(backend: &str, mut make_store: F) -> Result<()>
where
    S: RedisStore + Send + 'static,
    F: FnMut() -> (S, Option<Arc<dyn ReadRouter>>),
{
    let identities: Vec<_> = (0..RING_SIZE)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]))
        .collect::<Result<_, _>>()?;
    let fingerprints: Vec<_> = identities
        .iter()
        .map(|identity| identity.fingerprint)
        .collect();
    let ring_addrs: Vec<_> = (0..RING_SIZE).map(|_| pick_addr()).collect::<Result<_>>()?;
    let resp_addrs: Vec<_> = (0..RING_SIZE).map(|_| pick_addr()).collect::<Result<_>>()?;

    let mut handles = Vec::with_capacity(RING_SIZE);
    for (index, identity) in identities.into_iter().enumerate() {
        let config = ProxyConfig {
            id: index as u8,
            mode: DeliveryMode::UniformTotalOrder,
            issue_initial: index < NUM_ISSUERS,
            resp_listen: resp_addrs[index],
            client_tls: None,
            ring: RingConfig {
                identity,
                listen_addr: ring_addrs[index],
                successor_addr: ring_addrs[(index + 1) % RING_SIZE],
                pinned_peer_fingerprints: fingerprints.clone(),
            },
            ring_addrs: Vec::new(),
            snapshot_server: None,
            rejoin: None,
        };
        let (store, reader) = make_store();
        handles.push(run_proxy_node_with_reader(config, store, reader).await?);
    }

    // Let the initial trains complete a circuit before advertising readiness.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ready = Ready {
        event: "ready",
        backend,
        pid: std::process::id(),
        resp_addr: handles[0].resp_addr,
        ring_size: RING_SIZE,
    };
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &ready)?;
    writeln!(stdout)?;
    stdout.flush()?;

    std::future::pending::<()>().await;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let backend = std::env::args().nth(1).unwrap_or_default();
    match backend.as_str() {
        "mem" => serve("mutex-proxy", || (MemStore::default(), None)).await,
        "arctic" => {
            #[cfg(feature = "arctic-proxy")]
            {
                return serve("arctic-proxy", || {
                    let store = OrderedArcticStore::new();
                    let reader: Arc<dyn ReadRouter> = Arc::new(store.reader());
                    (store, Some(reader))
                })
                .await;
            }
            #[cfg(not(feature = "arctic-proxy"))]
            bail!("the arctic target requires --features arctic-proxy");
        }
        _ => bail!("usage: proxy-bench-server <mem|arctic>"),
    }
}
