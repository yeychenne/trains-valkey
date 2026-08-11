#![cfg(feature = "arctic-proxy")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use trains_core::DeliveryMode;
use trains_net::{NodeIdentity, RingConfig};
use trains_valkey::proxy::{run_proxy_node_with_reader, ProxyConfig, ProxyHandle, ReadRouter};
use trains_valkey::{Command, OrderedArcticStore, RedisStore, Reply};

const RING: usize = 3;
const NUM_ISSUERS: usize = 2;

fn pick_port() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect RESP port");
        let (reader, writer) = stream.into_split();
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn cmd(&mut self, parts: &[&str]) -> Reply {
        let mut request = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            request.extend_from_slice(part.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        self.writer
            .write_all(&request)
            .await
            .expect("write request");
        self.writer.flush().await.expect("flush request");
        tokio::time::timeout(Duration::from_secs(20), read_reply(&mut self.reader))
            .await
            .expect("timed out awaiting reply")
            .expect("read reply")
    }
}

async fn read_line(reader: &mut BufReader<OwnedReadHalf>) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).await?;
    while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(line)
}

fn read_reply<'a>(
    reader: &'a mut BufReader<OwnedReadHalf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Reply>> + Send + 'a>> {
    Box::pin(async move {
        let line = read_line(reader).await?;
        if line.is_empty() {
            return Ok(Reply::error("ERR empty reply line"));
        }
        let body = String::from_utf8_lossy(&line[1..]).into_owned();
        Ok(match line[0] {
            b'+' => Reply::Simple(body),
            b'-' => Reply::Error(body),
            b':' => Reply::Integer(body.trim().parse().unwrap_or(0)),
            b'$' => {
                let length: i64 = body.trim().parse().unwrap_or(-1);
                if length < 0 {
                    Reply::Nil
                } else {
                    let mut buffer = vec![0u8; length as usize + 2];
                    reader.read_exact(&mut buffer).await?;
                    buffer.truncate(length as usize);
                    Reply::Bulk(buffer)
                }
            }
            marker => Reply::error(format!("ERR unexpected reply marker {marker}")),
        })
    })
}

async fn spawn_ring() -> (Vec<ProxyHandle<OrderedArcticStore>>, Vec<SocketAddr>) {
    let identities: Vec<_> = (0..RING)
        .map(|_| NodeIdentity::generate(vec!["localhost".to_string()]).unwrap())
        .collect();
    let fingerprints: Vec<_> = identities
        .iter()
        .map(|identity| identity.fingerprint)
        .collect();
    let ring_addrs: Vec<_> = (0..RING).map(|_| pick_port()).collect();
    let resp_addrs: Vec<_> = (0..RING).map(|_| pick_port()).collect();

    let mut handles = Vec::new();
    let mut bound_resp = Vec::new();
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
                successor_addr: ring_addrs[(index + 1) % RING],
                pinned_peer_fingerprints: fingerprints.clone(),
            },
            ring_addrs: vec![],
            snapshot_server: None,
            rejoin: None,
        };
        let store = OrderedArcticStore::new();
        let reader: Arc<dyn ReadRouter> = Arc::new(store.reader());
        let handle = run_proxy_node_with_reader(config, store, Some(reader))
            .await
            .expect("spawn Arctic proxy node");
        bound_resp.push(handle.resp_addr);
        handles.push(handle);
    }
    (handles, bound_resp)
}

async fn await_get(addr: SocketAddr, key: &str, expected: &[u8]) {
    let mut client = Client::connect(addr).await;
    for _ in 0..100 {
        if client.cmd(&["GET", key]).await == Reply::Bulk(expected.to_vec()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("replica at {addr} did not converge GET {key}");
}

fn command(parts: &[&str]) -> Command {
    Command::parse(parts.iter().map(|part| part.as_bytes().to_vec()).collect()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn arctic_backend_runs_through_the_real_resp_ring() {
    let (handles, resp) = spawn_ring().await;
    let mut origin = Client::connect(resp[0]).await;

    assert_eq!(origin.cmd(&["SET", "hot", "initial"]).await, Reply::ok());
    assert_eq!(
        origin.cmd(&["GET", "hot"]).await,
        Reply::Bulk(b"initial".to_vec()),
        "same-node read after write acknowledgement"
    );
    for &addr in &resp {
        await_get(addr, "hot", b"initial").await;
    }

    assert_eq!(origin.cmd(&["EXISTS", "hot"]).await, Reply::Integer(1));
    assert_eq!(
        origin.cmd(&["EXISTS", "hot", "missing"]).await,
        Reply::Integer(1)
    );
    assert_eq!(origin.cmd(&["DBSIZE"]).await, Reply::Integer(1));

    // Holding the ordered store mutex proves this GET does not use that path.
    let mut fast_client = Client::connect(resp[0]).await;
    let ordered_store = Arc::clone(&handles[0].store);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_thread = std::thread::spawn(move || {
        let _ordered_guard = ordered_store.lock().expect("ordered store mutex");
        locked_tx.send(()).expect("report locked store");
        release_rx.recv().expect("release ordered store");
    });
    locked_rx.recv().expect("ordered store lock confirmation");
    let fast_reply = tokio::time::timeout(Duration::from_secs(1), fast_client.cmd(&["GET", "hot"]))
        .await
        .expect("GET blocked on the ordered store mutex");
    assert_eq!(fast_reply, Reply::Bulk(b"initial".to_vec()));
    release_tx.send(()).expect("release ordered store");
    lock_thread.join().expect("ordered store lock thread");

    let start_reads = Arc::new(tokio::sync::Barrier::new(5));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let addr = resp[0];
        let start_reads = Arc::clone(&start_reads);
        readers.push(tokio::spawn(async move {
            let mut client = Client::connect(addr).await;
            start_reads.wait().await;
            for _ in 0..100 {
                assert!(matches!(client.cmd(&["GET", "hot"]).await, Reply::Bulk(_)));
            }
        }));
    }
    start_reads.wait().await;
    for version in 0..100 {
        let value = format!("value-{version:03}");
        assert_eq!(origin.cmd(&["SET", "hot", &value]).await, Reply::ok());
    }
    for reader in readers {
        reader.await.expect("reader task");
    }
    for &addr in &resp {
        await_get(addr, "hot", b"value-099").await;
    }

    // Model a state-transfer install while RESP point reads remain active.
    let mut source = OrderedArcticStore::new();
    source.apply(&command(&["SET", "restored", "snapshot"]));
    let snapshot = source.export_snapshot();
    let addr = resp[0];
    let (observing_tx, observing_rx) = tokio::sync::oneshot::channel();
    let observer = tokio::spawn(async move {
        let mut client = Client::connect(addr).await;
        assert_eq!(client.cmd(&["GET", "restored"]).await, Reply::Nil);
        observing_tx
            .send(())
            .expect("report active snapshot observer");
        for _ in 0..100 {
            assert!(matches!(
                client.cmd(&["GET", "restored"]).await,
                Reply::Nil | Reply::Bulk(_)
            ));
        }
    });
    observing_rx.await.expect("snapshot observer started");
    handles[0]
        .store
        .lock()
        .expect("ordered store mutex")
        .import_snapshot(&snapshot)
        .expect("install Arctic snapshot");
    observer.await.expect("snapshot observer");
    await_get(resp[0], "restored", b"snapshot").await;
}
