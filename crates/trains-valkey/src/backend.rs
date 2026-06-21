//! Real-engine backend: forward the delivered command stream to a co-located
//! `redis-server` / Valkey over RESP (PR-RD-4).
//!
//! This is the drop-in for [`crate::store::MemStore`] behind the
//! [`RedisStore`] seam: instead of an in-process model, each replica applies
//! the totally-ordered delivered effects to a **local** engine (bound to
//! loopback — see `bench/reports/trains-valkey-ec2-backend-research-2026-05-25.md`).
//! TRAINS still provides the cross-node replication; the engine is a plain
//! single-node store.
//!
//! ## Blocking I/O, by design
//! `RedisStore` is synchronous (so [`crate::replica::Replica`] stays an
//! I/O-free pure state machine). `RedisBackend` therefore uses a **blocking**
//! loopback connection and does one RESP round-trip per `apply`/`query`. In the
//! async proxy this briefly blocks the driver task — acceptable for a co-located
//! engine (sub-millisecond round-trips) and for the chaos validation; a
//! high-throughput production path would pipeline / move this to `spawn_blocking`.
//! Interior mutability ([`RefCell`]) lets `query(&self)` drive the connection;
//! the proxy's outer `Mutex` serializes all access, so the `RefCell` is never
//! contended across threads.

use std::cell::RefCell;
use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::path::Path;

use crate::command::Command;
use crate::resp::{encode_request, read_reply, Reply};
use crate::store::{RedisStore, SnapshotError};

/// One snapshot entry per key: `(key, pttl_ms, DUMP blob)`.
type SnapshotEntries = Vec<(Vec<u8>, i64, Vec<u8>)>;

/// The blocking transport to the local engine: a loopback TCP socket or a
/// UNIX domain socket (R-07 — the hardened deployment binds Valkey to a UDS
/// only, with `requirepass`, so no host process can reach it over TCP).
enum Stream {
    Tcp(TcpStream),
    Uds(UnixStream),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Uds(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            Stream::Uds(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            Stream::Uds(s) => s.flush(),
        }
    }
}

/// A [`RedisStore`] backed by a real `redis-server` / Valkey over a blocking
/// loopback RESP connection (TCP or UNIX domain socket).
pub struct RedisBackend {
    conn: RefCell<BufReader<Stream>>,
}

impl RedisBackend {
    fn from_stream(stream: Stream) -> Self {
        RedisBackend { conn: RefCell::new(BufReader::new(stream)) }
    }

    /// Connect to a co-located engine over TCP (typically `127.0.0.1:6379`).
    pub fn connect<A: ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        Ok(Self::from_stream(Stream::Tcp(stream)))
    }

    /// Connect to a co-located engine over a UNIX domain socket (R-07). The
    /// hardened deployment runs Valkey with `port 0` + `unixsocket` so the
    /// engine is unreachable over TCP at all.
    pub fn connect_uds<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let stream = UnixStream::connect(path)?;
        Ok(Self::from_stream(Stream::Uds(stream)))
    }

    /// `AUTH` if a password is configured. Factored out of the TCP/UDS
    /// constructors so both auth the same way.
    fn authed(self, password: Option<&str>) -> std::io::Result<Self> {
        if let Some(pw) = password {
            if let Reply::Error(e) = self.command(&[b"AUTH", pw.as_bytes()]) {
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, e));
            }
        }
        Ok(self)
    }

    /// Connect over TCP and `AUTH` if the engine has `requirepass` set (the
    /// recommended loopback config still uses a password as defense in depth).
    pub fn connect_auth<A: ToSocketAddrs>(
        addr: A,
        password: Option<&str>,
    ) -> std::io::Result<Self> {
        Self::connect(addr)?.authed(password)
    }

    /// Connect over a UNIX domain socket and `AUTH` if `requirepass` is set
    /// (R-07 — the sanctioned hardened path).
    pub fn connect_uds_auth<P: AsRef<Path>>(
        path: P,
        password: Option<&str>,
    ) -> std::io::Result<Self> {
        Self::connect_uds(path)?.authed(password)
    }

    /// One RESP round-trip. I/O failures surface as a RESP error reply so the
    /// apply path stays infallible (mirroring how the engine reports errors).
    fn command(&self, argv: &[&[u8]]) -> Reply {
        let mut conn = self.conn.borrow_mut();
        let req = encode_request(argv);
        if let Err(e) = conn.get_mut().write_all(&req).and_then(|()| conn.get_mut().flush()) {
            return Reply::error(format!("ERR backend write failed: {e}"));
        }
        match read_reply(&mut *conn) {
            Ok(r) => r,
            Err(e) => Reply::error(format!("ERR backend read failed: {e}")),
        }
    }

    fn argv_refs(cmd: &Command) -> Vec<&[u8]> {
        cmd.argv.iter().map(|v| v.as_slice()).collect()
    }
}

impl RedisStore for RedisBackend {
    fn apply(&mut self, cmd: &Command) -> Reply {
        self.command(&Self::argv_refs(cmd))
    }

    fn query(&self, cmd: &Command) -> Reply {
        self.command(&Self::argv_refs(cmd))
    }

    /// Full keyspace snapshot via `SCAN` + `DUMP`/`PTTL` per key. Same-engine
    /// `RESTORE` (import) consumes these blobs; all ring nodes run the same
    /// engine version so the RDB serialization version matches.
    fn export_snapshot(&self) -> Vec<u8> {
        let mut entries: SnapshotEntries = Vec::new();
        let mut cursor = b"0".to_vec();
        loop {
            let (next, keys) = match self.command(&[b"SCAN", &cursor, b"COUNT", b"512"]) {
                Reply::Array(items) if items.len() == 2 => {
                    let next = match &items[0] {
                        Reply::Bulk(b) => b.clone(),
                        _ => break,
                    };
                    let keys = match &items[1] {
                        Reply::Array(ks) => ks.clone(),
                        _ => Vec::new(),
                    };
                    (next, keys)
                }
                _ => break,
            };
            for k in keys {
                if let Reply::Bulk(key) = k {
                    let dump = match self.command(&[b"DUMP", &key]) {
                        Reply::Bulk(b) => b,
                        _ => continue, // key vanished mid-scan
                    };
                    let ttl = match self.command(&[b"PTTL", &key]) {
                        Reply::Integer(n) => n,
                        _ => -1,
                    };
                    entries.push((key, ttl, dump));
                }
            }
            if next == b"0" {
                break;
            }
            cursor = next;
        }
        bincode::serde::encode_to_vec(&entries, bincode::config::standard())
            .expect("snapshot entries always serialize")
    }

    fn import_snapshot(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let (entries, _): (SnapshotEntries, _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        self.command(&[b"FLUSHDB"]);
        for (key, ttl, dump) in entries {
            let ttl = if ttl < 0 { 0 } else { ttl }; // RESTORE: 0 = no expiry
            let ttl_s = ttl.to_string();
            self.command(&[b"RESTORE", &key, ttl_s.as_bytes(), &dump, b"REPLACE"]);
        }
        Ok(())
    }
}
