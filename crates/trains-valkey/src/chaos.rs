//! Chaos workload + **no-acked-write-loss** verifier (PR-RD-4 / G5).
//!
//! The existing TRAINS bench coordinator measures *total-order delivery*; the
//! Redis chaos run needs the store-specific property instead: **every write the
//! client received `+OK` for survives a node crash on every surviving replica,
//! and survivors converge.** That is precisely what Redis async/Sentinel
//! failover does *not* guarantee (acked writes are lost on failover), so it is
//! the headline comparison.
//!
//! This module is deliberately **synchronous** (blocking sockets + the sync
//! [`crate::resp::read_reply`] / [`crate::backend::RedisBackend`]) so the driver
//! is a plain binary with no async runtime — the fault is injected out-of-band
//! by the bench coordinator (`bench/coordinator/faults.py`, fis-kill via SSM)
//! during the load's hold window.
//!
//! Flow for a run:
//! 1. `run_load(proxy, 0..N/2)` — write a monotonic `SET` stream, recording each
//!    key **only after `+OK`** (the *acked* set);
//! 2. hold while the coordinator `fis-kill`s a node;
//! 3. `run_load(proxy, N/2..N)` — keep writing *through* the masked window;
//! 4. [`verify`] every **surviving** engine: assert no acked write is missing
//!    and the survivors converged.

use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ops::Range;
use std::time::Duration;

use crate::backend::RedisBackend;
use crate::command::Command;
use crate::resp::{encode_request, read_reply, Reply};
use crate::store::RedisStore;

/// The set of writes the proxy acknowledged with `+OK`.
///
/// Keys/values are bytes internally; the JSON I/O helpers
/// ([`AckedWrites::write_json`] / [`AckedWrites::read_json`]) assume the chaos
/// workload's ASCII payloads (`chaos:k<N>` / `v<N>`) and surface non-UTF-8 as an
/// `io::Error`. That's enough for the EC2 chaos driver (option-B split: load on
/// one node writes the acked set, each survivor reads it for `verify-local`).
#[derive(Debug, Default, Clone)]
pub struct AckedWrites {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl AckedWrites {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn extend(&mut self, other: AckedWrites) {
        self.entries.extend(other.entries);
    }

    /// Serialize as JSON `[[key, val], ...]` (ASCII assumption — see type docs).
    pub fn write_json<W: Write>(&self, mut w: W) -> io::Result<()> {
        let view: Vec<(String, String)> = self
            .entries
            .iter()
            .map(|(k, v)| {
                Ok((
                    String::from_utf8(k.clone()).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("non-utf8 key: {e}"))
                    })?,
                    String::from_utf8(v.clone()).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("non-utf8 val: {e}"))
                    })?,
                ))
            })
            .collect::<io::Result<_>>()?;
        serde_json::to_writer(&mut w, &view)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        w.write_all(b"\n")
    }

    /// Inverse of [`AckedWrites::write_json`].
    pub fn read_json<R: Read>(mut r: R) -> io::Result<Self> {
        let mut buf = String::new();
        r.read_to_string(&mut buf)?;
        let view: Vec<(String, String)> = serde_json::from_str(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Self {
            entries: view.into_iter().map(|(k, v)| (k.into_bytes(), v.into_bytes())).collect(),
        })
    }
}

/// One RESP connection to the proxy, with an optional per-reply read timeout.
struct LoadConn {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl LoadConn {
    fn connect(resp_addr: SocketAddr, abandon: Option<Duration>) -> io::Result<Self> {
        let stream = TcpStream::connect(resp_addr)?;
        stream.set_nodelay(true).ok();
        // `None` ⇒ block forever (original behaviour); `Some(d)` ⇒ a read that
        // produces no byte within `d` returns WouldBlock/TimedOut.
        stream.set_read_timeout(abandon)?;
        let writer = stream.try_clone()?;
        Ok(Self { writer, reader: BufReader::new(stream) })
    }

    /// Send one `SET` and read its reply. `Ok(true)` ⇒ `+OK`; `Ok(false)` ⇒ a
    /// non-OK reply (not acked); `Err` ⇒ timeout or disconnect (caller abandons).
    fn set_and_read(&mut self, req: &[u8]) -> io::Result<bool> {
        self.writer.write_all(req)?;
        self.writer.flush()?;
        Ok(matches!(read_reply(&mut self.reader)?, Reply::Simple(s) if s == "OK"))
    }
}

/// A read timeout or a dead peer — the conditions under which a write is
/// abandoned (its reply never arrived, so it was never acked) and the
/// connection is rebuilt.
fn is_abandon(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    matches!(
        e.kind(),
        WouldBlock | TimedOut | UnexpectedEof | ConnectionReset | BrokenPipe | ConnectionAborted
    )
}

/// Write `SET {prefix}{i} v{i}` for `i` in `range` to a proxy's RESP port,
/// recording each key/value **only after** the proxy returns `+OK` (so the set
/// reflects genuinely-acked writes — the ones that must not be lost).
///
/// `abandon`: per-write reply deadline. `None` blocks forever (legacy). `Some(d)`
/// is the chaos analogue of the Python driver's `--abandon-secs`: a write whose
/// `+OK` doesn't arrive within `d` (e.g. its train was lost in a masked crash) is
/// **abandoned** — left out of the acked set (it was never acked, so dropping it
/// is *not* acked-write loss) — and a fresh connection is opened so a late `+OK`
/// can't desync the next write's reply. Without this the load hangs forever on
/// the single write in-flight at crash time (observed on EC2, 2026-06-15).
pub fn run_load(
    resp_addr: SocketAddr,
    prefix: &str,
    range: Range<usize>,
    abandon: Option<Duration>,
) -> io::Result<AckedWrites> {
    let mut conn = LoadConn::connect(resp_addr, abandon)?;
    let mut acked = AckedWrites::default();
    let mut abandoned = 0usize;
    for i in range {
        let key = format!("{prefix}{i}").into_bytes();
        let val = format!("v{i}").into_bytes();
        let req = encode_request(&[b"SET", &key, &val]);
        match conn.set_and_read(&req) {
            Ok(true) => acked.entries.push((key, val)),
            Ok(false) => {} // non-OK reply ⇒ not acked
            Err(e) if is_abandon(&e) => {
                abandoned += 1;
                // The connection may be mid-reply (a late +OK would be read as
                // the *next* write's reply); reconnect so the measurement stays
                // aligned. If we can't even reconnect, surface the error.
                conn = LoadConn::connect(resp_addr, abandon)?;
            }
            Err(e) => return Err(e),
        }
    }
    if abandoned > 0 {
        eprintln!(
            "[chaos] abandoned {abandoned} write(s): no +OK within {abandon:?} \
             (not counted as acked)"
        );
    }
    Ok(acked)
}

/// Result of checking the acked set against the surviving engines.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VerifyReport {
    pub engines: usize,
    pub acked_total: usize,
    /// Keys that an acked write got `+OK` for but are missing/wrong on at least
    /// one surviving engine — an **acked-write loss** (the failure mode TRAINS
    /// masking is meant to prevent). Must be empty.
    pub acked_loss: Vec<String>,
    /// `DBSIZE` of each surviving engine (should all match).
    pub dbsizes: Vec<i64>,
    /// True iff all survivors report the same `DBSIZE`.
    pub converged: bool,
}

impl VerifyReport {
    /// The run passes iff no acked write was lost and survivors converged.
    pub fn ok(&self) -> bool {
        self.acked_loss.is_empty() && self.converged
    }
}

/// Read each **surviving** engine directly and confirm every acked write is
/// present with the acked value, and that the survivors converged.
pub fn verify(
    engines: &[SocketAddr],
    password: Option<&str>,
    acked: &AckedWrites,
) -> io::Result<VerifyReport> {
    let backends: Vec<RedisBackend> = engines
        .iter()
        .map(|a| RedisBackend::connect_auth(*a, password))
        .collect::<io::Result<_>>()?;

    let mut acked_loss = Vec::new();
    for (k, v) in &acked.entries {
        let get = Command::parse(vec![b"GET".to_vec(), k.clone()]).expect("non-empty");
        let present_everywhere = backends
            .iter()
            .all(|be| be.query(&get) == Reply::Bulk(v.clone()));
        if !present_everywhere {
            acked_loss.push(String::from_utf8_lossy(k).into_owned());
        }
    }

    let dbsize = Command::parse(vec![b"DBSIZE".to_vec()]).expect("non-empty");
    let dbsizes: Vec<i64> = backends
        .iter()
        .map(|be| match be.query(&dbsize) {
            Reply::Integer(n) => n,
            _ => -1,
        })
        .collect();
    let converged = dbsizes.windows(2).all(|w| w[0] == w[1]);

    Ok(VerifyReport {
        engines: engines.len(),
        acked_total: acked.entries.len(),
        acked_loss,
        dbsizes,
        converged,
    })
}

/// One survivor's view of the acked set + its DBSIZE. Emitted by
/// `trains-valkey-chaos --mode verify-local` so a separate coordinator process
/// can aggregate across survivors without ever exposing the engine port off-box.
/// This is the option-B split (loopback Valkey + verify on each node) that
/// preserves the runbook's "Valkey bound to 127.0.0.1 only" guidance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartialReport {
    /// Operator-supplied label so the aggregator can identify the survivor
    /// (e.g. `node-1`, an instance id, or a private IP).
    pub engine_label: String,
    pub acked_total: usize,
    /// Acked keys that are missing or mismatched on *this* engine.
    pub missing_keys: Vec<String>,
    /// `DBSIZE` reported by this engine.
    pub dbsize: i64,
}

/// Verify a single engine against the acked set (one-of-N piece of [`verify`]).
/// Run on each survivor by `trains-valkey-chaos --mode verify-local`.
pub fn verify_one(
    engine: SocketAddr,
    password: Option<&str>,
    label: &str,
    acked: &AckedWrites,
) -> io::Result<PartialReport> {
    let backend = RedisBackend::connect_auth(engine, password)?;

    let mut missing_keys = Vec::new();
    for (k, v) in &acked.entries {
        let get = Command::parse(vec![b"GET".to_vec(), k.clone()]).expect("non-empty");
        if backend.query(&get) != Reply::Bulk(v.clone()) {
            missing_keys.push(String::from_utf8_lossy(k).into_owned());
        }
    }

    let dbsize = Command::parse(vec![b"DBSIZE".to_vec()]).expect("non-empty");
    let dbsize = match backend.query(&dbsize) {
        Reply::Integer(n) => n,
        _ => -1,
    };

    Ok(PartialReport {
        engine_label: label.to_owned(),
        acked_total: acked.entries.len(),
        missing_keys,
        dbsize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PR-RD-4 option B: `--mode load` writes the acked set to disk and each
    /// survivor's `--mode verify-local` reads it back. Round-trip must be
    /// lossless or the per-survivor verifier will spuriously flag missing keys.
    #[test]
    fn acked_writes_json_roundtrip() {
        let mut a = AckedWrites::default();
        a.entries.push((b"chaos:k0".to_vec(), b"v0".to_vec()));
        a.entries.push((b"chaos:k1".to_vec(), b"v1".to_vec()));
        a.entries.push((b"chaos:k999".to_vec(), b"v999".to_vec()));

        let mut buf = Vec::new();
        a.write_json(&mut buf).expect("write_json");
        let back = AckedWrites::read_json(&buf[..]).expect("read_json");

        assert_eq!(back.len(), a.len());
        assert_eq!(back.entries, a.entries);
    }

    /// A mock RESP server that replies `+OK` to the first `ok_first` requests it
    /// ever sees (across reconnects), then goes silent — reads further requests
    /// but never replies. Models a proxy that stops acking mid-load (a masked
    /// crash). Accepts repeatedly so the client can reconnect.
    fn spawn_mock(ok_first: usize) -> SocketAddr {
        use std::io::Read as _;
        use std::net::TcpListener;
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut served = 0usize;
            for conn in l.incoming() {
                let Ok(mut s) = conn else { continue };
                let mut buf = [0u8; 512];
                loop {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break, // client reconnected / closed
                        Ok(_) => {
                            // One read ≈ one pipelined SET for these tiny payloads.
                            if served < ok_first {
                                served += 1;
                                if s.write_all(b"+OK\r\n").is_err() {
                                    break;
                                }
                            }
                            // else: silent — force the client to time out + abandon.
                        }
                    }
                }
            }
        });
        addr
    }

    /// The abandon timeout must let the load **complete** (not hang) when the
    /// proxy stops acking, recording only the genuinely-acked writes.
    #[test]
    fn run_load_abandons_unacked_and_completes() {
        let addr = spawn_mock(2); // first 2 writes get +OK, the rest go silent
        let acked = run_load(addr, "chaos:k", 0..5, Some(Duration::from_millis(200)))
            .expect("run_load should return, not hang");
        // Exactly the 2 acked writes are recorded; the 3 silent ones are dropped.
        assert_eq!(acked.len(), 2, "only +OK'd writes count as acked");
        assert_eq!(acked.entries[0].0, b"chaos:k0");
        assert_eq!(acked.entries[1].0, b"chaos:k1");
    }
}
