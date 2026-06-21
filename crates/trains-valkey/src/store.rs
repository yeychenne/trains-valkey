//! The local key-value store a replica applies the delivered command stream to.
//!
//! In RD-1 the concrete store is [`MemStore`], an in-process deterministic
//! model of the Redis command subset. It stands in for "local Redis" so the
//! replication *mechanism* (intercept → total-order broadcast → apply) can be
//! tested for convergence without a running `redis-server` (none is required in
//! CI, and none is available on the dev box). The [`RedisStore`] trait is the
//! seam where a real-Redis backend drops in for the EC2 chaos work (PR-RD-4):
//! same `apply`/`query` contract, bytes forwarded to a co-located server.
//!
//! Determinism is the load-bearing property: feeding the **same** ordered
//! command stream to two `RedisStore`s must leave them in the **same** state.
//! `MemStore` therefore avoids wall-clock effects — `EXPIRE` records the TTL but
//! never actively evicts (time-based eviction is non-deterministic and is
//! handled by effect replication in PR-RD-2).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::resp::Reply;

/// The apply/query contract every replica store implements.
pub trait RedisStore {
    /// Apply a mutating command (already known deterministic) and return the
    /// reply the originating client should receive.
    fn apply(&mut self, cmd: &Command) -> Reply;

    /// Answer a read-only command from local state.
    fn query(&self, cmd: &Command) -> Reply;

    /// Serialize the full store state for replica **state transfer** (PR-RD-3):
    /// a rejoining/new replica imports a survivor's snapshot to catch up the
    /// whole keyspace (not just the live tail). The bytes are opaque; a real
    /// `redis-server` backend would use `DUMP`/RDB here.
    fn export_snapshot(&self) -> Vec<u8>;

    /// Replace the store state from bytes produced by [`RedisStore::export_snapshot`].
    fn import_snapshot(&mut self, bytes: &[u8]) -> Result<(), SnapshotError>;
}

/// Failure decoding a store snapshot during state transfer.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}

/// A stored value. Models strings, hashes (RD-1) and sets (RD-2, needed so
/// `SPOP`'s resolved `SREM` effect has a type to act on) — enough for AO's
/// coordination keys (locks, status flags, dispatch counters, member sets) and
/// the convergence tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Value {
    Str(Vec<u8>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
}

/// Deterministic in-memory Redis model.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemStore {
    data: BTreeMap<Vec<u8>, Value>,
    /// Logical TTLs in seconds, recorded by `EXPIRE` but never actively fired
    /// (active expiry is non-deterministic — see module docs / PR-RD-2).
    ttl: BTreeMap<Vec<u8>, i64>,
}

impl MemStore {
    pub fn new() -> Self {
        MemStore::default()
    }

    /// A deterministic, fully-ordered snapshot of the keyspace, for convergence
    /// assertions in tests. Two stores are in the same state iff their snapshots
    /// are equal.
    pub fn snapshot_sorted(&self) -> Vec<(Vec<u8>, StoreEntry)> {
        self.data
            .iter()
            .map(|(k, v)| {
                let entry = match v {
                    Value::Str(s) => StoreEntry::Str(s.clone()),
                    Value::Hash(h) => {
                        StoreEntry::Hash(h.iter().map(|(f, v)| (f.clone(), v.clone())).collect())
                    }
                    Value::Set(set) => StoreEntry::Set(set.iter().cloned().collect()),
                };
                (k.clone(), entry)
            })
            .collect()
    }

    pub fn key_count(&self) -> usize {
        self.data.len()
    }

    fn as_int(b: &[u8]) -> Option<i64> {
        std::str::from_utf8(b).ok()?.trim().parse().ok()
    }
}

/// Public, comparable view of one stored value (for tests / snapshots).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreEntry {
    Str(Vec<u8>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Set(Vec<Vec<u8>>),
}

impl RedisStore for MemStore {
    fn apply(&mut self, cmd: &Command) -> Reply {
        match cmd.name.as_str() {
            "SET" => {
                let (Some(k), Some(v)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("set");
                };
                self.data.insert(k.to_vec(), Value::Str(v.to_vec()));
                self.ttl.remove(k); // SET clears any TTL
                Reply::ok()
            }
            "SETNX" => {
                let (Some(k), Some(v)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("setnx");
                };
                if self.data.contains_key(k) {
                    Reply::Integer(0)
                } else {
                    self.data.insert(k.to_vec(), Value::Str(v.to_vec()));
                    Reply::Integer(1)
                }
            }
            "GETSET" => {
                let (Some(k), Some(v)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("getset");
                };
                let prev = match self.data.insert(k.to_vec(), Value::Str(v.to_vec())) {
                    Some(Value::Str(s)) => Reply::Bulk(s),
                    Some(_) => return wrongtype(),
                    None => Reply::Nil,
                };
                self.ttl.remove(k);
                prev
            }
            "APPEND" => {
                let (Some(k), Some(v)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("append");
                };
                let entry = self.data.entry(k.to_vec()).or_insert_with(|| Value::Str(Vec::new()));
                match entry {
                    Value::Str(s) => {
                        s.extend_from_slice(v);
                        Reply::Integer(s.len() as i64)
                    }
                    _ => wrongtype(),
                }
            }
            "DEL" | "UNLINK" => {
                let mut removed = 0i64;
                for i in 1..cmd.argv.len() {
                    if let Some(k) = cmd.arg(i) {
                        if self.data.remove(k).is_some() {
                            removed += 1;
                        }
                        self.ttl.remove(k);
                    }
                }
                Reply::Integer(removed)
            }
            "INCR" | "DECR" | "INCRBY" | "DECRBY" => self.apply_incr(cmd),
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
                let Some(k) = cmd.key() else { return wrongargs("expire") };
                if !self.data.contains_key(k) {
                    return Reply::Integer(0);
                }
                let secs = cmd.arg(2).and_then(MemStore::as_int).unwrap_or(0);
                self.ttl.insert(k.to_vec(), secs);
                Reply::Integer(1)
            }
            "PERSIST" => {
                let Some(k) = cmd.key() else { return wrongargs("persist") };
                if self.ttl.remove(k).is_some() {
                    Reply::Integer(1)
                } else {
                    Reply::Integer(0)
                }
            }
            "RENAME" => {
                let (Some(src), Some(dst)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("rename");
                };
                match self.data.remove(src) {
                    Some(v) => {
                        let t = self.ttl.remove(src);
                        self.data.insert(dst.to_vec(), v);
                        match t {
                            Some(t) => {
                                self.ttl.insert(dst.to_vec(), t);
                            }
                            None => {
                                self.ttl.remove(dst);
                            }
                        }
                        Reply::ok()
                    }
                    None => Reply::error("ERR no such key"),
                }
            }
            "HSET" | "HMSET" => self.apply_hset(cmd),
            "HSETNX" => {
                let (Some(k), Some(f), Some(v)) = (cmd.arg(1), cmd.arg(2), cmd.arg(3)) else {
                    return wrongargs("hsetnx");
                };
                let h = match self.data.entry(k.to_vec()).or_insert_with(|| Value::Hash(BTreeMap::new())) {
                    Value::Hash(h) => h,
                    _ => return wrongtype(),
                };
                if h.contains_key(f) {
                    Reply::Integer(0)
                } else {
                    h.insert(f.to_vec(), v.to_vec());
                    Reply::Integer(1)
                }
            }
            "HDEL" => {
                let Some(k) = cmd.key() else { return wrongargs("hdel") };
                let mut removed = 0i64;
                if let Some(Value::Hash(h)) = self.data.get_mut(k) {
                    for i in 2..cmd.argv.len() {
                        if let Some(f) = cmd.arg(i) {
                            if h.remove(f).is_some() {
                                removed += 1;
                            }
                        }
                    }
                    if h.is_empty() {
                        self.data.remove(k);
                    }
                } else if self.data.contains_key(k) {
                    return wrongtype();
                }
                Reply::Integer(removed)
            }
            "HINCRBY" => self.apply_hincrby(cmd),
            "SADD" => {
                let Some(k) = cmd.key() else { return wrongargs("sadd") };
                let set = match self.data.entry(k.to_vec()).or_insert_with(|| Value::Set(BTreeSet::new())) {
                    Value::Set(s) => s,
                    _ => return wrongtype(),
                };
                let mut added = 0i64;
                for i in 2..cmd.argv.len() {
                    if let Some(m) = cmd.arg(i) {
                        if set.insert(m.to_vec()) {
                            added += 1;
                        }
                    }
                }
                Reply::Integer(added)
            }
            "SREM" => {
                let Some(k) = cmd.key() else { return wrongargs("srem") };
                let mut removed = 0i64;
                match self.data.get_mut(k) {
                    Some(Value::Set(s)) => {
                        for i in 2..cmd.argv.len() {
                            if let Some(m) = cmd.arg(i) {
                                if s.remove(m) {
                                    removed += 1;
                                }
                            }
                        }
                        if s.is_empty() {
                            self.data.remove(k);
                        }
                    }
                    Some(_) => return wrongtype(),
                    None => {}
                }
                Reply::Integer(removed)
            }
            "FLUSHDB" | "FLUSHALL" => {
                self.data.clear();
                self.ttl.clear();
                Reply::ok()
            }
            // Classified as a deterministic write but not modelled by MemStore.
            // Returning a uniform error keeps replicas convergent (every node
            // errors identically and mutates nothing); a real-Redis backend
            // (PR-RD-4) handles these natively.
            other => Reply::error(format!(
                "ERR '{other}' is a write but not implemented by MemStore (RD-1); use the redis backend"
            )),
        }
    }

    fn query(&self, cmd: &Command) -> Reply {
        match cmd.name.as_str() {
            "PING" => match cmd.arg(1) {
                Some(msg) => Reply::Bulk(msg.to_vec()),
                None => Reply::Simple("PONG".to_string()),
            },
            "ECHO" => match cmd.arg(1) {
                Some(msg) => Reply::Bulk(msg.to_vec()),
                None => wrongargs("echo"),
            },
            "GET" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Str(s)) => Reply::Bulk(s.clone()),
                Some(_) => wrongtype(),
                None => Reply::Nil,
            },
            "STRLEN" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Str(s)) => Reply::Integer(s.len() as i64),
                Some(_) => wrongtype(),
                None => Reply::Integer(0),
            },
            "MGET" => {
                let mut out = Vec::new();
                for i in 1..cmd.argv.len() {
                    let k = cmd.arg(i).unwrap_or(b"");
                    match self.data.get(k) {
                        Some(Value::Str(s)) => out.push(Reply::Bulk(s.clone())),
                        _ => out.push(Reply::Nil),
                    }
                }
                Reply::Array(out)
            }
            "EXISTS" => {
                let mut n = 0i64;
                for i in 1..cmd.argv.len() {
                    if let Some(k) = cmd.arg(i) {
                        if self.data.contains_key(k) {
                            n += 1;
                        }
                    }
                }
                Reply::Integer(n)
            }
            "TYPE" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Str(_)) => Reply::Simple("string".to_string()),
                Some(Value::Hash(_)) => Reply::Simple("hash".to_string()),
                Some(Value::Set(_)) => Reply::Simple("set".to_string()),
                None => Reply::Simple("none".to_string()),
            },
            "TTL" => {
                let k = cmd.key().unwrap_or(b"");
                if !self.data.contains_key(k) {
                    Reply::Integer(-2)
                } else {
                    Reply::Integer(self.ttl.get(k).copied().unwrap_or(-1))
                }
            }
            "DBSIZE" => Reply::Integer(self.data.len() as i64),
            "KEYS" => {
                // Deterministic order (BTreeMap). RD-1 ignores the glob pattern
                // and returns all keys — sufficient for the coordination plane.
                let keys: Vec<Reply> =
                    self.data.keys().map(|k| Reply::Bulk(k.clone())).collect();
                Reply::Array(keys)
            }
            "HGET" => {
                let (Some(k), Some(f)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("hget");
                };
                match self.data.get(k) {
                    Some(Value::Hash(h)) => match h.get(f) {
                        Some(v) => Reply::Bulk(v.clone()),
                        None => Reply::Nil,
                    },
                    Some(_) => wrongtype(),
                    None => Reply::Nil,
                }
            }
            "HMGET" => {
                let Some(k) = cmd.key() else { return wrongargs("hmget") };
                let h = match self.data.get(k) {
                    Some(Value::Hash(h)) => Some(h),
                    Some(_) => return wrongtype(),
                    None => None,
                };
                let mut out = Vec::new();
                for i in 2..cmd.argv.len() {
                    let f = cmd.arg(i).unwrap_or(b"");
                    match h.and_then(|h| h.get(f)) {
                        Some(v) => out.push(Reply::Bulk(v.clone())),
                        None => out.push(Reply::Nil),
                    }
                }
                Reply::Array(out)
            }
            "HGETALL" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Hash(h)) => {
                    let mut out = Vec::with_capacity(h.len() * 2);
                    for (f, v) in h {
                        out.push(Reply::Bulk(f.clone()));
                        out.push(Reply::Bulk(v.clone()));
                    }
                    Reply::Array(out)
                }
                Some(_) => wrongtype(),
                None => Reply::Array(vec![]),
            },
            "HKEYS" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Hash(h)) => {
                    Reply::Array(h.keys().map(|f| Reply::Bulk(f.clone())).collect())
                }
                Some(_) => wrongtype(),
                None => Reply::Array(vec![]),
            },
            "HVALS" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Hash(h)) => {
                    Reply::Array(h.values().map(|v| Reply::Bulk(v.clone())).collect())
                }
                Some(_) => wrongtype(),
                None => Reply::Array(vec![]),
            },
            "HLEN" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Hash(h)) => Reply::Integer(h.len() as i64),
                Some(_) => wrongtype(),
                None => Reply::Integer(0),
            },
            "HEXISTS" => {
                let (Some(k), Some(f)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("hexists");
                };
                match self.data.get(k) {
                    Some(Value::Hash(h)) => Reply::Integer(h.contains_key(f) as i64),
                    Some(_) => wrongtype(),
                    None => Reply::Integer(0),
                }
            }
            // ── Set reads ──
            "SMEMBERS" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Set(s)) => Reply::Array(s.iter().map(|m| Reply::Bulk(m.clone())).collect()),
                Some(_) => wrongtype(),
                None => Reply::Array(vec![]),
            },
            "SCARD" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Set(s)) => Reply::Integer(s.len() as i64),
                Some(_) => wrongtype(),
                None => Reply::Integer(0),
            },
            "SISMEMBER" => {
                let (Some(k), Some(m)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("sismember");
                };
                match self.data.get(k) {
                    Some(Value::Set(s)) => Reply::Integer(s.contains(m) as i64),
                    Some(_) => wrongtype(),
                    None => Reply::Integer(0),
                }
            }
            // ── Non-deterministic reads, answered with a *deterministic* pick so
            //    no two replicas can diverge state (PR-RD-2). They never mutate.
            "SRANDMEMBER" => match self.data.get(cmd.key().unwrap_or(b"")) {
                // Deterministic: first member in iteration order (BTreeSet).
                Some(Value::Set(s)) => match s.iter().next() {
                    Some(m) => Reply::Bulk(m.clone()),
                    None => Reply::Nil,
                },
                Some(_) => wrongtype(),
                None => Reply::Nil,
            },
            "RANDOMKEY" => match self.data.keys().next() {
                Some(k) => Reply::Bulk(k.clone()),
                None => Reply::Nil,
            },
            "TIME" => {
                // Each replica answers from its own clock; this never feeds state.
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                Reply::Array(vec![
                    Reply::Bulk(now.as_secs().to_string().into_bytes()),
                    Reply::Bulk(now.subsec_micros().to_string().into_bytes()),
                ])
            }
            // Cursor scans: deterministic single-shot (cursor "0" returns the
            // full, ordered result; pattern/COUNT ignored — RD-2 skeleton).
            "SCAN" => {
                let keys: Vec<Reply> = self.data.keys().map(|k| Reply::Bulk(k.clone())).collect();
                Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(keys)])
            }
            "SSCAN" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Set(s)) => {
                    let members: Vec<Reply> = s.iter().map(|m| Reply::Bulk(m.clone())).collect();
                    Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(members)])
                }
                Some(_) => wrongtype(),
                None => Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(vec![])]),
            },
            "HSCAN" => match self.data.get(cmd.key().unwrap_or(b"")) {
                Some(Value::Hash(h)) => {
                    let mut pairs = Vec::with_capacity(h.len() * 2);
                    for (f, v) in h {
                        pairs.push(Reply::Bulk(f.clone()));
                        pairs.push(Reply::Bulk(v.clone()));
                    }
                    Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(pairs)])
                }
                Some(_) => wrongtype(),
                None => Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(vec![])]),
            },
            other => Reply::error(format!(
                "ERR '{other}' is a read but not implemented by MemStore"
            )),
        }
    }

    fn export_snapshot(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("MemStore is always serializable")
    }

    fn import_snapshot(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let (restored, _) =
            bincode::serde::decode_from_slice::<MemStore, _>(bytes, bincode::config::standard())?;
        *self = restored;
        Ok(())
    }
}

impl MemStore {
    fn apply_incr(&mut self, cmd: &Command) -> Reply {
        let Some(k) = cmd.key() else { return wrongargs("incr") };
        let delta = match cmd.name.as_str() {
            "INCR" => 1,
            "DECR" => -1,
            "INCRBY" => match cmd.arg(2).and_then(MemStore::as_int) {
                Some(n) => n,
                None => return Reply::error("ERR value is not an integer or out of range"),
            },
            "DECRBY" => match cmd.arg(2).and_then(MemStore::as_int) {
                Some(n) => -n,
                None => return Reply::error("ERR value is not an integer or out of range"),
            },
            _ => unreachable!(),
        };
        let cur = match self.data.get(k) {
            Some(Value::Str(s)) => match MemStore::as_int(s) {
                Some(n) => n,
                None => return Reply::error("ERR value is not an integer or out of range"),
            },
            Some(_) => return wrongtype(),
            None => 0,
        };
        let next = match cur.checked_add(delta) {
            Some(n) => n,
            None => return Reply::error("ERR increment or decrement would overflow"),
        };
        self.data
            .insert(k.to_vec(), Value::Str(next.to_string().into_bytes()));
        Reply::Integer(next)
    }

    fn apply_hset(&mut self, cmd: &Command) -> Reply {
        let Some(k) = cmd.key() else { return wrongargs("hset") };
        // Field/value pairs start at argv[2].
        if cmd.argv.len() < 4 || (cmd.argv.len() - 2) % 2 != 0 {
            return wrongargs("hset");
        }
        let h = match self.data.entry(k.to_vec()).or_insert_with(|| Value::Hash(BTreeMap::new())) {
            Value::Hash(h) => h,
            _ => return wrongtype(),
        };
        let mut added = 0i64;
        let mut i = 2;
        while i + 1 < cmd.argv.len() {
            let f = cmd.argv[i].clone();
            let v = cmd.argv[i + 1].clone();
            if h.insert(f, v).is_none() {
                added += 1;
            }
            i += 2;
        }
        // HMSET returns +OK; HSET returns the count of new fields.
        if cmd.name == "HMSET" {
            Reply::ok()
        } else {
            Reply::Integer(added)
        }
    }

    fn apply_hincrby(&mut self, cmd: &Command) -> Reply {
        let (Some(k), Some(f), Some(by)) = (cmd.arg(1), cmd.arg(2), cmd.arg(3)) else {
            return wrongargs("hincrby");
        };
        let Some(delta) = MemStore::as_int(by) else {
            return Reply::error("ERR value is not an integer or out of range");
        };
        let h = match self.data.entry(k.to_vec()).or_insert_with(|| Value::Hash(BTreeMap::new())) {
            Value::Hash(h) => h,
            _ => return wrongtype(),
        };
        let cur = match h.get(f) {
            Some(v) => match MemStore::as_int(v) {
                Some(n) => n,
                None => return Reply::error("ERR hash value is not an integer"),
            },
            None => 0,
        };
        let next = match cur.checked_add(delta) {
            Some(n) => n,
            None => return Reply::error("ERR increment or decrement would overflow"),
        };
        h.insert(f.to_vec(), next.to_string().into_bytes());
        Reply::Integer(next)
    }
}

fn wrongargs(name: &str) -> Reply {
    Reply::error(format!("ERR wrong number of arguments for '{name}' command"))
}

fn wrongtype() -> Reply {
    Reply::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(parts: &[&str]) -> Command {
        Command::parse(parts.iter().map(|s| s.as_bytes().to_vec()).collect()).unwrap()
    }

    #[test]
    fn set_get_del() {
        let mut s = MemStore::new();
        assert_eq!(s.apply(&cmd(&["SET", "a", "1"])), Reply::ok());
        assert_eq!(s.query(&cmd(&["GET", "a"])), Reply::Bulk(b"1".to_vec()));
        assert_eq!(s.query(&cmd(&["EXISTS", "a"])), Reply::Integer(1));
        assert_eq!(s.apply(&cmd(&["DEL", "a"])), Reply::Integer(1));
        assert_eq!(s.query(&cmd(&["GET", "a"])), Reply::Nil);
    }

    #[test]
    fn incr_chain() {
        let mut s = MemStore::new();
        assert_eq!(s.apply(&cmd(&["INCR", "c"])), Reply::Integer(1));
        assert_eq!(s.apply(&cmd(&["INCRBY", "c", "10"])), Reply::Integer(11));
        assert_eq!(s.apply(&cmd(&["DECR", "c"])), Reply::Integer(10));
        assert_eq!(s.apply(&cmd(&["DECRBY", "c", "4"])), Reply::Integer(6));
        assert_eq!(s.query(&cmd(&["GET", "c"])), Reply::Bulk(b"6".to_vec()));
    }

    #[test]
    fn incr_on_nonint_errors() {
        let mut s = MemStore::new();
        s.apply(&cmd(&["SET", "a", "x"]));
        assert!(matches!(s.apply(&cmd(&["INCR", "a"])), Reply::Error(_)));
    }

    #[test]
    fn hash_ops() {
        let mut s = MemStore::new();
        assert_eq!(s.apply(&cmd(&["HSET", "h", "f1", "v1", "f2", "v2"])), Reply::Integer(2));
        assert_eq!(s.query(&cmd(&["HGET", "h", "f1"])), Reply::Bulk(b"v1".to_vec()));
        assert_eq!(s.query(&cmd(&["HLEN", "h"])), Reply::Integer(2));
        assert_eq!(s.apply(&cmd(&["HINCRBY", "h", "n", "5"])), Reply::Integer(5));
        assert_eq!(s.apply(&cmd(&["HDEL", "h", "f1"])), Reply::Integer(1));
        // HGETALL is deterministically ordered (BTreeMap): f2, n.
        assert_eq!(
            s.query(&cmd(&["HGETALL", "h"])),
            Reply::Array(vec![
                Reply::Bulk(b"f2".to_vec()),
                Reply::Bulk(b"v2".to_vec()),
                Reply::Bulk(b"n".to_vec()),
                Reply::Bulk(b"5".to_vec()),
            ])
        );
    }

    #[test]
    fn wrongtype_guard() {
        let mut s = MemStore::new();
        s.apply(&cmd(&["SET", "a", "1"]));
        assert!(matches!(s.query(&cmd(&["HGET", "a", "f"])), Reply::Error(_)));
    }

    #[test]
    fn expire_records_ttl_but_does_not_evict() {
        let mut s = MemStore::new();
        s.apply(&cmd(&["SET", "a", "1"]));
        assert_eq!(s.apply(&cmd(&["EXPIRE", "a", "100"])), Reply::Integer(1));
        assert_eq!(s.query(&cmd(&["TTL", "a"])), Reply::Integer(100));
        // Key still present — no wall-clock eviction (determinism).
        assert_eq!(s.query(&cmd(&["GET", "a"])), Reply::Bulk(b"1".to_vec()));
        // SET clears TTL.
        s.apply(&cmd(&["SET", "a", "2"]));
        assert_eq!(s.query(&cmd(&["TTL", "a"])), Reply::Integer(-1));
    }

    #[test]
    fn set_ops_and_deterministic_reads() {
        let mut s = MemStore::new();
        assert_eq!(s.apply(&cmd(&["SADD", "s", "a", "b", "c"])), Reply::Integer(3));
        assert_eq!(s.apply(&cmd(&["SADD", "s", "a"])), Reply::Integer(0)); // dup
        assert_eq!(s.query(&cmd(&["SCARD", "s"])), Reply::Integer(3));
        assert_eq!(s.query(&cmd(&["SISMEMBER", "s", "b"])), Reply::Integer(1));
        assert_eq!(s.query(&cmd(&["SISMEMBER", "s", "z"])), Reply::Integer(0));
        assert_eq!(s.query(&cmd(&["TYPE", "s"])), Reply::Simple("set".into()));
        // SMEMBERS is deterministically ordered (BTreeSet).
        assert_eq!(
            s.query(&cmd(&["SMEMBERS", "s"])),
            Reply::Array(vec![
                Reply::Bulk(b"a".to_vec()),
                Reply::Bulk(b"b".to_vec()),
                Reply::Bulk(b"c".to_vec()),
            ])
        );
        // SRANDMEMBER picks deterministically (first in order) — never mutates.
        assert_eq!(s.query(&cmd(&["SRANDMEMBER", "s"])), Reply::Bulk(b"a".to_vec()));
        assert_eq!(s.query(&cmd(&["SCARD", "s"])), Reply::Integer(3), "SRANDMEMBER did not mutate");
        // SREM (the SPOP effect target) removes; empties → key dropped.
        assert_eq!(s.apply(&cmd(&["SREM", "s", "a", "b"])), Reply::Integer(2));
        assert_eq!(s.apply(&cmd(&["SREM", "s", "c"])), Reply::Integer(1));
        assert_eq!(s.query(&cmd(&["EXISTS", "s"])), Reply::Integer(0));
    }

    #[test]
    fn snapshot_roundtrips_full_state() {
        // RD-3: export/import transfers the whole keyspace (strings, hashes,
        // sets) and TTLs.
        let mut a = MemStore::new();
        a.apply(&cmd(&["SET", "s", "1"]));
        a.apply(&cmd(&["HSET", "h", "f", "v"]));
        a.apply(&cmd(&["SADD", "myset", "x", "y"]));
        a.apply(&cmd(&["EXPIRE", "s", "50"]));

        let bytes = a.export_snapshot();
        let mut b = MemStore::new();
        b.import_snapshot(&bytes).unwrap();

        assert_eq!(a.snapshot_sorted(), b.snapshot_sorted());
        assert_eq!(b.query(&cmd(&["TTL", "s"])), Reply::Integer(50), "TTL transferred too");
        assert_eq!(b.query(&cmd(&["SMEMBERS", "myset"])), a.query(&cmd(&["SMEMBERS", "myset"])));
    }

    #[test]
    fn scan_is_deterministic_single_shot() {
        let mut s = MemStore::new();
        s.apply(&cmd(&["SET", "k1", "1"]));
        s.apply(&cmd(&["SET", "k2", "2"]));
        match s.query(&cmd(&["SCAN", "0"])) {
            Reply::Array(items) => {
                assert_eq!(items[0], Reply::Bulk(b"0".to_vec()), "cursor returns to 0");
                assert_eq!(
                    items[1],
                    Reply::Array(vec![Reply::Bulk(b"k1".to_vec()), Reply::Bulk(b"k2".to_vec())])
                );
            }
            other => panic!("expected scan array, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_equality_reflects_state() {
        let mut a = MemStore::new();
        let mut b = MemStore::new();
        // Apply the same ops in the same order → identical snapshots.
        let ops: [&[&str]; 3] = [&["SET", "k", "1"], &["INCR", "k"], &["HSET", "h", "f", "v"]];
        for c in ops {
            a.apply(&cmd(c));
            b.apply(&cmd(c));
        }
        assert_eq!(a.snapshot_sorted(), b.snapshot_sorted());
        // Diverge b → snapshots differ.
        b.apply(&cmd(&["SET", "k", "999"]));
        assert_ne!(a.snapshot_sorted(), b.snapshot_sorted());
    }
}
