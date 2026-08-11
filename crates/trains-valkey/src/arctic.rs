//! Feature-gated Arctic materialized-state adapter for the local RESP proxy.
//!
//! `GET` and valid single-key `EXISTS` commands may use [`SharedArcticReader`].
//! Mutations, `DBSIZE`, multi-key `EXISTS`, snapshots, and readmission remain on
//! [`OrderedArcticStore`], whose mutation methods require exclusive ownership.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use arctic::key::{BoxedSlice, NonNull, Slice};
use arctic::ConcurrentMap;

use crate::command::Command;
use crate::resp::Reply;
use crate::store::{RedisStore, SnapshotError};

type ArcticKey = BoxedSlice<NonNull>;
type ArcticValue = Box<Vec<u8>>;

/// Concurrent string-only materialized state.
///
/// Keys containing NUL are rejected because Arctic reserves that byte in its
/// key representation. Full binary-key compatibility remains outside scope.
#[derive(Default)]
pub struct ConcurrentArcticStore {
    data: ConcurrentMap<ArcticKey, ArcticValue>,
    len: AtomicUsize,
}

impl ConcurrentArcticStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(bytes: &[u8]) -> Result<&Slice<NonNull>, Reply> {
        Slice::new(bytes)
            .map_err(|error| Reply::error(format!("ERR Arctic key invariant: {error}")))
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Reply {
        let key = match Self::key(key) {
            Ok(key) => key,
            Err(reply) => return reply,
        };
        let inserted = self
            .data
            .upsert(key, Box::new(value.to_vec()))
            .old()
            .is_none();
        if inserted {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        Reply::ok()
    }

    pub fn get(&self, key: &[u8]) -> Reply {
        let key = match Self::key(key) {
            Ok(key) => key,
            Err(reply) => return reply,
        };
        match self.data.get(key) {
            Some(value) => Reply::Bulk(value.clone()),
            None => Reply::Nil,
        }
    }

    fn exists(&self, keys: &[&[u8]]) -> Reply {
        let mut present = 0i64;
        for key in keys {
            let key = match Self::key(key) {
                Ok(key) => key,
                Err(reply) => return reply,
            };
            if self.data.get(key).is_some() {
                present += 1;
            }
        }
        Reply::Integer(present)
    }

    pub fn remove(&self, key: &[u8]) -> Result<bool, Reply> {
        let key = Self::key(key)?;
        let removed = self.data.remove(key).is_some();
        if removed {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(removed)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stable ordered view for barriers, snapshots, and test assertions.
    pub fn snapshot_sorted(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .all()
            .entries(arctic::Order::Ascend)
            .map(|(key, value)| (key.into_boxed_slice().into_vec(), value))
            .collect()
    }
}

struct StoreGeneration {
    epoch: u64,
    data: Arc<ConcurrentArcticStore>,
}

impl Default for StoreGeneration {
    fn default() -> Self {
        Self {
            epoch: 0,
            data: Arc::new(ConcurrentArcticStore::new()),
        }
    }
}

/// Cloneable point-read handle that never grants mutation access.
#[derive(Clone)]
pub struct SharedArcticReader {
    generation: Arc<ArcSwap<StoreGeneration>>,
}

impl SharedArcticReader {
    /// Handle only valid `GET` and single-key `EXISTS` commands.
    ///
    /// Returning `None` preserves the normal ordered query path, including its
    /// argument validation and error reply.
    pub fn try_read(&self, cmd: &Command) -> Option<Reply> {
        match cmd.name.as_str() {
            "GET" if cmd.argv.len() == 2 => {
                let key = cmd.arg(1)?;
                let generation = self.generation.load();
                Some(generation.data.get(key))
            }
            "EXISTS" if cmd.argv.len() == 2 => {
                let key = cmd.arg(1)?;
                let generation = self.generation.load();
                Some(generation.data.exists(&[key]))
            }
            _ => None,
        }
    }
}

/// Single-owner mutation and snapshot side of the Arctic data plane.
///
/// This type is deliberately not `Clone`. TRAINS delivery remains the sole
/// mutation owner, preserving C1-C7 while RESP tasks clone only the reader.
pub struct OrderedArcticStore {
    generation: Arc<ArcSwap<StoreGeneration>>,
}

impl Default for OrderedArcticStore {
    fn default() -> Self {
        Self {
            generation: Arc::new(ArcSwap::from_pointee(StoreGeneration::default())),
        }
    }
}

impl OrderedArcticStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reader(&self) -> SharedArcticReader {
        SharedArcticReader {
            generation: Arc::clone(&self.generation),
        }
    }

    pub fn snapshot_sorted(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.generation.load().data.snapshot_sorted()
    }
}

fn wrongargs(name: &str) -> Reply {
    Reply::error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

impl RedisStore for OrderedArcticStore {
    fn apply(&mut self, cmd: &Command) -> Reply {
        let current = self.generation.load();
        match cmd.name.as_str() {
            "SET" => {
                if cmd.argv.len() != 3 {
                    return wrongargs("set");
                }
                let (Some(key), Some(value)) = (cmd.arg(1), cmd.arg(2)) else {
                    return wrongargs("set");
                };
                current.data.set(key, value)
            }
            "DEL" => {
                if cmd.argv.len() < 2 {
                    return wrongargs("del");
                }
                let keys: Result<Vec<_>, _> = (1..cmd.argv.len())
                    .filter_map(|index| cmd.arg(index))
                    .map(|key| ConcurrentArcticStore::key(key).map(|_| key))
                    .collect();
                let keys = match keys {
                    Ok(keys) => keys,
                    Err(reply) => return reply,
                };
                let mut removed = 0i64;
                for key in keys {
                    match current.data.remove(key) {
                        Ok(true) => removed += 1,
                        Ok(false) => {}
                        Err(reply) => return reply,
                    }
                }
                Reply::Integer(removed)
            }
            other => Reply::error(format!(
                "ERR Arctic proxy adapter does not support '{other}'"
            )),
        }
    }

    fn query(&self, cmd: &Command) -> Reply {
        match cmd.name.as_str() {
            "GET" => {
                if cmd.argv.len() != 2 {
                    return wrongargs("get");
                }
                self.reader()
                    .try_read(cmd)
                    .expect("validated GET is handled by the shared reader")
            }
            "EXISTS" => {
                if cmd.argv.len() < 2 {
                    return wrongargs("exists");
                }
                if cmd.argv.len() == 2 {
                    return self
                        .reader()
                        .try_read(cmd)
                        .expect("validated single-key EXISTS is handled by the reader");
                }
                let keys: Vec<_> = (1..cmd.argv.len())
                    .filter_map(|index| cmd.arg(index))
                    .collect();
                self.generation.load().data.exists(&keys)
            }
            "DBSIZE" => {
                if cmd.argv.len() != 1 {
                    return wrongargs("dbsize");
                }
                Reply::Integer(self.generation.load().data.len() as i64)
            }
            other => Reply::error(format!(
                "ERR Arctic proxy adapter does not support '{other}' as a read"
            )),
        }
    }

    fn export_snapshot(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self.snapshot_sorted(), bincode::config::standard())
            .expect("Arctic snapshot is serializable")
    }

    fn import_snapshot(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let (entries, _) = bincode::serde::decode_from_slice::<Vec<(Vec<u8>, Vec<u8>)>, _>(
            bytes,
            bincode::config::standard(),
        )?;
        let replacement = ConcurrentArcticStore::new();
        for (key, value) in entries {
            Slice::<NonNull>::new(&key).map_err(|error| {
                SnapshotError::Invariant(format!("invalid Arctic key: {error}"))
            })?;
            debug_assert_eq!(replacement.set(&key, &value), Reply::ok());
        }
        let epoch = self.generation.load().epoch.wrapping_add(1);
        self.generation.store(Arc::new(StoreGeneration {
            epoch,
            data: Arc::new(replacement),
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(parts: &[&str]) -> Command {
        Command::parse(parts.iter().map(|part| part.as_bytes().to_vec()).collect()).unwrap()
    }

    #[test]
    fn set_get_del_through_ordered_store() {
        let mut store = OrderedArcticStore::new();
        assert_eq!(store.apply(&cmd(&["SET", "key1", "val1"])), Reply::ok());
        assert_eq!(
            store.query(&cmd(&["GET", "key1"])),
            Reply::Bulk(b"val1".to_vec())
        );
        assert_eq!(store.query(&cmd(&["EXISTS", "key1"])), Reply::Integer(1));
        assert_eq!(store.apply(&cmd(&["DEL", "key1"])), Reply::Integer(1));
        assert_eq!(store.query(&cmd(&["GET", "key1"])), Reply::Nil);
    }

    #[test]
    fn shared_reader_handles_only_valid_point_reads() {
        let mut store = OrderedArcticStore::new();
        store.apply(&cmd(&["SET", "a", "1"]));
        let reader = store.reader();
        assert_eq!(
            reader.try_read(&cmd(&["GET", "a"])),
            Some(Reply::Bulk(b"1".to_vec()))
        );
        assert_eq!(
            reader.try_read(&cmd(&["EXISTS", "a"])),
            Some(Reply::Integer(1))
        );
        assert_eq!(reader.try_read(&cmd(&["EXISTS", "a", "b"])), None);
        assert_eq!(reader.try_read(&cmd(&["DBSIZE"])), None);
    }

    #[test]
    fn malformed_point_reads_keep_explicit_errors() {
        let mut store = OrderedArcticStore::new();
        assert!(matches!(store.query(&cmd(&["GET"])), Reply::Error(_)));
        assert!(matches!(store.query(&cmd(&["EXISTS"])), Reply::Error(_)));
        assert!(matches!(
            store.apply(&cmd(&["SET", "key", "value", "NX"])),
            Reply::Error(_)
        ));

        assert_eq!(store.apply(&cmd(&["SET", "kept", "value"])), Reply::ok());
        let invalid_key = Command::parse(vec![
            b"DEL".to_vec(),
            b"kept".to_vec(),
            b"bad\0key".to_vec(),
        ])
        .unwrap();
        assert!(matches!(store.apply(&invalid_key), Reply::Error(_)));
        assert_eq!(
            store.query(&cmd(&["GET", "kept"])),
            Reply::Bulk(b"value".to_vec())
        );
    }

    #[test]
    fn dbsize_and_multi_key_exists_stay_ordered() {
        let mut store = OrderedArcticStore::new();
        store.apply(&cmd(&["SET", "a", "1"]));
        store.apply(&cmd(&["SET", "b", "2"]));
        assert_eq!(store.query(&cmd(&["DBSIZE"])), Reply::Integer(2));
        assert_eq!(
            store.query(&cmd(&["EXISTS", "a", "b", "c"])),
            Reply::Integer(2)
        );
    }

    #[test]
    fn snapshot_import_replaces_generation() {
        let mut store = OrderedArcticStore::new();
        store.apply(&cmd(&["SET", "old", "data"]));
        let reader = store.reader();

        let mut source = OrderedArcticStore::new();
        source.apply(&cmd(&["SET", "new", "fresh"]));
        store.import_snapshot(&source.export_snapshot()).unwrap();

        assert_eq!(reader.try_read(&cmd(&["GET", "old"])), Some(Reply::Nil));
        assert_eq!(
            reader.try_read(&cmd(&["GET", "new"])),
            Some(Reply::Bulk(b"fresh".to_vec()))
        );
    }

    #[test]
    fn snapshot_export_import_roundtrip() {
        let mut store = OrderedArcticStore::new();
        store.apply(&cmd(&["SET", "x", "10"]));
        store.apply(&cmd(&["SET", "y", "20"]));
        let mut restored = OrderedArcticStore::new();
        restored.import_snapshot(&store.export_snapshot()).unwrap();
        assert_eq!(store.snapshot_sorted(), restored.snapshot_sorted());
    }

    #[test]
    fn invalid_snapshot_key_returns_error() {
        let bytes = bincode::serde::encode_to_vec(
            vec![(b"bad\0key".to_vec(), b"value".to_vec())],
            bincode::config::standard(),
        )
        .unwrap();
        let mut store = OrderedArcticStore::new();
        assert!(matches!(
            store.import_snapshot(&bytes),
            Err(SnapshotError::Invariant(_))
        ));
    }
}
