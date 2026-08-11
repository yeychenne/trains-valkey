#[cfg(not(feature = "rwlock-generation"))]
use arc_swap::ArcSwap;
use arctic::key::{BoxedSlice, NonNull, Slice};
use arctic::{ConcurrentMap, Order};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[cfg(feature = "rwlock-generation")]
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use trains_valkey::{Command, RedisStore, Reply, SnapshotError};

type ArcticKey = BoxedSlice<NonNull>;
type ArcticValue = Box<Vec<u8>>;

/// Experimental string-only Valkey data plane backed by Arctic.
///
/// Keys must not contain NUL bytes. This is deliberate for the first shadow
/// experiment; full binary-safe Valkey compatibility needs a reversible key
/// encoding that preserves Arctic's prefix invariant.
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
        Slice::new(bytes).map_err(|e| Reply::error(format!("ERR Arctic key invariant: {e}")))
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

    /// Ordered state view. Call only after writers have crossed a TRAINS barrier.
    pub fn snapshot_sorted(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .all()
            .entries(Order::Ascend)
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

#[cfg(not(feature = "rwlock-generation"))]
struct GenerationCell(ArcSwap<StoreGeneration>);

#[cfg(feature = "rwlock-generation")]
struct GenerationCell(RwLock<StoreGeneration>);

impl GenerationCell {
    fn new() -> Self {
        #[cfg(not(feature = "rwlock-generation"))]
        {
            Self(ArcSwap::from_pointee(StoreGeneration::default()))
        }
        #[cfg(feature = "rwlock-generation")]
        {
            Self(RwLock::new(StoreGeneration::default()))
        }
    }

    fn with<R>(&self, inspect: impl FnOnce(&StoreGeneration) -> R) -> R {
        #[cfg(not(feature = "rwlock-generation"))]
        {
            let generation = self.0.load();
            inspect(&generation)
        }
        #[cfg(feature = "rwlock-generation")]
        {
            let generation = self.0.read().expect("Arctic generation lock poisoned");
            let snapshot = StoreGeneration {
                epoch: generation.epoch,
                data: Arc::clone(&generation.data),
            };
            drop(generation);
            inspect(&snapshot)
        }
    }

    fn load_owned(&self) -> Arc<StoreGeneration> {
        #[cfg(not(feature = "rwlock-generation"))]
        {
            self.0.load_full()
        }
        #[cfg(feature = "rwlock-generation")]
        {
            let generation = self.0.read().expect("Arctic generation lock poisoned");
            Arc::new(StoreGeneration {
                epoch: generation.epoch,
                data: Arc::clone(&generation.data),
            })
        }
    }

    fn replace(&self, generation: StoreGeneration) {
        #[cfg(not(feature = "rwlock-generation"))]
        {
            self.0.store(Arc::new(generation));
        }
        #[cfg(feature = "rwlock-generation")]
        {
            *self.0.write().expect("Arctic generation lock poisoned") = generation;
        }
    }
}

pub const fn generation_strategy() -> &'static str {
    if cfg!(feature = "rwlock-generation") {
        "rwlock"
    } else {
        "arc-swap"
    }
}

/// A pinned local Arctic generation.
///
/// Snapshot replacement never mutates a generation in place. An in-flight
/// reader may finish against the old generation; a subsequently pinned view
/// sees the fully built replacement.
#[derive(Clone)]
pub struct ArcticReadView {
    generation: Arc<StoreGeneration>,
}

impl ArcticReadView {
    pub fn epoch(&self) -> u64 {
        self.generation.epoch
    }

    pub fn get(&self, key: &[u8]) -> Reply {
        self.generation.data.get(key)
    }

    pub fn exists(&self, keys: &[&[u8]]) -> Reply {
        self.generation.data.exists(keys)
    }

    /// Diagnostic ordered view. It is stable across generation replacement;
    /// callers must still exclude point mutations when they need a snapshot cut.
    pub fn snapshot_sorted(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.generation.data.snapshot_sorted()
    }
}

/// Cloneable point-read handle for the concurrent data plane.
#[derive(Clone)]
pub struct SharedArcticReader {
    generation: Arc<GenerationCell>,
}

impl SharedArcticReader {
    pub fn pin(&self) -> ArcticReadView {
        ArcticReadView {
            generation: self.generation.load_owned(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Reply {
        self.generation.with(|generation| generation.data.get(key))
    }

    pub fn exists(&self, key: &[u8]) -> Reply {
        self.generation
            .with(|generation| generation.data.exists(&[key]))
    }

    /// Concurrent point reads only. `DBSIZE` remains on the ordered side
    /// because a separate cardinality counter has a mutation visibility window.
    pub fn query(&self, cmd: &Command) -> Reply {
        match cmd.name.as_str() {
            "GET" => match cmd.arg(1) {
                Some(key) => self.get(key),
                None => Reply::error("ERR wrong number of arguments for 'get' command"),
            },
            "EXISTS" => {
                if cmd.argv.len() != 2 {
                    if cmd.argv.len() < 2 {
                        return Reply::error("ERR wrong number of arguments for 'exists' command");
                    }
                    return Reply::error("ERR multi-key EXISTS requires the ordered Arctic writer");
                }
                let Some(key) = cmd.arg(1) else {
                    return Reply::error("ERR wrong number of arguments for 'exists' command");
                };
                self.exists(key)
            }
            "DBSIZE" => Reply::error("ERR DBSIZE requires the ordered Arctic writer"),
            other => Reply::error(format!(
                "ERR Arctic shared reader does not support '{other}'"
            )),
        }
    }
}

/// Single-owner mutation and snapshot side of the Arctic data plane.
///
/// The value is deliberately not `Clone`, and every mutation requires
/// `&mut self`. TRAINS' delivery driver can therefore remain the sole writer
/// while RESP tasks clone [`SharedArcticReader`] for local point reads.
pub struct OrderedArcticStore {
    generation: Arc<GenerationCell>,
}

impl Default for OrderedArcticStore {
    fn default() -> Self {
        Self {
            generation: Arc::new(GenerationCell::new()),
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
        self.generation
            .with(|generation| generation.data.snapshot_sorted())
    }
}

/// Compatibility name used by the original seven-gate shadow suite.
pub type ArcticStore = OrderedArcticStore;

impl RedisStore for OrderedArcticStore {
    fn apply(&mut self, cmd: &Command) -> Reply {
        self.generation.with(|current| match cmd.name.as_str() {
            "SET" => {
                let (Some(key), Some(value)) = (cmd.arg(1), cmd.arg(2)) else {
                    return Reply::error("ERR wrong number of arguments for 'set' command");
                };
                current.data.set(key, value)
            }
            "DEL" => {
                if cmd.argv.len() < 2 {
                    return Reply::error("ERR wrong number of arguments for 'del' command");
                }
                let mut removed = 0i64;
                for index in 1..cmd.argv.len() {
                    let Some(key) = cmd.arg(index) else { continue };
                    match current.data.remove(key) {
                        Ok(true) => removed += 1,
                        Ok(false) => {}
                        Err(reply) => return reply,
                    }
                }
                Reply::Integer(removed)
            }
            other => Reply::error(format!("ERR Arctic shadow does not support '{other}'")),
        })
    }

    fn query(&self, cmd: &Command) -> Reply {
        if cmd.name == "DBSIZE" {
            return self
                .generation
                .with(|generation| Reply::Integer(generation.data.len() as i64));
        }
        if cmd.name == "EXISTS" && cmd.argv.len() > 2 {
            let keys: Vec<_> = (1..cmd.argv.len())
                .filter_map(|index| cmd.arg(index))
                .collect();
            return self
                .generation
                .with(|generation| generation.data.exists(&keys));
        }
        self.reader().query(cmd)
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
            let reply = replacement.set(&key, &value);
            assert_eq!(
                reply,
                Reply::ok(),
                "snapshot contained an invalid Arctic key"
            );
        }
        let epoch = self
            .generation
            .with(|generation| generation.epoch)
            .wrapping_add(1);
        self.generation.replace(StoreGeneration {
            epoch,
            data: Arc::new(replacement),
        });
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct MirrorSnapshot {
    primary: Vec<u8>,
    shadow: Vec<u8>,
}

/// Test-only dual store: Valkey is authoritative and Arctic is checked inline.
pub struct MirrorStore<P> {
    primary: P,
    shadow: ArcticStore,
}

impl<P> MirrorStore<P> {
    pub fn new(primary: P) -> Self {
        Self {
            primary,
            shadow: ArcticStore::new(),
        }
    }

    pub fn primary(&self) -> &P {
        &self.primary
    }

    pub fn shadow(&self) -> &ArcticStore {
        &self.shadow
    }
}

impl<P: RedisStore> RedisStore for MirrorStore<P> {
    fn apply(&mut self, cmd: &Command) -> Reply {
        let primary = self.primary.apply(cmd);
        let shadow = self.shadow.apply(cmd);
        assert_eq!(primary, shadow, "Valkey/Arctic apply mismatch for {cmd:?}");
        primary
    }

    fn query(&self, cmd: &Command) -> Reply {
        let primary = self.primary.query(cmd);
        let shadow = self.shadow.query(cmd);
        assert_eq!(primary, shadow, "Valkey/Arctic query mismatch for {cmd:?}");
        primary
    }

    fn export_snapshot(&self) -> Vec<u8> {
        let snapshot = MirrorSnapshot {
            primary: self.primary.export_snapshot(),
            shadow: self.shadow.export_snapshot(),
        };
        bincode::serde::encode_to_vec(snapshot, bincode::config::standard())
            .expect("mirror snapshot is serializable")
    }

    fn import_snapshot(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let (snapshot, _) = bincode::serde::decode_from_slice::<MirrorSnapshot, _>(
            bytes,
            bincode::config::standard(),
        )?;
        self.primary.import_snapshot(&snapshot.primary)?;
        self.shadow.import_snapshot(&snapshot.shadow)?;
        Ok(())
    }
}
