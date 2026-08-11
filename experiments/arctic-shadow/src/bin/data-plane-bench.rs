use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use trains_valkey::{Command, RedisBackend, RedisStore, Reply};
use trains_valkey_arctic_shadow::{
    ConcurrentArcticStore, OrderedArcticStore, SharedArcticReader, generation_strategy,
};

const VALUE: &[u8] = b"0123456789abcdef0123456789abcdef";
const SAMPLE_EVERY: usize = 16;

#[derive(Clone, Copy)]
enum Workload {
    ReadOnly,
    Read90Write10,
    WriteOnly,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Read90Write10 => "read-90-write-10",
            Self::WriteOnly => "write-only",
        }
    }

    fn is_write(self, operation: usize) -> bool {
        match self {
            Self::ReadOnly => false,
            Self::Read90Write10 => operation.is_multiple_of(10),
            Self::WriteOnly => true,
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
            "read-only" => Self::ReadOnly,
            "read-90-write-10" => Self::Read90Write10,
            "write-only" => Self::WriteOnly,
            _ => panic!("unknown benchmark workload '{name}'"),
        }
    }
}

#[derive(Serialize)]
struct Config {
    keys: usize,
    operations_per_thread: usize,
    thread_counts: Vec<usize>,
    backends: Vec<String>,
    workloads: Vec<String>,
    repetitions: usize,
    latency_sample_every: usize,
    value_bytes: usize,
}

#[derive(Serialize)]
struct CaseResult {
    backend: &'static str,
    boundary: &'static str,
    workload: &'static str,
    threads: usize,
    repetition: usize,
    total_operations: usize,
    elapsed_ms: f64,
    operations_per_second: f64,
    latency_samples: usize,
    latency_p50_ns: u64,
    latency_p99_ns: u64,
    process_cpu_ms: f64,
    engine_cpu_ms: f64,
}

#[derive(Serialize)]
struct Report {
    generated_unix_seconds: u64,
    label: String,
    generation_strategy: &'static str,
    architecture: &'static str,
    available_parallelism: usize,
    process_max_rss_kb: Option<u64>,
    config: Config,
    results: Vec<CaseResult>,
}

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

#[cfg(target_os = "linux")]
fn process_cpu_ms(pid: u32) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    // SAFETY: sysconf reads a process-global constant and does not retain pointers.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    (ticks_per_second > 0)
        .then_some((user_ticks + system_ticks) as f64 * 1_000.0 / ticks_per_second as f64)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ms(_pid: u32) -> Option<f64> {
    None
}

fn own_process_cpu_ms() -> f64 {
    process_cpu_ms(std::process::id()).unwrap_or(0.0)
}

#[cfg(target_os = "linux")]
fn process_max_rss_kb() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(not(target_os = "linux"))]
fn process_max_rss_kb() -> Option<u64> {
    None
}

trait PointWorker {
    fn get(&mut self, key: &[u8]) -> bool;
    fn set(&mut self, key: &[u8], value: &[u8]);
}

struct ArcticWorker {
    store: Arc<ConcurrentArcticStore>,
}

impl PointWorker for ArcticWorker {
    fn get(&mut self, key: &[u8]) -> bool {
        matches!(self.store.get(key), Reply::Bulk(_))
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        assert_eq!(self.store.set(key, value), Reply::ok());
    }
}

struct OrderedWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    reply: mpsc::SyncSender<Reply>,
}

struct OrderedArcticWorker {
    reader: SharedArcticReader,
    writer: mpsc::SyncSender<OrderedWrite>,
}

impl PointWorker for OrderedArcticWorker {
    fn get(&mut self, key: &[u8]) -> bool {
        matches!(self.reader.get(key), Reply::Bulk(_))
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        self.writer
            .send(OrderedWrite {
                key: key.to_vec(),
                value: value.to_vec(),
                reply: reply_tx,
            })
            .expect("ordered Arctic writer stopped");
        assert_eq!(
            reply_rx.recv().expect("ordered Arctic write reply dropped"),
            Reply::ok()
        );
    }
}

struct MutexBTreeWorker {
    store: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl PointWorker for MutexBTreeWorker {
    fn get(&mut self, key: &[u8]) -> bool {
        self.store
            .lock()
            .expect("BTreeMap mutex poisoned")
            .get(key)
            .is_some()
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.store
            .lock()
            .expect("BTreeMap mutex poisoned")
            .insert(key.to_vec(), value.to_vec());
    }
}

struct OrderedMutexBTreeWorker {
    store: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    writer: mpsc::SyncSender<OrderedWrite>,
}

impl PointWorker for OrderedMutexBTreeWorker {
    fn get(&mut self, key: &[u8]) -> bool {
        self.store
            .lock()
            .expect("BTreeMap mutex poisoned")
            .get(key)
            .is_some()
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        self.writer
            .send(OrderedWrite {
                key: key.to_vec(),
                value: value.to_vec(),
                reply: reply_tx,
            })
            .expect("ordered BTreeMap writer stopped");
        assert_eq!(
            reply_rx
                .recv()
                .expect("ordered BTreeMap write reply dropped"),
            Reply::ok()
        );
    }
}

struct ValkeyWorker {
    backend: RedisBackend,
}

impl PointWorker for ValkeyWorker {
    fn get(&mut self, key: &[u8]) -> bool {
        matches!(
            self.backend.query(&Command {
                name: "GET".into(),
                argv: vec![b"GET".to_vec(), key.to_vec()],
            }),
            Reply::Bulk(_)
        )
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        assert_eq!(
            self.backend.apply(&Command {
                name: "SET".into(),
                argv: vec![b"SET".to_vec(), key.to_vec(), value.to_vec()],
            }),
            Reply::ok()
        );
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted[index]
}

fn run_case<F, W>(
    backend: &'static str,
    boundary: &'static str,
    workload: Workload,
    threads: usize,
    operations_per_thread: usize,
    keys: Arc<Vec<Vec<u8>>>,
    factory: F,
) -> CaseResult
where
    F: Fn() -> W + Send + Sync + 'static,
    W: PointWorker + 'static,
{
    let factory = Arc::new(factory);
    let start_barrier = Arc::new(Barrier::new(threads + 1));
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut handles = Vec::with_capacity(threads);

    for worker_id in 0..threads {
        let factory = factory.clone();
        let start_barrier = start_barrier.clone();
        let ready_tx = ready_tx.clone();
        let keys = keys.clone();
        handles.push(thread::spawn(move || {
            let mut worker = factory();
            let mut rng = DeterministicRng::new(0x9e37_79b9_u64 ^ worker_id as u64);
            let mut latencies = Vec::with_capacity(operations_per_thread / SAMPLE_EVERY + 1);
            let mut read_hits = 0usize;
            let mut reads = 0usize;
            ready_tx.send(()).unwrap();
            start_barrier.wait();

            for operation in 0..operations_per_thread {
                let key = &keys[rng.index(keys.len())];
                let sampled = operation.is_multiple_of(SAMPLE_EVERY);
                let started = sampled.then(Instant::now);
                if workload.is_write(operation) {
                    worker.set(key, VALUE);
                } else {
                    reads += 1;
                    read_hits += usize::from(worker.get(key));
                }
                if let Some(started) = started {
                    latencies.push(started.elapsed().as_nanos() as u64);
                }
            }
            assert_eq!(read_hits, reads, "benchmark read missed a preloaded key");
            latencies
        }));
    }
    drop(ready_tx);
    for _ in 0..threads {
        ready_rx
            .recv()
            .expect("benchmark worker failed before start");
    }

    let cpu_started = own_process_cpu_ms();
    let started = Instant::now();
    start_barrier.wait();
    let mut latencies = Vec::new();
    for handle in handles {
        latencies.extend(handle.join().expect("benchmark worker panicked"));
    }
    let elapsed = started.elapsed();
    let process_cpu_ms = (own_process_cpu_ms() - cpu_started).max(0.0);
    latencies.sort_unstable();

    let total_operations = threads * operations_per_thread;
    CaseResult {
        backend,
        boundary,
        workload: workload.name(),
        threads,
        repetition: 0,
        total_operations,
        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
        operations_per_second: total_operations as f64 / elapsed.as_secs_f64(),
        latency_samples: latencies.len(),
        latency_p50_ns: percentile(&latencies, 50),
        latency_p99_ns: percentile(&latencies, 99),
        process_cpu_ms,
        engine_cpu_ms: 0.0,
    }
}

fn make_keys(count: usize) -> Arc<Vec<Vec<u8>>> {
    Arc::new(
        (0..count)
            .map(|index| format!("bench:{index:08}").into_bytes())
            .collect(),
    )
}

fn run_arctic(
    workload: Workload,
    threads: usize,
    operations_per_thread: usize,
    keys: Arc<Vec<Vec<u8>>>,
) -> CaseResult {
    let store = Arc::new(ConcurrentArcticStore::new());
    for key in keys.iter() {
        assert_eq!(store.set(key, VALUE), Reply::ok());
    }
    let expected_len = keys.len();
    let worker_store = store.clone();
    let result = run_case(
        "arctic",
        "in-process lock-free map",
        workload,
        threads,
        operations_per_thread,
        keys,
        move || ArcticWorker {
            store: worker_store.clone(),
        },
    );
    assert_eq!(store.len(), expected_len);
    result
}

fn run_ordered_arctic(
    workload: Workload,
    threads: usize,
    operations_per_thread: usize,
    keys: Arc<Vec<Vec<u8>>>,
) -> CaseResult {
    let mut store = OrderedArcticStore::new();
    for key in keys.iter() {
        assert_eq!(
            store.apply(&command(vec![b"SET".to_vec(), key.clone(), VALUE.to_vec()])),
            Reply::ok()
        );
    }
    let expected_len = keys.len();
    let reader = store.reader();
    let (writer_tx, writer_rx) = mpsc::sync_channel::<OrderedWrite>(1_024);
    let writer_handle = thread::spawn(move || {
        while let Ok(request) = writer_rx.recv() {
            let reply = store.apply(&command(vec![b"SET".to_vec(), request.key, request.value]));
            request
                .reply
                .send(reply)
                .expect("ordered Arctic benchmark worker stopped");
        }
        store
    });

    let worker_reader = reader.clone();
    let worker_writer = writer_tx.clone();
    let result = run_case(
        "ordered-arctic",
        "shared point reads + single in-process writer queue",
        workload,
        threads,
        operations_per_thread,
        keys,
        move || OrderedArcticWorker {
            reader: worker_reader.clone(),
            writer: worker_writer.clone(),
        },
    );
    drop(writer_tx);
    let store = writer_handle
        .join()
        .expect("ordered Arctic writer panicked");
    assert_eq!(
        store.query(&command(vec![b"DBSIZE".to_vec()])),
        Reply::Integer(expected_len as i64)
    );
    result
}

fn run_mutex_btree(
    workload: Workload,
    threads: usize,
    operations_per_thread: usize,
    keys: Arc<Vec<Vec<u8>>>,
) -> CaseResult {
    let expected_len = keys.len();
    let initial: BTreeMap<_, _> = keys
        .iter()
        .map(|key| (key.clone(), VALUE.to_vec()))
        .collect();
    let store = Arc::new(Mutex::new(initial));
    let worker_store = store.clone();
    let result = run_case(
        "mutex-btree",
        "in-process Mutex<BTreeMap>",
        workload,
        threads,
        operations_per_thread,
        keys,
        move || MutexBTreeWorker {
            store: worker_store.clone(),
        },
    );
    assert_eq!(
        store.lock().expect("BTreeMap mutex poisoned").len(),
        expected_len
    );
    result
}

fn run_ordered_mutex_btree(
    workload: Workload,
    threads: usize,
    operations_per_thread: usize,
    keys: Arc<Vec<Vec<u8>>>,
) -> CaseResult {
    let expected_len = keys.len();
    let initial: BTreeMap<_, _> = keys
        .iter()
        .map(|key| (key.clone(), VALUE.to_vec()))
        .collect();
    let store = Arc::new(Mutex::new(initial));
    let writer_store = Arc::clone(&store);
    let (writer_tx, writer_rx) = mpsc::sync_channel::<OrderedWrite>(1_024);
    let writer_handle = thread::spawn(move || {
        while let Ok(request) = writer_rx.recv() {
            writer_store
                .lock()
                .expect("BTreeMap mutex poisoned")
                .insert(request.key, request.value);
            request
                .reply
                .send(Reply::ok())
                .expect("ordered BTreeMap benchmark worker stopped");
        }
    });

    let worker_store = Arc::clone(&store);
    let worker_writer = writer_tx.clone();
    let result = run_case(
        "ordered-mutex-btree",
        "shared Mutex<BTreeMap> + single in-process writer queue",
        workload,
        threads,
        operations_per_thread,
        keys,
        move || OrderedMutexBTreeWorker {
            store: Arc::clone(&worker_store),
            writer: worker_writer.clone(),
        },
    );
    drop(writer_tx);
    writer_handle
        .join()
        .expect("ordered BTreeMap writer panicked");
    assert_eq!(
        store.lock().expect("BTreeMap mutex poisoned").len(),
        expected_len
    );
    result
}

fn command(argv: Vec<Vec<u8>>) -> Command {
    Command::parse(argv).expect("non-empty command")
}

fn preload_valkey(addr: SocketAddr, keys: &[Vec<u8>]) {
    let mut backend = RedisBackend::connect(addr).expect("connect Valkey for preload");
    for key in keys {
        assert_eq!(
            backend.apply(&command(vec![b"SET".to_vec(), key.clone(), VALUE.to_vec()])),
            Reply::ok()
        );
    }
}

fn run_valkey(
    engine: &Engine,
    workload: Workload,
    threads: usize,
    operations_per_thread: usize,
    keys: Arc<Vec<Vec<u8>>>,
) -> CaseResult {
    preload_valkey(engine.addr, &keys);
    let engine_cpu_started = engine.cpu_ms();
    let addr = engine.addr;
    let mut result = run_case(
        "valkey",
        "local RESP process boundary",
        workload,
        threads,
        operations_per_thread,
        keys,
        move || ValkeyWorker {
            backend: RedisBackend::connect(addr).expect("connect Valkey benchmark worker"),
        },
    );
    result.engine_cpu_ms = (engine.cpu_ms() - engine_cpu_started).max(0.0);
    result
}

fn engine_bin() -> &'static str {
    ["valkey-server", "redis-server"]
        .into_iter()
        .find(|bin| {
            ProcessCommand::new(bin)
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        })
        .expect("valkey-server or redis-server must be on PATH")
}

fn pick_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct Engine {
    child: Child,
    addr: SocketAddr,
}

impl Engine {
    fn spawn() -> Self {
        let addr = pick_addr();
        let port = addr.port().to_string();
        let child = ProcessCommand::new(engine_bin())
            .args([
                "--port",
                &port,
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn Valkey engine");
        let engine = Self { child, addr };
        for _ in 0..100 {
            if let Ok(backend) = RedisBackend::connect(addr)
                && backend.query(&command(vec![b"PING".to_vec()])) == Reply::Simple("PONG".into())
            {
                return engine;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("Valkey engine at {addr} never became ready");
    }

    fn cpu_ms(&self) -> f64 {
        process_cpu_ms(self.child.id()).unwrap_or(0.0)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn thread_counts() -> Vec<usize> {
    std::env::var("ARCTIC_BENCH_THREADS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .filter(|threads| *threads > 0)
                .collect::<Vec<_>>()
        })
        .filter(|counts| !counts.is_empty())
        .unwrap_or_else(|| vec![1, 2, 4, 8])
}

fn selected_names(env_name: &str, allowed: &[&str]) -> Vec<String> {
    let requested: BTreeSet<String> = std::env::var(env_name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_else(|| allowed.iter().map(|name| (*name).to_owned()).collect());
    for name in &requested {
        assert!(
            allowed.contains(&name.as_str()),
            "unknown {env_name} value '{name}'"
        );
    }
    let selected: Vec<_> = allowed
        .iter()
        .filter(|name| requested.contains(**name))
        .map(|name| (*name).to_owned())
        .collect();
    assert!(!selected.is_empty(), "{env_name} selected no cases");
    selected
}

fn main() {
    const BACKENDS: [&str; 5] = [
        "arctic",
        "ordered-arctic",
        "mutex-btree",
        "ordered-mutex-btree",
        "valkey",
    ];
    const WORKLOADS: [&str; 3] = ["read-only", "read-90-write-10", "write-only"];

    let keys_count = env_usize("ARCTIC_BENCH_KEYS", 10_000);
    let operations_per_thread = env_usize("ARCTIC_BENCH_OPS_PER_THREAD", 10_000);
    let thread_counts = thread_counts();
    let backends = selected_names("ARCTIC_BENCH_BACKENDS", &BACKENDS);
    let workload_names = selected_names("ARCTIC_BENCH_WORKLOADS", &WORKLOADS);
    let selected_backends: BTreeSet<_> = backends.iter().map(String::as_str).collect();
    let repetitions = env_usize("ARCTIC_BENCH_REPETITIONS", 3);
    let keys = make_keys(keys_count);
    let engine = selected_backends.contains("valkey").then(Engine::spawn);
    let workloads = workload_names
        .iter()
        .map(|name| Workload::from_name(name))
        .collect::<Vec<_>>();
    let mut results = Vec::new();

    println!(
        "backend,workload,threads,repetition,ops_per_sec,p50_ns,p99_ns,elapsed_ms,process_cpu_ms,engine_cpu_ms"
    );
    for workload in workloads {
        for &threads in &thread_counts {
            for repetition in 0..repetitions {
                let order = match repetition % 5 {
                    0 => [0, 1, 2, 3, 4],
                    1 => [1, 2, 3, 4, 0],
                    2 => [2, 3, 4, 0, 1],
                    3 => [3, 4, 0, 1, 2],
                    _ => [4, 0, 1, 2, 3],
                };
                let mut cases = Vec::with_capacity(order.len());
                for backend in order {
                    if !selected_backends.contains(BACKENDS[backend]) {
                        continue;
                    }
                    cases.push(match backend {
                        0 => run_arctic(workload, threads, operations_per_thread, keys.clone()),
                        1 => run_ordered_arctic(
                            workload,
                            threads,
                            operations_per_thread,
                            keys.clone(),
                        ),
                        2 => {
                            run_mutex_btree(workload, threads, operations_per_thread, keys.clone())
                        }
                        3 => run_ordered_mutex_btree(
                            workload,
                            threads,
                            operations_per_thread,
                            keys.clone(),
                        ),
                        _ => run_valkey(
                            engine.as_ref().expect("Valkey backend has an engine"),
                            workload,
                            threads,
                            operations_per_thread,
                            keys.clone(),
                        ),
                    });
                }
                for mut result in cases {
                    result.repetition = repetition;
                    println!(
                        "{},{},{},{},{:.0},{},{},{:.3},{:.3},{:.3}",
                        result.backend,
                        result.workload,
                        result.threads,
                        result.repetition,
                        result.operations_per_second,
                        result.latency_p50_ns,
                        result.latency_p99_ns,
                        result.elapsed_ms,
                        result.process_cpu_ms,
                        result.engine_cpu_ms,
                    );
                    results.push(result);
                }
            }
        }
    }

    let report = Report {
        generated_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        label: std::env::var("ARCTIC_BENCH_LABEL").unwrap_or_else(|_| "unlabelled".into()),
        generation_strategy: generation_strategy(),
        architecture: std::env::consts::ARCH,
        available_parallelism: thread::available_parallelism().map_or(1, usize::from),
        process_max_rss_kb: process_max_rss_kb(),
        config: Config {
            keys: keys_count,
            operations_per_thread,
            thread_counts,
            backends,
            workloads: workload_names,
            repetitions,
            latency_sample_every: SAMPLE_EVERY,
            value_bytes: VALUE.len(),
        },
        results,
    };

    if let Ok(path) = std::env::var("ARCTIC_BENCH_OUTPUT") {
        let json = serde_json::to_vec_pretty(&report).expect("serialize benchmark report");
        std::fs::write(&path, json).expect("write benchmark report");
        eprintln!("wrote {path}");
    }
}
