use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use trains_valkey::Reply;
use trains_valkey::resp::{encode_request, read_reply};

const SEED_VALUE: &[u8] = b"0123456789abcdef0123456789abcdef";
const SAMPLE_EVERY: usize = 16;
const DEFAULT_SEED: u64 = 0x5452_4149_4e53_2026;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Target {
    MutexProxy,
    ArcticProxy,
    Valkey,
}

impl Target {
    fn name(self) -> &'static str {
        match self {
            Self::MutexProxy => "mutex-proxy",
            Self::ArcticProxy => "arctic-proxy",
            Self::Valkey => "valkey",
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
            "mutex-proxy" => Self::MutexProxy,
            "arctic-proxy" => Self::ArcticProxy,
            "valkey" => Self::Valkey,
            _ => panic!("unknown benchmark target '{name}'"),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    ReadOnly,
    #[serde(rename = "read-90-write-10")]
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

    fn from_name(name: &str) -> Self {
        match name {
            "read-only" => Self::ReadOnly,
            "read-90-write-10" => Self::Read90Write10,
            "write-only" => Self::WriteOnly,
            _ => panic!("unknown benchmark workload '{name}'"),
        }
    }

    fn is_write(self, operation: usize) -> bool {
        match self {
            Self::ReadOnly => false,
            Self::Read90Write10 => operation.is_multiple_of(10),
            Self::WriteOnly => true,
        }
    }

    fn seed_tag(self) -> u64 {
        match self {
            Self::ReadOnly => 0x10,
            Self::Read90Write10 => 0x90,
            Self::WriteOnly => 0xff,
        }
    }
}

#[derive(Serialize)]
struct Config {
    keys: usize,
    operations_per_client: BTreeMap<String, usize>,
    client_counts: Vec<usize>,
    targets: Vec<String>,
    workloads: Vec<String>,
    repetitions: usize,
    latency_sample_every: usize,
    value_bytes: usize,
    seed: u64,
}

#[derive(Serialize)]
struct CaseResult {
    case_id: String,
    target: Target,
    workload: Workload,
    clients: usize,
    repetition: usize,
    total_operations: usize,
    elapsed_ms: f64,
    operations_per_second: f64,
    latency_samples: usize,
    latency_p50_us: f64,
    latency_p99_us: f64,
    target_cpu_ms: f64,
    target_peak_rss_kb: u64,
    client_cpu_ms: f64,
    validated_keys: usize,
}

#[derive(Clone, Serialize)]
struct TelemetrySample {
    case_id: String,
    target: Target,
    elapsed_ms: f64,
    process_cpu_ms: f64,
    rss_kb: u64,
}

#[derive(Serialize)]
struct Report {
    generated_unix_seconds: u64,
    architecture: &'static str,
    available_parallelism: usize,
    valkey_version: String,
    config: Config,
    results: Vec<CaseResult>,
}

#[derive(Deserialize)]
struct Ready {
    event: String,
    backend: String,
    pid: u32,
    resp_addr: SocketAddr,
    ring_size: usize,
}

struct Binaries {
    mutex_proxy: PathBuf,
    arctic_proxy: PathBuf,
    valkey: String,
}

#[derive(Clone, Copy)]
struct CaseSpec {
    target: Target,
    workload: Workload,
    clients: usize,
    repetition: usize,
    operations: usize,
    seed: u64,
}

#[derive(Clone, Copy)]
struct WorkerSpec {
    workload: Workload,
    client_id: usize,
    clients: usize,
    operations: usize,
    seed: u64,
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

struct RespClient {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl RespClient {
    fn connect(addr: SocketAddr) -> Result<Self> {
        let writer = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
        writer.set_nodelay(true)?;
        writer.set_read_timeout(Some(Duration::from_secs(20)))?;
        writer.set_write_timeout(Some(Duration::from_secs(20)))?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Self { reader, writer })
    }

    fn command(&mut self, argv: &[&[u8]]) -> Result<Reply> {
        self.writer.write_all(&encode_request(argv))?;
        self.writer.flush()?;
        Ok(read_reply(&mut self.reader)?)
    }
}

struct TargetProcess {
    child: Child,
    addr: SocketAddr,
}

impl TargetProcess {
    fn spawn(target: Target, binaries: &Binaries) -> Result<Self> {
        match target {
            Target::MutexProxy => Self::spawn_proxy(target, &binaries.mutex_proxy, "mem"),
            Target::ArcticProxy => Self::spawn_proxy(target, &binaries.arctic_proxy, "arctic"),
            Target::Valkey => Self::spawn_valkey(&binaries.valkey),
        }
    }

    fn spawn_proxy(target: Target, binary: &Path, backend: &str) -> Result<Self> {
        ensure!(
            binary.is_file(),
            "missing proxy benchmark binary: {}",
            binary.display()
        );
        let mut child = Command::new(binary)
            .arg(backend)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn {}", binary.display()))?;
        let stdout = child
            .stdout
            .take()
            .context("proxy target stdout was not piped")?;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = tx.send(result);
        });
        let readiness = (|| -> Result<Ready> {
            let line = rx
                .recv_timeout(Duration::from_secs(30))
                .context("timed out waiting for proxy target readiness")?
                .context("read proxy target readiness")?;
            let ready: Ready = serde_json::from_str(line.trim())
                .with_context(|| format!("parse readiness from {line:?}"))?;
            ensure!(ready.event == "ready", "unexpected readiness event");
            ensure!(
                ready.backend == target.name(),
                "target reported wrong backend"
            );
            ensure!(ready.pid == child.id(), "target reported wrong pid");
            ensure!(
                ready.ring_size == 3,
                "target did not launch a three-node ring"
            );
            Ok(ready)
        })();
        let ready = match readiness {
            Ok(ready) => ready,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        Ok(Self {
            child,
            addr: ready.resp_addr,
        })
    }

    fn spawn_valkey(binary: &str) -> Result<Self> {
        let addr = pick_addr()?;
        let mut child = Command::new(binary)
            .args([
                "--port",
                &addr.port().to_string(),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn {binary}"))?;
        for _ in 0..200 {
            if let Ok(mut client) = RespClient::connect(addr)
                && matches!(
                    client.command(&[b"PING"]),
                    Ok(Reply::Simple(ref value)) if value == "PONG"
                )
            {
                return Ok(Self { child, addr });
            }
            if child.try_wait()?.is_some() {
                bail!("{binary} exited before readiness");
            }
            thread::sleep(Duration::from_millis(25));
        }
        let _ = child.kill();
        let _ = child.wait();
        bail!("timed out waiting for {binary} readiness");
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
            self.child.wait()?;
        }
        ensure!(self.child.try_wait()?.is_some(), "target survived cleanup");
        Ok(())
    }
}

impl Drop for TargetProcess {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct WorkerResult {
    latencies_ns: Vec<u64>,
    updates: BTreeMap<usize, Vec<u8>>,
}

fn pick_addr() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

fn make_keys(count: usize) -> Arc<Vec<Vec<u8>>> {
    Arc::new(
        (0..count)
            .map(|index| format!("key-{index:08}").into_bytes())
            .collect(),
    )
}

fn write_value(client: usize, operation: usize) -> Vec<u8> {
    format!("{client:08x}{operation:024x}").into_bytes()
}

fn preload(addr: SocketAddr, keys: &[Vec<u8>]) -> Result<()> {
    let mut client = RespClient::connect(addr)?;
    for key in keys {
        let reply = client.command(&[b"SET", key, SEED_VALUE])?;
        ensure!(
            reply == Reply::Simple("OK".into()),
            "preload SET failed: {reply:?}"
        );
    }
    Ok(())
}

fn worker(
    mut client: RespClient,
    barrier: Arc<Barrier>,
    keys: Arc<Vec<Vec<u8>>>,
    spec: WorkerSpec,
) -> Result<WorkerResult> {
    let shard_len = (keys.len() - spec.client_id).div_ceil(spec.clients);
    let mut rng = DeterministicRng::new(
        spec.seed ^ spec.workload.seed_tag() ^ ((spec.client_id as u64) << 32),
    );
    let mut updates = BTreeMap::new();
    let mut latencies_ns = Vec::with_capacity(spec.operations.div_ceil(SAMPLE_EVERY));
    barrier.wait();
    for operation in 0..spec.operations {
        let key_index = spec.client_id + rng.index(shard_len) * spec.clients;
        let key = &keys[key_index];
        let sampled = operation % SAMPLE_EVERY == 0;
        let started = sampled.then(Instant::now);
        if spec.workload.is_write(operation) {
            let value = write_value(spec.client_id, operation);
            let reply = client.command(&[b"SET", key, &value])?;
            ensure!(reply == Reply::Simple("OK".into()), "SET failed: {reply:?}");
            updates.insert(key_index, value);
        } else {
            let expected = updates
                .get(&key_index)
                .map(Vec::as_slice)
                .unwrap_or(SEED_VALUE);
            let reply = client.command(&[b"GET", key])?;
            ensure!(
                reply == Reply::Bulk(expected.to_vec()),
                "GET mismatch for key {key_index}"
            );
        }
        if let Some(started) = started {
            latencies_ns.push(started.elapsed().as_nanos() as u64);
        }
    }
    Ok(WorkerResult {
        latencies_ns,
        updates,
    })
}

fn validate_final(
    addr: SocketAddr,
    keys: &[Vec<u8>],
    updates: &BTreeMap<usize, Vec<u8>>,
) -> Result<usize> {
    let mut client = RespClient::connect(addr)?;
    let samples = keys.len().min(64);
    for sample in 0..samples {
        let key_index = sample * keys.len() / samples;
        let expected = updates
            .get(&key_index)
            .map(Vec::as_slice)
            .unwrap_or(SEED_VALUE);
        let reply = client.command(&[b"GET", &keys[key_index]])?;
        ensure!(
            reply == Reply::Bulk(expected.to_vec()),
            "final GET mismatch for key {key_index}"
        );
    }
    Ok(samples)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted[index]
}

fn run_case(
    spec: CaseSpec,
    keys: Arc<Vec<Vec<u8>>>,
    binaries: &Binaries,
) -> Result<(CaseResult, Vec<TelemetrySample>)> {
    let case_id = format!(
        "{}-{}-c{}-r{}",
        spec.target.name(),
        spec.workload.name(),
        spec.clients,
        spec.repetition + 1
    );
    eprintln!("START {case_id}");
    let mut process = TargetProcess::spawn(spec.target, binaries)?;
    preload(process.addr, &keys).with_context(|| format!("preload {case_id}"))?;

    let mut connections = Vec::with_capacity(spec.clients);
    for _ in 0..spec.clients {
        connections.push(RespClient::connect(process.addr)?);
    }
    let barrier = Arc::new(Barrier::new(spec.clients + 1));
    let stop_telemetry = Arc::new(AtomicBool::new(false));
    let telemetry_thread = spawn_telemetry(
        process.pid(),
        case_id.clone(),
        spec.target,
        Arc::clone(&stop_telemetry),
    );
    let target_cpu_started = process_usage(process.pid())
        .map(|(cpu_ms, _)| cpu_ms)
        .unwrap_or(0.0);
    let client_cpu_started = own_cpu_ms();
    let mut workers = Vec::with_capacity(spec.clients);
    for (client_id, connection) in connections.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let keys = Arc::clone(&keys);
        workers.push(thread::spawn(move || {
            worker(
                connection,
                barrier,
                keys,
                WorkerSpec {
                    workload: spec.workload,
                    client_id,
                    clients: spec.clients,
                    operations: spec.operations,
                    seed: spec.seed ^ spec.repetition as u64,
                },
            )
        }));
    }
    let started = Instant::now();
    barrier.wait();
    let mut latencies = Vec::new();
    let mut updates = BTreeMap::new();
    for handle in workers {
        let result = handle
            .join()
            .map_err(|_| anyhow::anyhow!("worker panicked"))??;
        latencies.extend(result.latencies_ns);
        for (key, value) in result.updates {
            ensure!(
                updates.insert(key, value).is_none(),
                "client key shards overlapped"
            );
        }
    }
    let elapsed = started.elapsed();
    let client_cpu_ms = (own_cpu_ms() - client_cpu_started).max(0.0);
    let target_cpu_ended = process_usage(process.pid())
        .map(|(cpu_ms, _)| cpu_ms)
        .unwrap_or(target_cpu_started);
    stop_telemetry.store(true, Ordering::Relaxed);
    let telemetry = telemetry_thread
        .join()
        .map_err(|_| anyhow::anyhow!("telemetry thread panicked"))?;
    let validated_keys = validate_final(process.addr, &keys, &updates)?;
    process.stop()?;

    latencies.sort_unstable();
    let target_cpu_ms = (target_cpu_ended - target_cpu_started).max(0.0);
    let target_peak_rss_kb = telemetry
        .iter()
        .map(|sample| sample.rss_kb)
        .max()
        .unwrap_or(0);
    let total_operations = spec.clients * spec.operations;
    let result = CaseResult {
        case_id: case_id.clone(),
        target: spec.target,
        workload: spec.workload,
        clients: spec.clients,
        repetition: spec.repetition,
        total_operations,
        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
        operations_per_second: total_operations as f64 / elapsed.as_secs_f64(),
        latency_samples: latencies.len(),
        latency_p50_us: percentile(&latencies, 50) as f64 / 1_000.0,
        latency_p99_us: percentile(&latencies, 99) as f64 / 1_000.0,
        target_cpu_ms,
        target_peak_rss_kb,
        client_cpu_ms,
        validated_keys,
    };
    eprintln!(
        "DONE  {case_id}: {:.0} ops/s p99 {:.3} us",
        result.operations_per_second, result.latency_p99_us
    );
    Ok((result, telemetry))
}

fn spawn_telemetry(
    pid: u32,
    case_id: String,
    target: Target,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<Vec<TelemetrySample>> {
    thread::spawn(move || {
        let started = Instant::now();
        let mut samples = Vec::new();
        loop {
            if let Some((process_cpu_ms, rss_kb)) = process_usage(pid) {
                samples.push(TelemetrySample {
                    case_id: case_id.clone(),
                    target,
                    elapsed_ms: started.elapsed().as_secs_f64() * 1_000.0,
                    process_cpu_ms,
                    rss_kb,
                });
            }
            if stop.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        samples
    })
}

#[cfg(target_os = "linux")]
fn process_usage(pid: u32) -> Option<(f64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    // SAFETY: sysconf reads a process-global constant and retains no pointers.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks_per_second <= 0 {
        return None;
    }
    let cpu_ms = (user_ticks + system_ticks) as f64 * 1_000.0 / ticks_per_second as f64;
    let rss_kb = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some((cpu_ms, rss_kb))
}

#[cfg(not(target_os = "linux"))]
fn process_usage(pid: u32) -> Option<(f64, u64)> {
    let output = Command::new("ps")
        .args(["-o", "time=", "-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let mut fields = text.split_whitespace();
    let cpu_ms = parse_cpu_time(fields.next()?)?;
    let rss_kb = fields.next()?.parse().ok()?;
    Some((cpu_ms, rss_kb))
}

#[cfg(not(target_os = "linux"))]
fn parse_cpu_time(value: &str) -> Option<f64> {
    let (days, clock) = match value.split_once('-') {
        Some((days, clock)) => (days.parse().ok()?, clock),
        None => (0.0, value),
    };
    let fields = clock.split(':').collect::<Vec<_>>();
    let seconds = match fields.as_slice() {
        [minutes, seconds] => minutes.parse::<f64>().ok()? * 60.0 + seconds.parse::<f64>().ok()?,
        [hours, minutes, seconds] => {
            hours.parse::<f64>().ok()? * 3_600.0
                + minutes.parse::<f64>().ok()? * 60.0
                + seconds.parse::<f64>().ok()?
        }
        _ => return None,
    };
    Some((days * 86_400.0 + seconds) * 1_000.0)
}

fn own_cpu_ms() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage on a successful call.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return 0.0;
    }
    // SAFETY: the successful call above initialized usage.
    let usage = unsafe { usage.assume_init() };
    let user = usage.ru_utime.tv_sec as f64 * 1_000.0 + usage.ru_utime.tv_usec as f64 / 1_000.0;
    let system = usage.ru_stime.tv_sec as f64 * 1_000.0 + usage.ru_stime.tv_usec as f64 / 1_000.0;
    user + system
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn selected_names(name: &str, allowed: &[&str]) -> Vec<String> {
    let requested: BTreeSet<String> = std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_else(|| allowed.iter().map(|value| (*value).to_owned()).collect());
    for value in &requested {
        assert!(
            allowed.contains(&value.as_str()),
            "unknown {name} value '{value}'"
        );
    }
    allowed
        .iter()
        .filter(|value| requested.contains(**value))
        .map(|value| (*value).to_owned())
        .collect()
}

fn list_usize(name: &str, defaults: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|part| part.trim().parse::<usize>().expect("invalid integer list"))
                .filter(|value| *value > 0)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| defaults.to_vec())
}

fn valkey_binary() -> String {
    std::env::var("PROXY_BENCH_VALKEY_BIN").unwrap_or_else(|_| "valkey-server".into())
}

fn valkey_version(binary: &str) -> Result<String> {
    let output = Command::new(binary).arg("--version").output()?;
    ensure!(output.status.success(), "{binary} --version failed");
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn output_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var(name).with_context(|| format!("{name} is required"))?,
    ))
}

fn append_checkpoint(result: &CaseResult, telemetry: &[TelemetrySample]) -> Result<()> {
    let checkpoint = output_path("PROXY_BENCH_CHECKPOINT_OUTPUT")?;
    let mut writer = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(checkpoint)?;
    serde_json::to_writer(&mut writer, result)?;
    writer.write_all(b"\n")?;

    let telemetry_output = output_path("PROXY_BENCH_TELEMETRY_OUTPUT")?;
    let mut writer = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(telemetry_output)?;
    for sample in telemetry {
        serde_json::to_writer(&mut writer, sample)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn write_report(report: &Report) -> Result<()> {
    let output = output_path("PROXY_BENCH_OUTPUT")?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    serde_json::to_writer_pretty(BufWriter::new(File::create(&output)?), report)?;
    Ok(())
}

fn main() -> Result<()> {
    const TARGETS: [&str; 3] = ["mutex-proxy", "arctic-proxy", "valkey"];
    const WORKLOADS: [&str; 3] = ["read-only", "read-90-write-10", "write-only"];

    let keys_count = env_usize("PROXY_BENCH_KEYS", 1_000);
    let default_operations = env_usize("PROXY_BENCH_OPS_PER_CLIENT", 2_000);
    let operation_counts = BTreeMap::from([
        (
            "read-only".to_string(),
            env_usize("PROXY_BENCH_READ_OPS_PER_CLIENT", default_operations),
        ),
        (
            "read-90-write-10".to_string(),
            env_usize("PROXY_BENCH_MIXED_OPS_PER_CLIENT", default_operations),
        ),
        (
            "write-only".to_string(),
            env_usize("PROXY_BENCH_WRITE_OPS_PER_CLIENT", default_operations),
        ),
    ]);
    let repetitions = env_usize("PROXY_BENCH_REPETITIONS", 1);
    let client_counts = list_usize("PROXY_BENCH_CLIENTS", &[1, 8]);
    let target_names = selected_names("PROXY_BENCH_TARGETS", &TARGETS);
    let workload_names = selected_names("PROXY_BENCH_WORKLOADS", &WORKLOADS);
    let seed = env_u64("PROXY_BENCH_SEED", DEFAULT_SEED);
    ensure!(
        keys_count >= *client_counts.iter().max().unwrap_or(&1),
        "fewer keys than clients"
    );

    let binaries = Binaries {
        mutex_proxy: PathBuf::from(
            std::env::var("PROXY_BENCH_MUTEX_BIN").context("PROXY_BENCH_MUTEX_BIN is required")?,
        ),
        arctic_proxy: PathBuf::from(
            std::env::var("PROXY_BENCH_ARCTIC_BIN")
                .context("PROXY_BENCH_ARCTIC_BIN is required")?,
        ),
        valkey: valkey_binary(),
    };
    let targets: Vec<_> = target_names
        .iter()
        .map(|name| Target::from_name(name))
        .collect();
    let workloads: Vec<_> = workload_names
        .iter()
        .map(|name| Workload::from_name(name))
        .collect();
    let keys = make_keys(keys_count);
    let mut results = Vec::new();

    for path in [
        output_path("PROXY_BENCH_CHECKPOINT_OUTPUT")?,
        output_path("PROXY_BENCH_TELEMETRY_OUTPUT")?,
    ] {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }

    for workload in workloads {
        let operations = operation_counts[workload.name()];
        for &clients in &client_counts {
            for repetition in 0..repetitions {
                for offset in 0..targets.len() {
                    let target = targets[(offset + repetition) % targets.len()];
                    let (result, samples) = run_case(
                        CaseSpec {
                            target,
                            workload,
                            clients,
                            repetition,
                            operations,
                            seed,
                        },
                        Arc::clone(&keys),
                        &binaries,
                    )?;
                    append_checkpoint(&result, &samples)?;
                    results.push(result);
                }
            }
        }
    }

    let report = Report {
        generated_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        architecture: std::env::consts::ARCH,
        available_parallelism: thread::available_parallelism().map_or(1, usize::from),
        valkey_version: valkey_version(&binaries.valkey)?,
        config: Config {
            keys: keys_count,
            operations_per_client: operation_counts,
            client_counts,
            targets: target_names,
            workloads: workload_names,
            repetitions,
            latency_sample_every: SAMPLE_EVERY,
            value_bytes: SEED_VALUE.len(),
            seed,
        },
        results,
    };
    write_report(&report)?;
    Ok(())
}
