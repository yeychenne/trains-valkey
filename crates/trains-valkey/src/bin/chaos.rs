//! `trains-valkey-chaos` — the EC2 fis-kill workload + no-acked-write-loss
//! verifier (PR-RD-4 / G5).
//!
//! Writes a monotonic `SET` stream to a proxy's RESP port (recording only
//! `+OK`-acked writes), pauses for a hold window during which the bench
//! coordinator injects the `fis-kill` fault, keeps writing *through* the masked
//! window, then verifies no acked write was lost on the survivors. Exits
//! non-zero on any acked-write loss or divergence.
//!
//! Three modes:
//! - `full` — single-process load + cross-engine verify (legacy; assumes
//!   each survivor's engine is network-reachable from the driver host).
//!   Used by `tests/redis_backend.rs::chaos_driver_*`.
//! - `load` — load + hold + load; write the acked-set to `--acked-out`
//!   and exit. For the option-B EC2 split: Valkey stays loopback-only;
//!   the verify step runs on each survivor.
//! - `verify-local` — read the acked-set from `--acked-in`, query a SINGLE
//!   engine (typically `127.0.0.1:6379`), write a per-engine
//!   `PartialReport` to `--report-out`. Run on each survivor;
//!   a small coordinator script aggregates the partials.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use trains_valkey::chaos::{run_load, verify, verify_one, AckedWrites};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Mode {
    /// Legacy single-process flow: load + cross-engine verify.
    Full,
    /// EC2-split phase 1: load + hold + load; emit acked-set JSON.
    Load,
    /// EC2-split phase 2: read acked-set, query one local engine, emit partial.
    VerifyLocal,
}

#[derive(Parser)]
#[command(name = "trains-valkey-chaos", about = "TRAINS-replicated Redis chaos workload + verifier")]
struct Cli {
    /// Pipeline mode (see binary docs).
    #[arg(long, value_enum, default_value_t = Mode::Full)]
    mode: Mode,

    // ── full + load ──────────────────────────────────────────────────────────
    /// Proxy RESP address to drive writes at (the load target).
    #[arg(long, required_if_eq_any([("mode", "full"), ("mode", "load")]))]
    resp: Option<SocketAddr>,
    /// Total number of SET writes.
    #[arg(long, default_value_t = 1000)]
    count: usize,
    /// Hold (seconds) at the halfway point for the coordinator to inject the
    /// fault; the second half is written through the masked window.
    #[arg(long, default_value_t = 15)]
    hold_secs: u64,
    /// Per-write reply deadline (seconds). A write whose `+OK` doesn't arrive in
    /// time — e.g. its train was lost in a masked crash — is **abandoned** (left
    /// out of the acked set; it was never acked) and the connection is rebuilt,
    /// so the load measures acked-write loss *through* a failover instead of
    /// hanging on the in-flight write. `0` blocks forever (legacy).
    #[arg(long, default_value_t = 5)]
    abandon_secs: u64,
    /// Key prefix.
    #[arg(long, default_value = "chaos:k")]
    prefix: String,

    // ── load: output ─────────────────────────────────────────────────────────
    /// `load` mode: path to write the acked-set JSON.
    #[arg(long, required_if_eq("mode", "load"))]
    acked_out: Option<PathBuf>,

    // ── full only: cross-engine verify ───────────────────────────────────────
    /// `full` mode: comma-separated SURVIVING engine addresses (exclude victim).
    #[arg(long, value_delimiter = ',')]
    engines: Vec<SocketAddr>,

    // ── verify-local ─────────────────────────────────────────────────────────
    /// `verify-local` mode: path to read the acked-set JSON.
    #[arg(long, required_if_eq("mode", "verify-local"))]
    acked_in: Option<PathBuf>,
    /// `verify-local` mode: single engine address to query (typically
    /// `127.0.0.1:6379` so the engine stays loopback-only).
    #[arg(long, required_if_eq("mode", "verify-local"))]
    engine: Option<SocketAddr>,
    /// `verify-local` mode: identifier surfaced in the partial report (e.g.
    /// the node id, instance id, or private IP).
    #[arg(long, default_value = "node-?")]
    label: String,
    /// `verify-local` mode: where to write the `PartialReport` JSON.
    #[arg(long, required_if_eq("mode", "verify-local"))]
    report_out: Option<PathBuf>,

    /// Engine password (if `requirepass` is set).
    #[arg(long)]
    password: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.mode {
        Mode::Full => run_full(&cli),
        Mode::Load => run_load_phase(&cli),
        Mode::VerifyLocal => run_verify_local(&cli),
    }
}

fn drive_load(cli: &Cli) -> Result<AckedWrites> {
    let resp = cli.resp.context("--resp required")?;
    let half = cli.count / 2;
    // 0 ⇒ block forever (legacy); >0 ⇒ abandon a write after this many seconds.
    let abandon = (cli.abandon_secs > 0).then(|| Duration::from_secs(cli.abandon_secs));

    eprintln!("[chaos] writing 0..{half} to {resp}");
    let mut acked = run_load(resp, &cli.prefix, 0..half, abandon).context("load phase 1")?;
    eprintln!("[chaos] phase 1 acked {} writes; holding {}s for fault injection", acked.len(), cli.hold_secs);

    std::thread::sleep(Duration::from_secs(cli.hold_secs));

    eprintln!("[chaos] writing {half}..{} through the masked window", cli.count);
    let phase2 = run_load(resp, &cli.prefix, half..cli.count, abandon).context("load phase 2")?;
    acked.extend(phase2);
    eprintln!("[chaos] total acked {} writes", acked.len());

    Ok(acked)
}

fn run_full(cli: &Cli) -> Result<()> {
    let acked = drive_load(cli)?;
    if cli.engines.is_empty() {
        bail!("--engines required in full mode");
    }
    let report = verify(&cli.engines, cli.password.as_deref(), &acked).context("verify")?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    if report.ok() {
        eprintln!(
            "[chaos] PASS — {} acked writes, 0 lost, {} survivors converged (DBSIZE {:?})",
            report.acked_total, report.engines, report.dbsizes
        );
        Ok(())
    } else {
        eprintln!(
            "[chaos] FAIL — {} acked writes lost, converged={} (DBSIZE {:?})",
            report.acked_loss.len(), report.converged, report.dbsizes
        );
        std::process::exit(1);
    }
}

fn run_load_phase(cli: &Cli) -> Result<()> {
    let acked = drive_load(cli)?;
    let path = cli.acked_out.as_ref().context("--acked-out required")?;
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    acked.write_json(BufWriter::new(f)).context("serialize acked set")?;
    eprintln!("[chaos] wrote {} acked entries to {}", acked.len(), path.display());
    Ok(())
}

fn run_verify_local(cli: &Cli) -> Result<()> {
    let acked_in = cli.acked_in.as_ref().context("--acked-in required")?;
    let engine = cli.engine.context("--engine required")?;
    let report_out = cli.report_out.as_ref().context("--report-out required")?;

    let f = File::open(acked_in).with_context(|| format!("open {}", acked_in.display()))?;
    let acked = AckedWrites::read_json(BufReader::new(f)).context("deserialize acked set")?;
    eprintln!("[chaos] verify-local: {} acked entries against {}", acked.len(), engine);

    let partial = verify_one(engine, cli.password.as_deref(), &cli.label, &acked)
        .context("verify_one")?;
    let f = File::create(report_out).with_context(|| format!("create {}", report_out.display()))?;
    serde_json::to_writer_pretty(BufWriter::new(f), &partial).context("write report")?;
    eprintln!(
        "[chaos] {} dbsize={} missing={}",
        partial.engine_label, partial.dbsize, partial.missing_keys.len()
    );
    // Exit non-zero on local loss so SSM surfaces it.
    if !partial.missing_keys.is_empty() {
        std::process::exit(2);
    }
    Ok(())
}
