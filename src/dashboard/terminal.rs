// dashboard/terminal.rs — Live terminal dashboard (Advanced Feature: Dashboard).
// Refreshes every 1s via ANSI escape codes. Shows all component metrics in real time.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::ingestion::sse_client::SseState;
use crate::metrics::collector::MetricsCollector;
use crate::models::{EditTier, SystemMode, ScheduledFault, PipelineMode};

const REFRESH_MS: u64 = 1000;
const W: usize = 70; // display width

/// Async dashboard loop: renders metrics snapshot to terminal every 1s.
pub async fn run_dashboard_async(
    state: Arc<SseState>,
    metrics: MetricsCollector,
    mode: PipelineMode,
    duration_secs: u64,
    faults_enabled: bool,
) {
    let start = Instant::now();
    let faults = if faults_enabled { ScheduledFault::default_schedule() } else { vec![] };

    loop {
        if state.should_stop.load(Ordering::Acquire) { return; }
        let elapsed = start.elapsed().as_secs();
        if elapsed >= duration_secs { return; }

        let snap = metrics.snapshot();
        let connected = state.connected.load(Ordering::Acquire);
        render(&snap, mode, connected, elapsed, duration_secs - elapsed, &faults);

        tokio::time::sleep(Duration::from_millis(REFRESH_MS)).await;
    }
}

/// Threaded dashboard loop: blocking version for std::thread pipeline.
pub fn run_dashboard_threaded(
    state: Arc<SseState>,
    metrics: MetricsCollector,
    mode: PipelineMode,
    duration_secs: u64,
    faults_enabled: bool,
) {
    let start = Instant::now();
    let faults = if faults_enabled { ScheduledFault::default_schedule() } else { vec![] };

    loop {
        if state.should_stop.load(Ordering::Acquire) { return; }
        let elapsed = start.elapsed().as_secs();
        if elapsed >= duration_secs { return; }

        let snap = metrics.snapshot();
        let connected = state.connected.load(Ordering::Acquire);
        render(&snap, mode, connected, elapsed, duration_secs - elapsed, &faults);

        std::thread::sleep(Duration::from_millis(REFRESH_MS));
    }
}

/// Clears terminal and renders full dashboard: stream, latency, deadlines, drift, sync, leaderboard, faults.
fn render(
    snap: &crate::metrics::collector::DashboardSnapshot,
    mode: PipelineMode,
    connected: bool,
    elapsed: u64,
    remaining: u64,
    faults: &[ScheduledFault],
) {
    let rst = "\x1b[0m";
    let bld = "\x1b[1m";
    let dim = "\x1b[2m";
    let grn = "\x1b[32m";
    let ylw = "\x1b[33m";
    let red = "\x1b[31m";
    let cyn = "\x1b[36m";
    let wht = "\x1b[37m";

    let border = match snap.mode {
        SystemMode::Normal => grn,
        SystemMode::Degraded1 => ylw,
        _ => red,
    };

    print!("\x1b[2J\x1b[H");

    let line = "═".repeat(W);

    println!("{border}{line}{rst}");
    println!("{bld}  Wiki-RTS{rst}   Mode: {ylw}{mode}{rst}   State: {border}{}{rst}   Uptime: {wht}{elapsed}s{rst}",
        snap.mode);
    println!("{border}{line}{rst}");

    if snap.mode != SystemMode::Normal {
        let desc = match snap.mode {
            SystemMode::Degraded1 => "Shedding T5 (minor bots)",
            SystemMode::Degraded2 => "Shedding T4+T5 (all bots)",
            SystemMode::Degraded3 => "Shedding T3-T5 (human main only)",
            SystemMode::Degraded4 => "Shedding T2-T5 (T1 only)",
            _ => "",
        };
        println!("  {red}! {}: {}{rst}", snap.mode, desc);
        println!();
    }

    println!("  {cyn}STREAM{rst}");
    let conn = if connected { format!("{grn}CONNECTED{rst}") } else { format!("{red}DISCONNECTED{rst}") };
    let fill_pct = if snap.channel_capacity > 0 {
        snap.channel_fill as f64 / snap.channel_capacity as f64 * 100.0
    } else { 0.0 };
    let fill_color = if fill_pct > 70.0 { ylw } else { grn };
    println!("  Connection: {}   Events/sec: {wht}{:.0}{rst}", conn, snap.events_per_sec);
    println!("  Channel: {fill_color}{}/{} ({:.0}%){rst}   Total: {wht}{}{rst}   H: {wht}{}{rst}   B: {wht}{}{rst}",
        snap.channel_fill, snap.channel_capacity, fill_pct,
        snap.total_processed, snap.human_count, snap.bot_count);
    println!("  Overflows: {}{}{}   Shed: {}{}{}",
        if snap.overflow_count > 0 { ylw } else { grn }, snap.overflow_count, rst,
        if snap.shed_count > 0 { ylw } else { grn }, snap.shed_count, rst);
    println!();

    println!("  {cyn}LATENCY{rst}                            {cyn}DEADLINES{rst}");
    let lat = &snap.latency;
    let lat_strs = [
        format!("  p50: {grn}{}{rst}", lat.p50_ms()),
        format!("  p90: {grn}{}{rst}", lat.p90_ms()),
        format!("  p99: {}{}{rst}",
            if lat.p99 > 3000 { red } else { grn }, lat.p99_ms()),
    ];

    for (i, tier) in EditTier::all_tiers().iter().enumerate() {
        let (met, total) = snap.deadline_by_tier.get(tier).copied().unwrap_or((0, 0));
        let dl_ms = tier.deadline_us() as f64 / 1000.0;

        let dl_str = if snap.mode.should_shed(*tier) {
            format!("{ylw}-- SHEDDING --{rst}")
        } else if total > 0 {
            let pct = met as f64 / total as f64 * 100.0;
            let c = if pct >= 99.0 { grn } else if pct >= 95.0 { ylw } else { red };
            format!("{} ({:.0}ms): {c}{}/{} {:.1}%{rst}", tier.label(), dl_ms, met, total, pct)
        } else {
            format!("{} ({:.0}ms): waiting...", tier.label(), dl_ms)
        };

        let lat_part = if i < lat_strs.len() { &lat_strs[i] } else { &String::new() };
        println!("{:<38} {}", lat_part, dl_str);
    }
    println!();

    println!("  {cyn}SCHEDULING DRIFT (p99){rst}");
    for tier in EditTier::all_tiers() {
        if snap.mode.should_shed(*tier) {
            println!("  {:25} {ylw}-- SHEDDING --{rst}", tier.label());
        } else if let Some(p) = snap.drift_percentiles.get(tier) {
            let bar_len = ((p.p99 as f64 / 3000.0) * 20.0).min(20.0) as usize;
            let bar: String = "█".repeat(bar_len);
            let c = if p.p99 > 2000 { ylw } else { grn };
            println!("  {:25} {c}{}{rst}  {c}{}{rst}", tier.label(), p.p99_ms(), bar);
        } else {
            println!("  {:25} waiting...", tier.label());
        }
    }
    println!();

    println!("  {cyn}SYNC (parallel leaderboards){rst}");
    println!("  Atomic: wait: {grn}lock-free{rst}  {grn}MAIN PATH{rst} (DashMap — no contention possible)");
    println!("  Mutex:  wait: {wht}{:.2}ms{rst}  contention: {wht}{}{rst}  inversions: {}{}{rst}   shadow",
        snap.mutex_avg_wait_us as f64 / 1000.0,
        snap.mutex_contention,
        if snap.mutex_inversions > 0 { ylw } else { grn },
        snap.mutex_inversions);
    println!("  RwLock: wait: {grn}{:.2}ms{rst}  contention: {grn}{}{rst}   inversions: {grn}{}{rst}   shadow",
        snap.rwlock_avg_wait_us as f64 / 1000.0,
        snap.rwlock_contention,
        snap.rwlock_inversions);
    println!();

    println!("  {cyn}LEADERBOARD{rst}");
    let max_count = snap.top_domains.first().map(|d| d.1).unwrap_or(1).max(1);
    for (i, (domain, count)) in snap.top_domains.iter().take(3).enumerate() {
        let bar_len = ((*count as f64 / max_count as f64) * 20.0) as usize;
        let bar: String = "█".repeat(bar_len);
        let frozen = if snap.mode != SystemMode::Normal
            && (domain.contains("wikidata") || domain.contains("commons")) {
            format!(" {ylw}frozen{rst}")
        } else {
            String::new()
        };
        println!("  {}. {:26} {:>4}  {dim}{}{rst}{}", i + 1, domain, count, bar, frozen);
    }
    println!();

    if !faults.is_empty() {
        println!("  {cyn}FAULT SCHEDULE{rst}");
        for fault in faults {
            let dur = fault_duration(&fault.fault);
            let status = if elapsed >= fault.trigger_at_secs + dur {
                format!("{grn}✓ completed{rst}")
            } else if elapsed >= fault.trigger_at_secs {
                let left = (fault.trigger_at_secs + dur).saturating_sub(elapsed);
                format!("{red}▶ ACTIVE — {}s left{rst}", left)
            } else {
                let until = fault.trigger_at_secs.saturating_sub(elapsed);
                if until <= 5 {
                    format!("{ylw}○ in {}s{rst}", until)
                } else {
                    format!("{dim}○ scheduled{rst}")
                }
            };
            println!("    {:35} {}", fault.label(), status);
        }
        println!();
    }

    println!("{border}{line}{rst}");
    println!("{dim} Automated run — {}s remaining{rst}", remaining);

    let _ = io::stdout().flush();
}

/// Returns the duration in seconds for a given fault type.
fn fault_duration(fault: &crate::models::FaultType) -> u64 {
    match fault {
        crate::models::FaultType::NetworkDrop { duration_secs } => *duration_secs,
        crate::models::FaultType::CpuSpike { duration_secs, .. } => *duration_secs,
        crate::models::FaultType::ChannelFlood { duration_secs } => *duration_secs,
    }
}
