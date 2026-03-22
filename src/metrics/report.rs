// metrics/report.rs — Final report generation and JSON export.
// Prints comprehensive summary covering all 5 components + advanced features.

use crate::models::{EditTier, PipelineMode};
use crate::metrics::collector::MetricsCollector;

/// Prints the final run report to stdout with all component metrics.
pub fn print_final_report(mode: PipelineMode, duration: u64, collector: &MetricsCollector) {
    let snap = collector.snapshot();
    let total_events = snap.total_processed + snap.overflow_count as usize + snap.shed_count as usize;

    println!("\n\x1b[33m═══════════════════════════════════════════════════════════════\x1b[0m");
    println!("\x1b[1m                      FINAL RUN REPORT\x1b[0m");
    println!("\x1b[1m                 Mode: {} | Duration: {}s\x1b[0m", mode, duration);
    println!("\x1b[33m═══════════════════════════════════════════════════════════════\x1b[0m\n");

    println!("\x1b[36mTHROUGHPUT\x1b[0m");
    println!("  Received: {}   Processed: {}   Overflow: {}   Shed: {}",
        total_events, snap.total_processed, snap.overflow_count, snap.shed_count);
    println!("  Effective: {:.1} events/sec\n",
        snap.total_processed as f64 / duration as f64);

    println!("\x1b[36mLATENCY (p50/p90/p99/max)\x1b[0m");
    println!("  {}  {}  {}  {}\n",
        snap.latency.p50_ms(), snap.latency.p90_ms(),
        snap.latency.p99_ms(), snap.latency.max_ms());

    println!("\x1b[36mDEADLINE COMPLIANCE\x1b[0m");
    for tier in EditTier::all_tiers() {
        if let Some(&(met, total)) = snap.deadline_by_tier.get(tier) {
            let pct = if total > 0 { (met as f64 / total as f64) * 100.0 } else { 100.0 };
            let deadline_ms = tier.deadline_us() as f64 / 1000.0;
            let missed = total - met;
            if snap.mode.should_shed(*tier) {
                println!("  {:25} ({:.0}ms): SHEDDING", tier.label(), deadline_ms);
            } else {
                println!("  {:25} ({:.0}ms): {}/{} {:.1}%{}",
                    tier.label(), deadline_ms, met, total, pct,
                    if missed > 0 { format!("  {} miss", missed) } else { String::new() });
            }
        }
    }
    println!();

    println!("\x1b[36mSCHEDULING DRIFT\x1b[0m");
    for tier in EditTier::all_tiers() {
        if let Some(perc) = snap.drift_percentiles.get(tier) {
            println!("  {} ({} edits): p50={} p90={} p99={}",
                tier.label(), perc.count,
                perc.p50_ms(), perc.p90_ms(), perc.p99_ms());
        }
    }
    let t1_p99 = snap.drift_percentiles.get(&EditTier::Tier1HumanMainNonMinor)
        .map(|p| p.p99).unwrap_or(0);
    let t4_p99 = snap.drift_percentiles.get(&EditTier::Tier4BotNonMinor)
        .map(|p| p.p99).unwrap_or(0);
    if t4_p99 > t1_p99 {
        println!("  Human edits have {:.1}x lower p99 drift than bot edits.",
            t4_p99 as f64 / t1_p99.max(1) as f64);
    }
    println!();

    println!("\x1b[36mSYNC BENCHMARK (Mutex vs RwLock vs Atomic)\x1b[0m");
    println!("  Atomic/DashMap: lock-free (main path)");
    println!("  Mutex:  wait={:.2}ms  contention={}  inversions={}",
        snap.mutex_avg_wait_us as f64 / 1000.0,
        snap.mutex_contention, snap.mutex_inversions);
    println!("  RwLock: wait={:.2}ms  contention={}  inversions={}",
        snap.rwlock_avg_wait_us as f64 / 1000.0,
        snap.rwlock_contention, snap.rwlock_inversions);
    println!();

    println!("\x1b[36mLEADERBOARD\x1b[0m");
    for (i, (domain, count)) in snap.top_domains.iter().enumerate() {
        println!("  {}. {:30} {} edits", i + 1, domain, count);
    }
    println!();

    println!("\x1b[36mFAULT TOLERANCE\x1b[0m");
    println!("  Watchdog triggers: {}", snap.watchdog_triggers);
    for event in &snap.fault_events {
        println!("  {}", event);
    }
    println!();

    println!("\x1b[36mHEAP ALLOCATION (hot path)\x1b[0m");
    println!("  Allocations: \x1b[32m{}\x1b[0m (custom GlobalAlloc counter)",
        snap.hot_path_allocs);

    println!("\n\x1b[33m═══════════════════════════════════════════════════════════════\x1b[0m");
}

/// Exports all metrics as a JSON string for comparison tooling.
pub fn export_json(mode: PipelineMode, duration: u64, collector: &MetricsCollector) -> String {
    let snap = collector.snapshot();
    let mut json = String::from("{\n");
    json.push_str(&format!("  \"mode\": \"{}\",\n", mode));
    json.push_str(&format!("  \"duration_secs\": {},\n", duration));
    json.push_str(&format!("  \"total_processed\": {},\n", snap.total_processed));
    json.push_str(&format!("  \"overflow_count\": {},\n", snap.overflow_count));
    json.push_str(&format!("  \"shed_count\": {},\n", snap.shed_count));
    json.push_str(&format!("  \"latency_p50_us\": {},\n", snap.latency.p50));
    json.push_str(&format!("  \"latency_p90_us\": {},\n", snap.latency.p90));
    json.push_str(&format!("  \"latency_p99_us\": {},\n", snap.latency.p99));
    json.push_str(&format!("  \"latency_max_us\": {},\n", snap.latency.max));

    json.push_str("  \"drift\": {\n");
    for (i, tier) in EditTier::all_tiers().iter().enumerate() {
        if let Some(p) = snap.drift_percentiles.get(tier) {
            json.push_str(&format!("    \"{}\": {{\"p50\": {}, \"p90\": {}, \"p99\": {}, \"count\": {}}}",
                tier.label(), p.p50, p.p90, p.p99, p.count));
        } else {
            json.push_str(&format!("    \"{}\": {{\"p50\": 0, \"p90\": 0, \"p99\": 0, \"count\": 0}}",
                tier.label()));
        }
        if i < EditTier::all_tiers().len() - 1 { json.push_str(",\n"); } else { json.push('\n'); }
    }
    json.push_str("  },\n");

    json.push_str("  \"deadline\": {\n");
    for (i, tier) in EditTier::all_tiers().iter().enumerate() {
        let (met, total) = snap.deadline_by_tier.get(tier).copied().unwrap_or((0, 0));
        json.push_str(&format!("    \"{}\": {{\"met\": {}, \"total\": {}}}",
            tier.label(), met, total));
        if i < EditTier::all_tiers().len() - 1 { json.push_str(",\n"); } else { json.push('\n'); }
    }
    json.push_str("  },\n");

    json.push_str(&format!("  \"mutex_contention\": {},\n", snap.mutex_contention));
    json.push_str(&format!("  \"mutex_inversions\": {},\n", snap.mutex_inversions));
    json.push_str(&format!("  \"mutex_avg_wait_us\": {},\n", snap.mutex_avg_wait_us));
    json.push_str(&format!("  \"rwlock_contention\": {},\n", snap.rwlock_contention));
    json.push_str(&format!("  \"rwlock_inversions\": {},\n", snap.rwlock_inversions));
    json.push_str(&format!("  \"rwlock_avg_wait_us\": {},\n", snap.rwlock_avg_wait_us));
    json.push_str(&format!("  \"watchdog_triggers\": {},\n", snap.watchdog_triggers));
    json.push_str(&format!("  \"hot_path_allocs\": {},\n", snap.hot_path_allocs));

    json.push_str("  \"leaderboard\": [\n");
    for (i, (domain, count)) in snap.top_domains.iter().enumerate() {
        json.push_str(&format!("    {{\"domain\": \"{}\", \"edits\": {}}}",
            domain, count));
        if i < snap.top_domains.len() - 1 { json.push_str(",\n"); } else { json.push('\n'); }
    }
    json.push_str("  ]\n");
    json.push_str("}\n");

    json
}
