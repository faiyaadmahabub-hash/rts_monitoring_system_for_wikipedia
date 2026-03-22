// metrics/comparison.rs — Side-by-side comparison of two pipeline run reports.
// Advanced Feature: Comparative Analysis — Async vs Threaded architecture comparison.

use std::fs;

/// Loads two JSON reports and prints a side-by-side comparison with winner annotations.
pub fn compare_runs(file1: &str, file2: &str) {
    let json1 = fs::read_to_string(file1).expect("Cannot read first report file");
    let json2 = fs::read_to_string(file2).expect("Cannot read second report file");

    let r1: serde_json::Value = serde_json::from_str(&json1).expect("Invalid JSON in file 1");
    let r2: serde_json::Value = serde_json::from_str(&json2).expect("Invalid JSON in file 2");

    let mode1 = r1["mode"].as_str().unwrap_or("RUN1");
    let mode2 = r2["mode"].as_str().unwrap_or("RUN2");

    println!("\n\x1b[33m═══════════════════════════════════════════════════════════════════\x1b[0m");
    println!("\x1b[1m                      ARCHITECTURE COMPARISON\x1b[0m");
    println!("\x1b[1m            {}  vs  {}\x1b[0m", mode1, mode2);
    println!("\x1b[33m═══════════════════════════════════════════════════════════════════\x1b[0m\n");

    let get = |v: &serde_json::Value, key: &str| -> u64 {
        v[key].as_u64().unwrap_or(0)
    };

    println!("\x1b[36m{:<30} {:>12} {:>12} {:>12}\x1b[0m", "LATENCY", mode1, mode2, "WINNER");
    println!("{}", "─".repeat(68));
    compare_row_us("p50 (median)", get(&r1, "latency_p50_us"), get(&r2, "latency_p50_us"), mode1, mode2, true);
    compare_row_us("p90", get(&r1, "latency_p90_us"), get(&r2, "latency_p90_us"), mode1, mode2, true);
    compare_row_us("p99 (tail)", get(&r1, "latency_p99_us"), get(&r2, "latency_p99_us"), mode1, mode2, true);
    compare_row_us("max", get(&r1, "latency_max_us"), get(&r2, "latency_max_us"), mode1, mode2, true);
    println!();

    println!("\x1b[36m{:<30} {:>12} {:>12} {:>12}\x1b[0m", "THROUGHPUT", mode1, mode2, "WINNER");
    println!("{}", "─".repeat(68));
    compare_row_int("Events processed", get(&r1, "total_processed"), get(&r2, "total_processed"), mode1, mode2, false);
    compare_row_int("Overflow events", get(&r1, "overflow_count"), get(&r2, "overflow_count"), mode1, mode2, true);
    println!();

    println!("\x1b[36m{:<30} {:>12} {:>12} {:>12}\x1b[0m", "SYNC CONTENTION", mode1, mode2, "WINNER");
    println!("{}", "─".repeat(68));
    compare_row_int("Mutex contention", get(&r1, "mutex_contention"), get(&r2, "mutex_contention"), mode1, mode2, true);
    compare_row_int("Mutex inversions", get(&r1, "mutex_inversions"), get(&r2, "mutex_inversions"), mode1, mode2, true);
    compare_row_us("Mutex avg wait", get(&r1, "mutex_avg_wait_us"), get(&r2, "mutex_avg_wait_us"), mode1, mode2, true);
    compare_row_int("RwLock contention", get(&r1, "rwlock_contention"), get(&r2, "rwlock_contention"), mode1, mode2, true);
    println!();

    let p99_1 = get(&r1, "latency_p99_us");
    let p99_2 = get(&r2, "latency_p99_us");
    let throughput_1 = get(&r1, "total_processed");
    let throughput_2 = get(&r2, "total_processed");

    let tail_winner = if p99_1 < p99_2 { mode1 } else { mode2 };
    let tail_loser = if p99_1 < p99_2 { mode2 } else { mode1 };
    let throughput_winner = if throughput_1 > throughput_2 { mode1 } else { mode2 };

    println!("\x1b[1mCONCLUSION\x1b[0m");
    println!("  p99 latency: {} ({:.2}ms) vs {} ({:.2}ms)",
        tail_winner, p99_1.min(p99_2) as f64 / 1000.0,
        tail_loser, p99_1.max(p99_2) as f64 / 1000.0);
    println!("  Throughput:  {} ({} vs {})",
        throughput_winner,
        throughput_1.max(throughput_2), throughput_1.min(throughput_2));

    println!("\n\x1b[33m═══════════════════════════════════════════════════════════════════\x1b[0m");
}

/// Prints a comparison row with microsecond values formatted as milliseconds.
fn compare_row_us(label: &str, v1: u64, v2: u64, m1: &str, m2: &str, lower_is_better: bool) {
    let ms1 = format!("{:.2}ms", v1 as f64 / 1000.0);
    let ms2 = format!("{:.2}ms", v2 as f64 / 1000.0);
    let winner = if v1 == v2 {
        "Tie".to_string()
    } else if (lower_is_better && v1 < v2) || (!lower_is_better && v1 > v2) {
        let pct = ((v2 as f64 - v1 as f64) / v2 as f64 * 100.0).abs();
        format!("{} -{:.0}%", m1, pct)
    } else {
        let pct = ((v1 as f64 - v2 as f64) / v1 as f64 * 100.0).abs();
        format!("{} -{:.0}%", m2, pct)
    };
    println!("  {:<28} {:>12} {:>12} {:>12}", label, ms1, ms2, winner);
}

/// Prints a comparison row with integer values.
fn compare_row_int(label: &str, v1: u64, v2: u64, m1: &str, m2: &str, lower_is_better: bool) {
    let winner = if v1 == v2 {
        "Tie".to_string()
    } else if (lower_is_better && v1 < v2) || (!lower_is_better && v1 > v2) {
        let pct = ((v2 as f64 - v1 as f64) / v2 as f64 * 100.0).abs();
        format!("{} -{:.0}%", m1, pct)
    } else {
        let pct = ((v1 as f64 - v2 as f64) / v1 as f64 * 100.0).abs();
        format!("{} -{:.0}%", m2, pct)
    };
    println!("  {:<28} {:>12} {:>12} {:>12}", label, v1, v2, winner);
}
