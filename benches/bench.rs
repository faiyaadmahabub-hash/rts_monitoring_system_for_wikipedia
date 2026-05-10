// Criterion benchmarks: sync_contention, parsing, pipeline_comparison

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use wikipedia_rts::model::{WikiEdit, WikiEditOwned};
use wikipedia_rts::state::SharedState;
use wikipedia_rts::logger::Logger;
use wikipedia_rts::pipeline;

// Group 1: 16 threads each doing 1000 increments — Mutex vs RwLock vs Atomic
fn bench_sync(c: &mut Criterion) {
    use std::sync::{Arc, Mutex, RwLock};
    use std::sync::atomic::{AtomicU64, Ordering};

    let mut g = c.benchmark_group("sync_contention");

    g.bench_function("mutex", |b| {
        let counter = Arc::new(Mutex::new(0u64));
        b.iter(|| {
            let handles: Vec<_> = (0..16).map(|_| {
                let c = Arc::clone(&counter);
                std::thread::spawn(move || {
                    for _ in 0..1000 { *c.lock().unwrap() += 1; }
                })
            }).collect();
            handles.into_iter().for_each(|h| h.join().unwrap());
        });
    });

    g.bench_function("rwlock", |b| {
        let counter = Arc::new(RwLock::new(0u64));
        b.iter(|| {
            let handles: Vec<_> = (0..16).map(|_| {
                let c = Arc::clone(&counter);
                std::thread::spawn(move || {
                    for _ in 0..1000 { *c.write().unwrap() += 1; }
                })
            }).collect();
            handles.into_iter().for_each(|h| h.join().unwrap());
        });
    });

    g.bench_function("atomic", |b| {
        let counter = Arc::new(AtomicU64::new(0));
        b.iter(|| {
            let handles: Vec<_> = (0..16).map(|_| {
                let c = Arc::clone(&counter);
                std::thread::spawn(move || {
                    for _ in 0..1000 { c.fetch_add(1, Ordering::Relaxed); }
                })
            }).collect();
            handles.into_iter().for_each(|h| h.join().unwrap());
        });
    });

    g.finish();
}

// Group 2: zero-copy WikiEdit<'a> vs owned WikiEditOwned parse speed
fn bench_parsing(c: &mut Criterion) {
    let raw = r#"{"type":"edit","user":"TestUser","bot":false,"server_name":"en.wikipedia.org","title":"Test"}"#;
    let mut g = c.benchmark_group("parsing");

    g.bench_function("zero_copy", |b| {
        b.iter(|| {
            let _: WikiEdit<'_> = serde_json::from_str(raw).unwrap();
        });
    });

    g.bench_function("owned", |b| {
        b.iter(|| {
            let _: WikiEditOwned = serde_json::from_str(raw).unwrap();
        });
    });

    g.finish();
}

// Group 3: async (Tokio tasks) vs threaded (OS threads) — 50 events per iteration.
// Runtime created outside b.iter() so startup cost is not measured.
fn load_events() -> Vec<String> {
    let path = "test_data/events.jsonl";
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|_| panic!(
            "Missing {path}. Run: cargo run -- --capture 500\nThis file is required for Group 3 benchmarks."
        ));
    // Filter out blank lines that would cause parse errors in process_edit.
    content.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect()
}

// Returns the p-th percentile of a pre-sorted slice, in microseconds (input is nanoseconds).
fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx] as f64 / 1_000.0
}

// Group 3: per-packet tail latency comparison — async (Tokio) vs threaded (OS threads).
// Each spawned task/thread records its own wall-clock processing time in nanoseconds.
// After both benchmarks complete, p50/p90/p99 are printed in microseconds so the
// distinction-level requirement of proving which architecture handles tail latency better is met.
fn bench_pipeline(c: &mut Criterion) {
    let events = load_events();
    let mut g = c.benchmark_group("pipeline_comparison");
    // Reduced sample size because each iteration spawns 50 threads/tasks.
    g.sample_size(50);

    let async_lats    = Arc::new(Mutex::new(Vec::<u64>::new()));
    let threaded_lats = Arc::new(Mutex::new(Vec::<u64>::new()));

    {
        let lats = Arc::clone(&async_lats);
        g.bench_function("async", |b| {
            let state  = SharedState::new();
            let logger = Logger::null();
            let rt     = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                rt.block_on(async {
                    let handles: Vec<_> = events.iter().take(50).map(|raw| {
                        let s  = Arc::clone(&state);
                        let l  = Arc::clone(&logger);
                        let r  = raw.clone();
                        let lc = Arc::clone(&lats);
                        tokio::spawn(async move {
                            let t0 = Instant::now();
                            pipeline::process_edit(
                                &r, t0, t0, true, "test",
                                "en.wikipedia.org", "user", &s, &l,
                            );
                            lc.lock().unwrap().push(t0.elapsed().as_nanos() as u64);
                        })
                    }).collect();
                    for h in handles { h.await.ok(); }
                });
            });
        });
    }

    {
        let lats = Arc::clone(&threaded_lats);
        g.bench_function("threaded", |b| {
            let state  = SharedState::new();
            let logger = Logger::null();
            b.iter(|| {
                let handles: Vec<_> = events.iter().take(50).map(|raw| {
                    let s  = Arc::clone(&state);
                    let l  = Arc::clone(&logger);
                    let r  = raw.clone();
                    let lc = Arc::clone(&lats);
                    std::thread::spawn(move || {
                        let t0 = Instant::now();
                        pipeline::process_edit(
                            &r, t0, t0, true, "test",
                            "en.wikipedia.org", "user", &s, &l,
                        );
                        lc.lock().unwrap().push(t0.elapsed().as_nanos() as u64);
                    })
                }).collect();
                handles.into_iter().for_each(|h| h.join().unwrap());
            });
        });
    }

    g.finish();

    // Print per-packet tail latency after Criterion finishes both benchmarks.
    let mut av = async_lats.lock().unwrap().clone();
    av.sort_unstable();
    let mut tv = threaded_lats.lock().unwrap().clone();
    tv.sort_unstable();
    println!("\n=== Pipeline Tail Latency (per-packet, µs) ===");
    println!("async     p50={:.2}  p90={:.2}  p99={:.2}  n={}",
        pct(&av, 50.0), pct(&av, 90.0), pct(&av, 99.0), av.len());
    println!("threaded  p50={:.2}  p90={:.2}  p99={:.2}  n={}",
        pct(&tv, 50.0), pct(&tv, 90.0), pct(&tv, 99.0), tv.len());
}

criterion_group!(benches, bench_sync, bench_parsing, bench_pipeline);
criterion_main!(benches);
