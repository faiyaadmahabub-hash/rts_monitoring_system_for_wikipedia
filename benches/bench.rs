// Criterion benchmarks: sync_contention, parsing, pipeline_comparison

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
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

fn bench_pipeline(c: &mut Criterion) {
    let events = load_events();
    let mut g = c.benchmark_group("pipeline_comparison");
    // Reduced sample size because each iteration spawns 50 threads/tasks.
    g.sample_size(50);

    g.bench_function("async", |b| {
        let state  = SharedState::new();
        let logger = Logger::null();
        let rt     = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            rt.block_on(async {
                let handles: Vec<_> = events.iter().take(50).map(|raw| {
                    let s = Arc::clone(&state);
                    let l = Arc::clone(&logger);
                    let r = raw.clone();
                    tokio::spawn(async move {
                        let now = Instant::now();
                        pipeline::process_edit(
                            &r, now, now, true, "test",
                            "en.wikipedia.org", "user", &s, &l,
                        );
                    })
                }).collect();
                for h in handles { h.await.ok(); }
            });
        });
    });

    g.bench_function("threaded", |b| {
        let state  = SharedState::new();
        let logger = Logger::null();
        b.iter(|| {
            let handles: Vec<_> = events.iter().take(50).map(|raw| {
                let s = Arc::clone(&state);
                let l = Arc::clone(&logger);
                let r = raw.clone();
                std::thread::spawn(move || {
                    let now = Instant::now();
                    pipeline::process_edit(
                        &r, now, now, true, "test",
                        "en.wikipedia.org", "user", &s, &l,
                    );
                })
            }).collect();
            handles.into_iter().for_each(|h| h.join().unwrap());
        });
    });

    g.finish();
}

criterion_group!(benches, bench_sync, bench_parsing, bench_pipeline);
criterion_main!(benches);
