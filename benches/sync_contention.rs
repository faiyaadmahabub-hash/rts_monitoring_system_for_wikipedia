// benches/sync_contention.rs — Sync primitive contention benchmark (Component D).
// Compares Mutex vs RwLock vs Atomic (DashMap) with 1/2/4/8 threads.
// Advanced Feature: Synchronization Benchmark via Criterion.rs.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;

use wiki_rts::shared::leaderboard_atomic::AtomicLeaderboard;
use wiki_rts::shared::leaderboard_mutex::MutexLeaderboard;
use wiki_rts::shared::leaderboard_rwlock::RwLockLeaderboard;
use wiki_rts::metrics::collector::MetricsCollector;

/// Benchmarks all three sync strategies under varying thread counts.
fn benchmark_sync_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention");
    let domains = vec![
        "en.wikipedia.org",
        "www.wikidata.org",
        "commons.wikimedia.org",
        "de.wikipedia.org",
        "fr.wikipedia.org",
    ];

    for thread_count in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("MutexLeaderboard", thread_count),
            &thread_count,
            |b, &tc| {
                let lb = MutexLeaderboard::new();
                let metrics = MetricsCollector::new(100);
                b.iter(|| {
                    let handles: Vec<_> = (0..tc).map(|i| {
                        let lb = lb.clone();
                        let m = metrics.clone();
                        let domain = domains[i % domains.len()].to_string();
                        let is_hp = i % 2 == 0;
                        std::thread::spawn(move || {
                            for _ in 0..100 {
                                lb.increment(&domain, is_hp, &m);
                            }
                        })
                    }).collect();
                    for h in handles { h.join().unwrap(); }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("RwLockLeaderboard", thread_count),
            &thread_count,
            |b, &tc| {
                let lb = RwLockLeaderboard::new();
                let metrics = MetricsCollector::new(100);
                b.iter(|| {
                    let handles: Vec<_> = (0..tc).map(|i| {
                        let lb = lb.clone();
                        let m = metrics.clone();
                        let domain = domains[i % domains.len()].to_string();
                        let is_hp = i % 2 == 0;
                        std::thread::spawn(move || {
                            for _ in 0..100 {
                                lb.increment(&domain, is_hp, &m);
                            }
                        })
                    }).collect();
                    for h in handles { h.join().unwrap(); }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("AtomicLeaderboard", thread_count),
            &thread_count,
            |b, &tc| {
                let lb = Arc::new(AtomicLeaderboard::new());
                b.iter(|| {
                    let handles: Vec<_> = (0..tc).map(|i| {
                        let lb = lb.clone();
                        let domain = domains[i % domains.len()].to_string();
                        std::thread::spawn(move || {
                            for _ in 0..100 {
                                lb.increment(&domain);
                            }
                        })
                    }).collect();
                    for h in handles { h.join().unwrap(); }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_sync_strategies);
criterion_main!(benches);
