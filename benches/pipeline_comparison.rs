// benches/pipeline_comparison.rs — Criterion benchmarks using real Wikipedia data.
// Advanced Feature: Comparative Analysis — quantitative proof via Criterion.rs.
// Requires: cargo run --release -- --mode async --duration 30 (to capture events first)

use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::VecDeque;

use wiki_rts::parsing::zero_copy;
use wiki_rts::models::EditTier;

/// Loads captured SSE events from data/captured_events.jsonl for benchmark use.
fn load_captured_events() -> Vec<String> {
    match std::fs::read_to_string("data/captured_events.jsonl") {
        Ok(content) => content.lines().filter(|l| !l.is_empty()).map(|l| l.to_string()).collect(),
        Err(_) => {
            eprintln!("No captured events. Run: cargo run --release -- --mode async --duration 30");
            vec![]
        }
    }
}

/// Benchmarks zero-copy parsing throughput on real Wikipedia events.
fn benchmark_zero_copy_parse(c: &mut Criterion) {
    let events = load_captured_events();
    if events.is_empty() { return; }

    let mut group = c.benchmark_group("zero_copy_parse");

    group.bench_function("real_wikipedia_events", |b| {
        b.iter(|| {
            for event in &events {
                let _ = zero_copy::zero_copy_parse(event);
            }
        });
    });

    let mid = &events[events.len() / 2];
    group.bench_function("single_real_event", |b| {
        b.iter(|| zero_copy::zero_copy_parse(mid));
    });

    group.finish();
}

/// Benchmarks lightweight tier classification on real events.
fn benchmark_quick_classify(c: &mut Criterion) {
    let events = load_captured_events();
    if events.is_empty() { return; }

    let mut group = c.benchmark_group("quick_classify");

    group.bench_function("real_wikipedia_events", |b| {
        b.iter(|| {
            for event in &events {
                let _ = zero_copy::quick_classify(event);
            }
        });
    });

    group.finish();
}

/// Benchmarks priority queue tier selection with real-world tier distribution.
fn benchmark_priority_queue(c: &mut Criterion) {
    let events = load_captured_events();
    if events.is_empty() { return; }

    let mut group = c.benchmark_group("priority_scheduling");

    let mut tier_counts: [usize; 5] = [0; 5];
    for event in &events {
        if let Some((tier, _)) = zero_copy::quick_classify(event) {
            tier_counts[tier as usize - 1] += 1;
        }
    }

    group.bench_function("tier_selection_real_distribution", |b| {
        let mut queues: [VecDeque<u64>; 5] = Default::default();
        for (i, &count) in tier_counts.iter().enumerate() {
            for j in 0..count {
                queues[i].push_back(j as u64);
            }
        }

        b.iter(|| {
            let _ = queues[0].front()
                .or_else(|| queues[1].front())
                .or_else(|| queues[2].front())
                .or_else(|| queues[3].front())
                .or_else(|| queues[4].front());
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_zero_copy_parse, benchmark_quick_classify, benchmark_priority_queue);
criterion_main!(benches);
