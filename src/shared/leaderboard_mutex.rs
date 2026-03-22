// shared/leaderboard_mutex.rs — Mutex-based leaderboard shadow (Component D).
// Measures lock wait time and detects priority inversions for sync benchmarking.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::metrics::collector::MetricsCollector;

#[derive(Clone)]
pub struct MutexLeaderboard {
    counts: Arc<Mutex<HashMap<String, u64>>>,
}

impl MutexLeaderboard {
    /// Creates an empty Mutex-protected leaderboard.
    pub fn new() -> Self {
        Self { counts: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Increments domain count. Measures lock acquisition time and detects priority inversion.
    pub fn increment(&self, domain: &str, is_high_priority: bool, metrics: &MetricsCollector) {
        let start = Instant::now();
        let mut guard = self.counts.lock().unwrap();
        let wait_us = start.elapsed().as_micros() as u64;

        *guard.entry(domain.to_string()).or_insert(0) += 1;
        drop(guard);

        if wait_us > 0 {
            let inversion = is_high_priority && wait_us > 10;
            metrics.record_mutex_contention(wait_us, inversion);
        }
    }

    /// Returns top N domains. Also measures lock contention for read operations.
    pub fn top_n(&self, n: usize, is_high_priority: bool, metrics: &MetricsCollector) -> Vec<(String, u64)> {
        let start = Instant::now();
        let guard = self.counts.lock().unwrap();
        let wait_us = start.elapsed().as_micros() as u64;

        let mut entries: Vec<(String, u64)> = guard
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        drop(guard);

        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);

        if wait_us > 0 {
            let inversion = is_high_priority && wait_us > 10;
            metrics.record_mutex_contention(wait_us, inversion);
        }

        entries
    }
}
