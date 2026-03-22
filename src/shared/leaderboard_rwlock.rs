// shared/leaderboard_rwlock.rs — RwLock-based leaderboard shadow (Component D).
// Write-lock for increments, read-lock for reads. Measures contention for sync comparison.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::metrics::collector::MetricsCollector;

#[derive(Clone)]
pub struct RwLockLeaderboard {
    counts: Arc<RwLock<HashMap<String, u64>>>,
}

impl RwLockLeaderboard {
    /// Creates an empty RwLock-protected leaderboard.
    pub fn new() -> Self {
        Self { counts: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Write-locks to increment domain count. Measures lock wait and detects priority inversion.
    pub fn increment(&self, domain: &str, is_high_priority: bool, metrics: &MetricsCollector) {
        let start = Instant::now();
        let mut guard = self.counts.write().unwrap();
        let wait_us = start.elapsed().as_micros() as u64;

        *guard.entry(domain.to_string()).or_insert(0) += 1;
        drop(guard);

        if wait_us > 0 {
            let inversion = is_high_priority && wait_us > 10;
            metrics.record_rwlock_contention(wait_us, inversion);
        }
    }

    /// Read-locks to return top N domains sorted by edit count.
    pub fn top_n(&self, n: usize) -> Vec<(String, u64)> {
        let guard = self.counts.read().unwrap();
        let mut entries: Vec<(String, u64)> = guard
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }
}
