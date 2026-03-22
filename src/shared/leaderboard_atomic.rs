// shared/leaderboard_atomic.rs — Lock-free leaderboard using DashMap (Component D).
// Main path: zero contention. Used as primary leaderboard for the live dashboard.

use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct AtomicLeaderboard {
    counts: Arc<DashMap<String, u64>>,
}

impl AtomicLeaderboard {
    /// Creates an empty lock-free leaderboard.
    pub fn new() -> Self {
        Self { counts: Arc::new(DashMap::new()) }
    }

    /// Atomically increments the edit count for a domain.
    pub fn increment(&self, domain: &str) {
        self.counts
            .entry(domain.to_string())
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    /// Returns the top N domains sorted by edit count.
    pub fn top_n(&self, n: usize) -> Vec<(String, u64)> {
        let mut entries: Vec<(String, u64)> = self.counts
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }
}
