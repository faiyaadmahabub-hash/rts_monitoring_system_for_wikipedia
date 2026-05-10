//! Shared system state, constants, and mode transitions.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
use crate::metrics::PacketRecord;

pub const QUEUE_CAPACITY:      usize = 100;
pub const DEGRADE_THRESHOLD:   f64   = 0.50;
pub const RECOVERY_THRESHOLD:  f64   = 0.20;
pub const WATCHDOG_TIMEOUT_S:  u64   = 10;
pub const ROLLING_WINDOW_SIZE: usize = 20;

pub struct QueuedEdit {
    pub raw:         String,
    pub arrived_at:  std::time::Instant,
    pub is_human:    bool,
    pub title:       String,
    pub server_name: String,
    pub user:        String,
}

// Mutex for structures needing multi-field atomicity; AtomicU64 for simple counters.
pub struct SharedState {
    pub queue:            Mutex<VecDeque<QueuedEdit>>,
    pub leaderboard:      Mutex<HashMap<String, u64>>,
    pub last_doc_editor:  Mutex<HashMap<String, bool>>,
    pub records:          Mutex<Vec<PacketRecord>>,
    pub recent_results:   Mutex<VecDeque<bool>>,
    pub degraded:         AtomicBool,
    pub reconnect_needed: AtomicBool,
    pub last_data_ns:     AtomicU64,
    pub total_processed:  AtomicU64,
    pub total_missed:     AtomicU64,
    pub overflow_count:   AtomicU64,
    pub override_count:   AtomicU64,
    pub reset_count:      AtomicU64,
}

impl SharedState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queue:            Mutex::new(VecDeque::with_capacity(QUEUE_CAPACITY)),
            leaderboard:      Mutex::new(HashMap::new()),
            last_doc_editor:  Mutex::new(HashMap::new()),
            records:          Mutex::new(Vec::new()),
            recent_results:   Mutex::new(VecDeque::with_capacity(ROLLING_WINDOW_SIZE + 1)),
            degraded:         AtomicBool::new(false),
            reconnect_needed: AtomicBool::new(false),
            last_data_ns:     AtomicU64::new(0),
            total_processed:  AtomicU64::new(0),
            total_missed:     AtomicU64::new(0),
            overflow_count:   AtomicU64::new(0),
            override_count:   AtomicU64::new(0),
            reset_count:      AtomicU64::new(0),
        })
    }

    pub fn update_mode(&self, logger: &Arc<crate::logger::Logger>) {
        let window = self.recent_results.lock().unwrap();
        if window.len() < ROLLING_WINDOW_SIZE { return; }
        let miss_count = window.iter().filter(|&&met| !met).count();
        let miss_rate  = miss_count as f64 / window.len() as f64;
        let currently  = self.degraded.load(std::sync::atomic::Ordering::Relaxed);

        if !currently && miss_rate > DEGRADE_THRESHOLD {
            self.degraded.store(true, std::sync::atomic::Ordering::Relaxed);
            logger.log(&format!(
                "[DEGRADED]  miss_rate={:.0}% > {:.0}% | Mode: DEGRADED (bot edits skipped)",
                miss_rate * 100.0, DEGRADE_THRESHOLD * 100.0
            ));
        } else if currently && miss_rate < RECOVERY_THRESHOLD {
            self.degraded.store(false, std::sync::atomic::Ordering::Relaxed);
            logger.log(&format!(
                "[RECOVER]   miss_rate={:.0}% < {:.0}% | Mode: NORMAL",
                miss_rate * 100.0, RECOVERY_THRESHOLD * 100.0
            ));
        }
    }

    pub fn push_result(&self, deadline_met: bool) {
        let mut window = self.recent_results.lock().unwrap();
        window.push_back(deadline_met);
        if window.len() > ROLLING_WINDOW_SIZE {
            window.pop_front();
        }
    }
}
