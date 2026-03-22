// metrics/collector.rs — Central metrics collector (Component D). Thread-safe via Mutex.
// Aggregates per-edit timing, sync contention, overflow, shed counts, and mode transitions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::models::{EditMetrics, EditTier, SystemMode};
use crate::metrics::percentile::Percentiles;

#[derive(Clone)]
pub struct MetricsCollector {
    inner: Arc<Mutex<MetricsInner>>,
}

struct MetricsInner {
    edits: Vec<EditMetrics>,
    overflow_count: u64,
    overflow_timestamps: Vec<Instant>,
    shed_count: u64,
    shed_by_tier: HashMap<EditTier, u64>,
    current_mode: SystemMode,
    mode_changes: Vec<(Instant, SystemMode)>,
    fault_events: Vec<(Instant, String)>,
    hot_path_allocs: u64,
    start_time: Instant,
    watchdog_triggers: u64,
    mutex_contention: u64,
    mutex_inversions: u64,
    mutex_total_wait_us: u64,
    rwlock_contention: u64,
    rwlock_inversions: u64,
    rwlock_total_wait_us: u64,
    events_per_sec: f64,
    channel_fill: usize,
    channel_capacity: usize,
}

impl MetricsCollector {
    /// Creates a new collector with pre-allocated storage.
    pub fn new(channel_capacity: usize) -> Self {
        let start = Instant::now();
        Self {
            inner: Arc::new(Mutex::new(MetricsInner {
                edits: Vec::with_capacity(4096),
                overflow_count: 0,
                overflow_timestamps: Vec::new(),
                shed_count: 0,
                shed_by_tier: HashMap::new(),
                current_mode: SystemMode::Normal,
                mode_changes: vec![(start, SystemMode::Normal)],
                fault_events: Vec::new(),
                hot_path_allocs: 0,
                start_time: start,
                watchdog_triggers: 0,
                mutex_contention: 0,
                mutex_inversions: 0,
                mutex_total_wait_us: 0,
                rwlock_contention: 0,
                rwlock_inversions: 0,
                rwlock_total_wait_us: 0,
                events_per_sec: 0.0,
                channel_fill: 0,
                channel_capacity,
            })),
        }
    }

    /// Records a fully processed edit with all timing metadata.
    pub fn record_edit(&self, metrics: EditMetrics) {
        let mut inner = self.inner.lock().unwrap();
        inner.edits.push(metrics);
    }

    /// Records a channel overflow (oldest packet dropped).
    pub fn record_overflow(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.overflow_count += 1;
        inner.overflow_timestamps.push(Instant::now());
    }

    /// Records a shed event (edit discarded due to degradation mode).
    pub fn record_shed(&self, tier: EditTier) {
        let mut inner = self.inner.lock().unwrap();
        inner.shed_count += 1;
        *inner.shed_by_tier.entry(tier).or_insert(0) += 1;
    }

    /// Transitions system mode (e.g., Normal → Degraded-1).
    pub fn set_mode(&self, mode: SystemMode) {
        let mut inner = self.inner.lock().unwrap();
        if inner.current_mode != mode {
            inner.mode_changes.push((Instant::now(), mode));
            inner.current_mode = mode;
        }
    }

    /// Returns the current degradation mode.
    pub fn current_mode(&self) -> SystemMode {
        self.inner.lock().unwrap().current_mode
    }

    /// Logs a fault event description with timestamp.
    pub fn record_fault_event(&self, description: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.fault_events.push((Instant::now(), description));
    }

    /// Increments the watchdog trigger counter.
    pub fn record_watchdog_trigger(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.watchdog_triggers += 1;
    }

    /// Records hot-path heap allocations detected by the custom allocator.
    pub fn record_hot_path_allocs(&self, count: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.hot_path_allocs += count;
    }

    /// Records a Mutex lock contention event with wait time and inversion flag.
    pub fn record_mutex_contention(&self, wait_us: u64, inversion: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.mutex_contention += 1;
        inner.mutex_total_wait_us += wait_us;
        if inversion {
            inner.mutex_inversions += 1;
        }
    }

    /// Records an RwLock contention event with wait time and inversion flag.
    pub fn record_rwlock_contention(&self, wait_us: u64, inversion: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.rwlock_contention += 1;
        inner.rwlock_total_wait_us += wait_us;
        if inversion {
            inner.rwlock_inversions += 1;
        }
    }

    /// Updates current channel fill level (non-blocking try_lock).
    pub fn update_channel_fill(&self, fill: usize) {
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.channel_fill = fill;
        }
    }

    /// Updates the events-per-second throughput counter.
    pub fn update_events_per_sec(&self, eps: f64) {
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.events_per_sec = eps;
        }
    }

    /// Sliding window p99 (last 100 edits) — used by fail-safe controller.
    pub fn recent_p99_latency_us(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        let window_size = 100.min(inner.edits.len());
        if window_size == 0 {
            return 0;
        }
        let start = inner.edits.len() - window_size;
        let values: Vec<u64> = inner.edits[start..]
            .iter()
            .map(|e| e.latency_us())
            .collect();
        Percentiles::from_values(&values).p99
    }

    /// Computes latency percentiles across all processed edits.
    pub fn latency_percentiles(&self) -> Percentiles {
        let inner = self.inner.lock().unwrap();
        let values: Vec<u64> = inner.edits.iter().map(|e| e.latency_us()).collect();
        Percentiles::from_values(&values)
    }

    /// Computes scheduling drift percentiles grouped by tier.
    pub fn drift_percentiles_by_tier(&self) -> HashMap<EditTier, Percentiles> {
        let inner = self.inner.lock().unwrap();
        let mut by_tier: HashMap<EditTier, Vec<u64>> = HashMap::new();
        for edit in &inner.edits {
            by_tier.entry(edit.tier).or_default().push(edit.drift_us());
        }
        by_tier
            .into_iter()
            .map(|(tier, values)| (tier, Percentiles::from_values(&values)))
            .collect()
    }

    /// Computes deadline compliance (met/total) grouped by tier.
    pub fn deadline_compliance_by_tier(&self) -> HashMap<EditTier, (u64, u64)> {
        let inner = self.inner.lock().unwrap();
        let mut by_tier: HashMap<EditTier, (u64, u64)> = HashMap::new();
        for edit in &inner.edits {
            let entry = by_tier.entry(edit.tier).or_insert((0, 0));
            entry.1 += 1;
            if edit.deadline_met {
                entry.0 += 1;
            }
        }
        by_tier
    }

    /// Takes a point-in-time snapshot of all metrics for the live dashboard.
    pub fn snapshot(&self) -> DashboardSnapshot {
        let inner = self.inner.lock().unwrap();
        let total = inner.edits.len();
        let human_count = inner.edits.iter().filter(|e| !e.tier.is_bot()).count();
        let bot_count = total - human_count;

        let latency_values: Vec<u64> = inner.edits.iter().map(|e| e.latency_us()).collect();
        let latency = Percentiles::from_values(&latency_values);

        let mut drift_by_tier: HashMap<EditTier, Vec<u64>> = HashMap::new();
        let mut deadline_by_tier: HashMap<EditTier, (u64, u64)> = HashMap::new();
        for edit in &inner.edits {
            drift_by_tier.entry(edit.tier).or_default().push(edit.drift_us());
            let entry = deadline_by_tier.entry(edit.tier).or_insert((0, 0));
            entry.1 += 1;
            if edit.deadline_met {
                entry.0 += 1;
            }
        }
        let drift_percentiles: HashMap<EditTier, Percentiles> = drift_by_tier
            .into_iter()
            .map(|(t, v)| (t, Percentiles::from_values(&v)))
            .collect();

        let mut domains: HashMap<String, u64> = HashMap::new();
        for edit in &inner.edits {
            *domains.entry(edit.server_name.clone()).or_insert(0) += 1;
        }
        let mut domain_list: Vec<(String, u64)> = domains.into_iter().collect();
        domain_list.sort_by(|a, b| b.1.cmp(&a.1));
        domain_list.truncate(5);

        let uptime = inner.start_time.elapsed().as_secs();

        DashboardSnapshot {
            uptime_secs: uptime,
            mode: inner.current_mode,
            total_processed: total,
            human_count,
            bot_count,
            overflow_count: inner.overflow_count,
            shed_count: inner.shed_count,
            events_per_sec: inner.events_per_sec,
            channel_fill: inner.channel_fill,
            channel_capacity: inner.channel_capacity,
            latency,
            drift_percentiles,
            deadline_by_tier,
            top_domains: domain_list,
            mutex_contention: inner.mutex_contention,
            mutex_inversions: inner.mutex_inversions,
            mutex_avg_wait_us: if inner.mutex_contention > 0 {
                inner.mutex_total_wait_us / inner.mutex_contention
            } else {
                0
            },
            rwlock_contention: inner.rwlock_contention,
            rwlock_inversions: inner.rwlock_inversions,
            rwlock_avg_wait_us: if inner.rwlock_contention > 0 {
                inner.rwlock_total_wait_us / inner.rwlock_contention
            } else {
                0
            },
            fault_events: inner.fault_events.iter().map(|(_, s)| s.clone()).collect(),
            watchdog_triggers: inner.watchdog_triggers,
            hot_path_allocs: inner.hot_path_allocs,
        }
    }

    /// Exports metrics as ReportData for JSON serialization.
    pub fn export_report_data(&self) -> ReportData {
        let snap = self.snapshot();

        ReportData {
            mode: snap.mode,
            uptime_secs: snap.uptime_secs,
            total_processed: snap.total_processed,
            human_count: snap.human_count,
            bot_count: snap.bot_count,
            overflow_count: snap.overflow_count,
            shed_count: snap.shed_count,
            latency: snap.latency,
            drift_percentiles: snap.drift_percentiles,
            deadline_by_tier: snap.deadline_by_tier,
            top_domains: snap.top_domains,
            mutex_contention: snap.mutex_contention,
            mutex_inversions: snap.mutex_inversions,
            mutex_avg_wait_us: snap.mutex_avg_wait_us,
            rwlock_contention: snap.rwlock_contention,
            rwlock_inversions: snap.rwlock_inversions,
            rwlock_avg_wait_us: snap.rwlock_avg_wait_us,
            watchdog_triggers: snap.watchdog_triggers,
            hot_path_allocs: snap.hot_path_allocs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DashboardSnapshot {
    pub uptime_secs: u64,
    pub mode: SystemMode,
    pub total_processed: usize,
    pub human_count: usize,
    pub bot_count: usize,
    pub overflow_count: u64,
    pub shed_count: u64,
    pub events_per_sec: f64,
    pub channel_fill: usize,
    pub channel_capacity: usize,
    pub latency: Percentiles,
    pub drift_percentiles: HashMap<EditTier, Percentiles>,
    pub deadline_by_tier: HashMap<EditTier, (u64, u64)>,
    pub top_domains: Vec<(String, u64)>,
    pub mutex_contention: u64,
    pub mutex_inversions: u64,
    pub mutex_avg_wait_us: u64,
    pub rwlock_contention: u64,
    pub rwlock_inversions: u64,
    pub rwlock_avg_wait_us: u64,
    pub fault_events: Vec<String>,
    pub watchdog_triggers: u64,
    pub hot_path_allocs: u64,
}

#[derive(Debug, Clone)]
pub struct ReportData {
    pub mode: SystemMode,
    pub uptime_secs: u64,
    pub total_processed: usize,
    pub human_count: usize,
    pub bot_count: usize,
    pub overflow_count: u64,
    pub shed_count: u64,
    pub latency: Percentiles,
    pub drift_percentiles: HashMap<EditTier, Percentiles>,
    pub deadline_by_tier: HashMap<EditTier, (u64, u64)>,
    pub top_domains: Vec<(String, u64)>,
    pub mutex_contention: u64,
    pub mutex_inversions: u64,
    pub mutex_avg_wait_us: u64,
    pub rwlock_contention: u64,
    pub rwlock_inversions: u64,
    pub rwlock_avg_wait_us: u64,
    pub watchdog_triggers: u64,
    pub hot_path_allocs: u64,
}
