//! Timing and deadline tracking for processed edit packets.

use std::time::Instant;

// Three timestamps per packet: arrived_at -> dequeued_at -> processed_at.
// drift_ms = dequeued - arrived (queue wait), exec_ms = processed - dequeued,
// total_ms = processed - arrived (checked against the 2 ms deadline).
#[derive(Clone)]
pub struct PacketRecord {
    pub arrived_at:   Instant,
    pub dequeued_at:  Instant,
    pub processed_at: Instant,
    pub is_human:     bool,
    pub domain:       String,
    pub user:         String,
}

impl PacketRecord {
    pub fn drift_ms(&self) -> f64 {
        self.dequeued_at.duration_since(self.arrived_at).as_secs_f64() * 1000.0
    }

    pub fn exec_ms(&self) -> f64 {
        self.processed_at.duration_since(self.dequeued_at).as_secs_f64() * 1000.0
    }

    pub fn total_ms(&self) -> f64 {
        self.processed_at.duration_since(self.arrived_at).as_secs_f64() * 1000.0
    }

    pub fn deadline_met(&self) -> bool {
        self.total_ms() < 2.0
    }
}

pub fn percentile(mut data: Vec<f64>, p: f64) -> f64 {
    if data.is_empty() { return 0.0; }
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (data.len() - 1) as f64).round() as usize;
    data[idx]
}

pub fn print_stats(records: &[PacketRecord]) {
    let human_drift: Vec<f64> = records.iter()
        .filter(|r| r.is_human).map(|r| r.drift_ms()).collect();
    let bot_drift: Vec<f64> = records.iter()
        .filter(|r| !r.is_human).map(|r| r.drift_ms()).collect();

    let h_miss  = records.iter().filter(|r| r.is_human  && !r.deadline_met()).count();
    let h_total = records.iter().filter(|r| r.is_human).count();
    let b_miss  = records.iter().filter(|r| !r.is_human && !r.deadline_met()).count();
    let b_total = records.iter().filter(|r| !r.is_human).count();

    println!();
    println!("human  p50={:.2}ms  p90={:.2}ms  p99={:.2}ms  miss={}/{}",
        percentile(human_drift.clone(), 50.0),
        percentile(human_drift.clone(), 90.0),
        percentile(human_drift.clone(), 99.0),
        h_miss, h_total);
    println!("bot    p50={:.2}ms  p90={:.2}ms  p99={:.2}ms  miss={}/{}",
        percentile(bot_drift.clone(), 50.0),
        percentile(bot_drift.clone(), 90.0),
        percentile(bot_drift.clone(), 99.0),
        b_miss, b_total);
}
