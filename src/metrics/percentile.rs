// metrics/percentile.rs — p50, p90, p99 percentile calculator.
// Advanced Feature: Statistical Rigor — analysis uses percentiles, not averages.

/// Computes the value at the given percentile from a sorted array.
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    if sorted.len() == 1 { return sorted[0]; }
    let index = ((sorted.len() as f64 - 1.0) * p).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Percentiles {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub max: u64,
    pub count: usize,
}

impl Percentiles {
    /// Computes p50, p90, p99, and max from an unsorted slice of microsecond values.
    pub fn from_values(values: &[u64]) -> Self {
        if values.is_empty() { return Self::default(); }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        Self {
            p50: percentile(&sorted, 0.50),
            p90: percentile(&sorted, 0.90),
            p99: percentile(&sorted, 0.99),
            max: *sorted.last().unwrap_or(&0),
            count: sorted.len(),
        }
    }

    /// Formats p50 as milliseconds string.
    pub fn p50_ms(&self) -> String { format!("{:.2}ms", self.p50 as f64 / 1000.0) }
    /// Formats p90 as milliseconds string.
    pub fn p90_ms(&self) -> String { format!("{:.2}ms", self.p90 as f64 / 1000.0) }
    /// Formats p99 as milliseconds string.
    pub fn p99_ms(&self) -> String { format!("{:.2}ms", self.p99 as f64 / 1000.0) }
    /// Formats max as milliseconds string.
    pub fn max_ms(&self) -> String { format!("{:.2}ms", self.max as f64 / 1000.0) }
}
