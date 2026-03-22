// watchdog/failsafe.rs — Fail-safe degradation controller (Component E).
// Advanced Feature: Safety Interlocks — auto-triggered by timing violations, recovers when stable.
// Monitors p99 latency. Escalates through 4 degradation stages when p99
// exceeds threshold. Recovers after sustained stability below recovery threshold.
// Hysteresis gap (3ms degrade / 2ms recover) prevents mode flapping.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::ingestion::sse_client::SseState;
use crate::metrics::collector::MetricsCollector;
use crate::models::{AppConfig, SystemMode};

const CHECK_INTERVAL_MS: u64 = 200;

/// Async failsafe: monitors p99 latency, escalates/recovers degradation mode.
/// Skips first 10s warmup to avoid cold-start false triggers.
/// Enforces 1s cooldown between escalations to let shedding take effect.
pub async fn run_failsafe_async(
    state: Arc<SseState>,
    metrics: MetricsCollector,
    config: AppConfig,
) {
    let mut stable_since: Option<Instant> = None;
    let mut last_escalation: Option<Instant> = None;

    tokio::time::sleep(Duration::from_secs(10)).await;

    loop {
        if state.should_stop.load(Ordering::Acquire) { return; }
        tokio::time::sleep(Duration::from_millis(CHECK_INTERVAL_MS)).await;

        let p99_us = metrics.recent_p99_latency_us();
        let current_mode = metrics.current_mode();

        if p99_us > config.degrade_threshold_us {
            stable_since = None;
            let can_escalate = last_escalation
                .map(|t| t.elapsed().as_secs() >= 1)
                .unwrap_or(true);

            if can_escalate {
                let new_mode = current_mode.escalate();
                if new_mode != current_mode {
                    metrics.set_mode(new_mode);
                    last_escalation = Some(Instant::now());
                    metrics.record_fault_event(format!(
                        "FAILSAFE: p99 {:.2}ms > {:.2}ms → {}",
                        p99_us as f64 / 1000.0,
                        config.degrade_threshold_us as f64 / 1000.0,
                        new_mode,
                    ));
                }
            }
        } else if p99_us < config.recover_threshold_us && current_mode != SystemMode::Normal {
            match stable_since {
                None => { stable_since = Some(Instant::now()); }
                Some(since) => {
                    if since.elapsed().as_secs() >= config.stability_window_secs {
                        let new_mode = current_mode.deescalate();
                        metrics.set_mode(new_mode);
                        metrics.record_fault_event(format!(
                            "RECOVER: p99 {:.2}ms < {:.2}ms stable {}s → {}",
                            p99_us as f64 / 1000.0,
                            config.recover_threshold_us as f64 / 1000.0,
                            config.stability_window_secs, new_mode,
                        ));
                        stable_since = None;
                    }
                }
            }
        } else {
            stable_since = None;
        }
    }
}

/// Threaded failsafe: blocking version of run_failsafe_async for std::thread pipeline.
pub fn run_failsafe_threaded(
    state: Arc<SseState>,
    metrics: MetricsCollector,
    config: AppConfig,
) {
    let mut stable_since: Option<Instant> = None;
    let mut last_escalation: Option<Instant> = None;

    std::thread::sleep(Duration::from_secs(10));

    loop {
        if state.should_stop.load(Ordering::Acquire) { return; }
        std::thread::sleep(Duration::from_millis(CHECK_INTERVAL_MS));

        let p99_us = metrics.recent_p99_latency_us();
        let current_mode = metrics.current_mode();

        if p99_us > config.degrade_threshold_us {
            stable_since = None;
            let can_escalate = last_escalation
                .map(|t| t.elapsed().as_secs() >= 1)
                .unwrap_or(true);

            if can_escalate {
                let new_mode = current_mode.escalate();
                if new_mode != current_mode {
                    metrics.set_mode(new_mode);
                    last_escalation = Some(Instant::now());
                    metrics.record_fault_event(format!(
                        "FAILSAFE: p99 {:.2}ms > {:.2}ms → {}",
                        p99_us as f64 / 1000.0,
                        config.degrade_threshold_us as f64 / 1000.0,
                        new_mode,
                    ));
                }
            }
        } else if p99_us < config.recover_threshold_us && current_mode != SystemMode::Normal {
            match stable_since {
                None => { stable_since = Some(Instant::now()); }
                Some(since) => {
                    if since.elapsed().as_secs() >= config.stability_window_secs {
                        let new_mode = current_mode.deescalate();
                        metrics.set_mode(new_mode);
                        metrics.record_fault_event(format!(
                            "RECOVER: p99 {:.2}ms < {:.2}ms stable {}s → {}",
                            p99_us as f64 / 1000.0,
                            config.recover_threshold_us as f64 / 1000.0,
                            config.stability_window_secs, new_mode,
                        ));
                        stable_since = None;
                    }
                }
            }
        } else {
            stable_since = None;
        }
    }
}
