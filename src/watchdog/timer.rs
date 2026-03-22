// watchdog/timer.rs — Network watchdog (Component E).
// Triggers SSE reconnect if no data received for 10 seconds (supervisor pattern).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::ingestion::sse_client::SseState;
use crate::metrics::collector::MetricsCollector;

const WATCHDOG_TIMEOUT_MS: u64 = 10_000;
const CHECK_INTERVAL_MS: u64 = 500;

/// Async watchdog: polls SSE silence period, triggers reconnect if timeout exceeded.
pub async fn run_watchdog_async(state: Arc<SseState>, metrics: MetricsCollector) {
    loop {
        if state.should_stop.load(Ordering::Acquire) { return; }
        tokio::time::sleep(Duration::from_millis(CHECK_INTERVAL_MS)).await;

        let silence_ms = state.millis_since_last_event();
        if silence_ms > WATCHDOG_TIMEOUT_MS {
            metrics.record_fault_event(format!(
                "WATCHDOG: No data for {:.1}s — triggering reconnect",
                silence_ms as f64 / 1000.0
            ));
            metrics.record_watchdog_trigger();
            state.should_reconnect.store(true, Ordering::Release);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// Threaded watchdog: blocking version of run_watchdog_async for std::thread pipeline.
pub fn run_watchdog_threaded(state: Arc<SseState>, metrics: MetricsCollector) {
    loop {
        if state.should_stop.load(Ordering::Acquire) { return; }
        std::thread::sleep(Duration::from_millis(CHECK_INTERVAL_MS));

        let silence_ms = state.millis_since_last_event();
        if silence_ms > WATCHDOG_TIMEOUT_MS {
            metrics.record_fault_event(format!(
                "WATCHDOG: No data for {:.1}s — triggering reconnect",
                silence_ms as f64 / 1000.0
            ));
            metrics.record_watchdog_trigger();
            state.should_reconnect.store(true, Ordering::Release);
            std::thread::sleep(Duration::from_secs(2));
        }
    }
}
