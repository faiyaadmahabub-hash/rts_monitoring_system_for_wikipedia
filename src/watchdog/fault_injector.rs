// watchdog/fault_injector.rs — Automated fault injection (Component E).
// Advanced Feature: Fault Injection — stress-tests pipeline resilience.
// Schedule: Network drop @20s, CPU spike +4ms @40s, Channel flood @55s.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::ingestion::sse_client::SseState;
use crate::metrics::collector::MetricsCollector;
use crate::models::{ScheduledFault, FaultType};

/// Global CPU spike delay (µs) read by the scheduler on each edit.
pub static CPU_SPIKE_DELAY_US: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Async fault injector: executes scheduled faults at their trigger times.
pub async fn run_fault_injector_async(state: Arc<SseState>, metrics: MetricsCollector) {
    let schedule = ScheduledFault::default_schedule();
    let start = Instant::now();

    for fault in &schedule {
        loop {
            if state.should_stop.load(Ordering::Acquire) { return; }
            if start.elapsed().as_secs() >= fault.trigger_at_secs { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        match &fault.fault {
            FaultType::NetworkDrop { duration_secs } => {
                metrics.record_fault_event(format!("INJECT: Network drop ({}s)", duration_secs));
                state.network_blocked.store(true, Ordering::Release);
                tokio::time::sleep(Duration::from_secs(*duration_secs)).await;
                state.network_blocked.store(false, Ordering::Release);
                metrics.record_fault_event("INJECT: Network restored".into());
            }
            FaultType::CpuSpike { delay_ms, duration_secs } => {
                metrics.record_fault_event(format!("INJECT: CPU spike +{}ms ({}s)", delay_ms, duration_secs));
                CPU_SPIKE_DELAY_US.store(delay_ms * 1000, Ordering::Release);
                tokio::time::sleep(Duration::from_secs(*duration_secs)).await;
                CPU_SPIKE_DELAY_US.store(0, Ordering::Release);
                metrics.record_fault_event("INJECT: CPU spike ended".into());
            }
            FaultType::ChannelFlood { duration_secs } => {
                metrics.record_fault_event(format!("INJECT: Channel flood ({}s)", duration_secs));
                tokio::time::sleep(Duration::from_secs(*duration_secs)).await;
                metrics.record_fault_event("INJECT: Channel flood ended".into());
            }
        }
    }
}

/// Threaded fault injector: blocking version of run_fault_injector_async.
pub fn run_fault_injector_threaded(state: Arc<SseState>, metrics: MetricsCollector) {
    let schedule = ScheduledFault::default_schedule();
    let start = Instant::now();

    for fault in &schedule {
        loop {
            if state.should_stop.load(Ordering::Acquire) { return; }
            if start.elapsed().as_secs() >= fault.trigger_at_secs { break; }
            std::thread::sleep(Duration::from_millis(100));
        }

        match &fault.fault {
            FaultType::NetworkDrop { duration_secs } => {
                metrics.record_fault_event(format!("INJECT: Network drop ({}s)", duration_secs));
                state.network_blocked.store(true, Ordering::Release);
                std::thread::sleep(Duration::from_secs(*duration_secs));
                state.network_blocked.store(false, Ordering::Release);
                metrics.record_fault_event("INJECT: Network restored".into());
            }
            FaultType::CpuSpike { delay_ms, duration_secs } => {
                metrics.record_fault_event(format!("INJECT: CPU spike +{}ms ({}s)", delay_ms, duration_secs));
                CPU_SPIKE_DELAY_US.store(delay_ms * 1000, Ordering::Release);
                std::thread::sleep(Duration::from_secs(*duration_secs));
                CPU_SPIKE_DELAY_US.store(0, Ordering::Release);
                metrics.record_fault_event("INJECT: CPU spike ended".into());
            }
            FaultType::ChannelFlood { duration_secs } => {
                metrics.record_fault_event(format!("INJECT: Channel flood ({}s)", duration_secs));
                std::thread::sleep(Duration::from_secs(*duration_secs));
                metrics.record_fault_event("INJECT: Channel flood ended".into());
            }
        }
    }
}

/// Applies CPU spike delay via busy-wait spin loop (called by scheduler per edit).
pub fn apply_cpu_spike_delay() {
    let delay_us = CPU_SPIKE_DELAY_US.load(Ordering::Relaxed);
    if delay_us > 0 {
        let start = Instant::now();
        while start.elapsed().as_micros() < delay_us as u128 {
            std::hint::spin_loop();
        }
    }
}
