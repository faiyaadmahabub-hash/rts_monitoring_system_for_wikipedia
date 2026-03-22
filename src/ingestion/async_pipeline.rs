// ingestion/async_pipeline.rs — Async pipeline using Tokio (Component A).
// Spawns concurrent tasks: SSE ingestion, scheduler, watchdog, failsafe, fault injector.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::ingestion::overflow_channel::OverflowChannel;
use crate::ingestion::sse_client::{self, SseState};
use crate::metrics::collector::MetricsCollector;
use crate::models::AppConfig;
use crate::scheduling::scheduler;
use crate::shared::leaderboard_atomic::AtomicLeaderboard;
use crate::shared::leaderboard_mutex::MutexLeaderboard;
use crate::shared::leaderboard_rwlock::RwLockLeaderboard;
use crate::watchdog::{timer, failsafe, fault_injector};

/// Orchestrates the full async pipeline: spawns all subsystem tasks, waits for duration, then shuts down.
pub async fn run(config: AppConfig, metrics: MetricsCollector, sse_state: Arc<SseState>) {
    let channel = OverflowChannel::new(config.channel_capacity, metrics.clone());
    let sender = channel.sender();
    let receiver = channel.receiver();

    let atomic_lb = AtomicLeaderboard::new();
    let mutex_lb = MutexLeaderboard::new();
    let rwlock_lb = RwLockLeaderboard::new();

    let s = sse_state.clone();
    let h1 = tokio::spawn(async move { sse_client::connect_sse_async(s, sender).await; });

    let m = metrics.clone(); let s = sse_state.clone();
    let h2 = tokio::spawn(async move {
        scheduler::run_async_scheduler(receiver, m, s, atomic_lb, mutex_lb, rwlock_lb).await;
    });

    let s = sse_state.clone(); let m = metrics.clone();
    let h3 = tokio::spawn(async move { timer::run_watchdog_async(s, m).await; });

    let s = sse_state.clone(); let m = metrics.clone(); let c = config.clone();
    let h4 = tokio::spawn(async move { failsafe::run_failsafe_async(s, m, c).await; });

    let h5 = if config.faults_enabled {
        let s = sse_state.clone(); let m = metrics.clone();
        Some(tokio::spawn(async move { fault_injector::run_fault_injector_async(s, m).await; }))
    } else { None };

    tokio::time::sleep(tokio::time::Duration::from_secs(config.duration_secs)).await;
    sse_state.should_stop.store(true, Ordering::Release);

    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
        let _ = h1.await; let _ = h2.await; let _ = h3.await; let _ = h4.await;
        if let Some(h) = h5 { let _ = h.await; }
    }).await;
}
