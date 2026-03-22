// ingestion/threaded_pipeline.rs — Multi-threaded pipeline using std::thread (Component A).
// Spawns OS threads: SSE ingestion, scheduler, watchdog, failsafe, fault injector.

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

/// Orchestrates the full threaded pipeline: spawns all subsystem threads, waits for duration, then shuts down.
pub fn run(config: AppConfig, metrics: MetricsCollector, sse_state: Arc<SseState>) {
    let channel = OverflowChannel::new(config.channel_capacity, metrics.clone());
    let sender = channel.sender();
    let receiver = channel.receiver();

    let atomic_lb = AtomicLeaderboard::new();
    let mutex_lb = MutexLeaderboard::new();
    let rwlock_lb = RwLockLeaderboard::new();

    let s = sse_state.clone();
    let t1 = std::thread::Builder::new().name("sse-ingestion".into())
        .spawn(move || { sse_client::connect_sse_threaded(s, sender); })
        .expect("spawn ingestion");

    let m = metrics.clone(); let s = sse_state.clone();
    let t2 = std::thread::Builder::new().name("scheduler".into())
        .spawn(move || {
            scheduler::run_threaded_scheduler(receiver, m, s, atomic_lb, mutex_lb, rwlock_lb);
        })
        .expect("spawn scheduler");

    let s = sse_state.clone(); let m = metrics.clone();
    let t3 = std::thread::Builder::new().name("watchdog".into())
        .spawn(move || { timer::run_watchdog_threaded(s, m); })
        .expect("spawn watchdog");

    let s = sse_state.clone(); let m = metrics.clone(); let c = config.clone();
    let t4 = std::thread::Builder::new().name("failsafe".into())
        .spawn(move || { failsafe::run_failsafe_threaded(s, m, c); })
        .expect("spawn failsafe");

    let t5 = if config.faults_enabled {
        let s = sse_state.clone(); let m = metrics.clone();
        Some(std::thread::Builder::new().name("fault-injector".into())
            .spawn(move || { fault_injector::run_fault_injector_threaded(s, m); })
            .expect("spawn fault injector"))
    } else { None };

    std::thread::sleep(std::time::Duration::from_secs(config.duration_secs));
    sse_state.should_stop.store(true, Ordering::Release);

    let _ = t1.join(); let _ = t2.join(); let _ = t3.join(); let _ = t4.join();
    if let Some(t) = t5 { let _ = t.join(); }
}
