// scheduling/scheduler.rs — Priority scheduler (Components B + C).
// Drains channel → classifies into 5 tier queues → processes highest first.
// Component C: Scheduling Drift measured as T5-T4 (actual vs expected start).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use std::collections::VecDeque;

use crate::models::*;
use crate::metrics::collector::MetricsCollector;
use crate::parsing::zero_copy;
use crate::ingestion::sse_client::SseState;
use crate::ingestion::overflow_channel::OverflowReceiver;
use crate::shared::leaderboard_atomic::AtomicLeaderboard;
use crate::shared::leaderboard_mutex::MutexLeaderboard;
use crate::shared::leaderboard_rwlock::RwLockLeaderboard;

/// Async scheduler loop: ingests from channel, schedules by priority, yields when idle.
pub async fn run_async_scheduler(
    receiver: OverflowReceiver,
    metrics: MetricsCollector,
    state: Arc<SseState>,
    atomic_lb: AtomicLeaderboard,
    mutex_lb: MutexLeaderboard,
    rwlock_lb: RwLockLeaderboard,
) {
    let mut tq: [VecDeque<QueuedEdit>; 5] = Default::default();
    let mut eps: u32 = 0;
    let mut last_eps = Instant::now();

    loop {
        if state.should_stop.load(Ordering::Acquire) {
            drain(&mut tq, &metrics, &atomic_lb, &mutex_lb, &rwlock_lb);
            return;
        }

        ingest_all(&receiver, &metrics, &mut tq, &mut eps, &mut last_eps);

        let edit = tq[0].pop_front()
            .or_else(|| tq[1].pop_front())
            .or_else(|| tq[2].pop_front())
            .or_else(|| tq[3].pop_front())
            .or_else(|| tq[4].pop_front());

        if let Some(q) = edit {
            process_edit(q, &metrics, &atomic_lb, &mutex_lb, &rwlock_lb);
        } else {
            tokio::task::yield_now().await;
        }
    }
}

/// Threaded scheduler loop: ingests from channel, schedules by priority, sleeps when idle.
pub fn run_threaded_scheduler(
    receiver: OverflowReceiver,
    metrics: MetricsCollector,
    state: Arc<SseState>,
    atomic_lb: AtomicLeaderboard,
    mutex_lb: MutexLeaderboard,
    rwlock_lb: RwLockLeaderboard,
) {
    let mut tq: [VecDeque<QueuedEdit>; 5] = Default::default();
    let mut eps: u32 = 0;
    let mut last_eps = Instant::now();

    loop {
        if state.should_stop.load(Ordering::Acquire) {
            drain(&mut tq, &metrics, &atomic_lb, &mutex_lb, &rwlock_lb);
            return;
        }

        ingest_all(&receiver, &metrics, &mut tq, &mut eps, &mut last_eps);

        let edit = tq[0].pop_front()
            .or_else(|| tq[1].pop_front())
            .or_else(|| tq[2].pop_front())
            .or_else(|| tq[3].pop_front())
            .or_else(|| tq[4].pop_front());

        if let Some(q) = edit {
            process_edit(q, &metrics, &atomic_lb, &mutex_lb, &rwlock_lb);
        } else {
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    }
}

/// Drains all available items from the channel, classifies by tier, and enqueues.
/// Sheds edits whose tier is marked for shedding in the current degradation mode.
fn ingest_all(
    receiver: &OverflowReceiver,
    metrics: &MetricsCollector,
    tq: &mut [VecDeque<QueuedEdit>; 5],
    eps: &mut u32,
    last_eps: &mut Instant,
) {
    while let Some((raw, ingestion_time)) = receiver.try_recv() {
        let channel_exit_time = Instant::now();
        *eps += 1;

        if last_eps.elapsed().as_secs() >= 1 {
            metrics.update_events_per_sec(*eps as f64);
            *eps = 0;
            *last_eps = Instant::now();
        }

        if let Some((tier, _)) = zero_copy::quick_classify(&raw) {
            let mode = metrics.current_mode();
            if mode.should_shed(tier) {
                metrics.record_shed(tier);
            } else {
                let queued = QueuedEdit {
                    raw,
                    tier,
                    ingestion_time,
                    channel_exit_time,
                    expected_start: Instant::now(),
                };
                tq[tier as usize - 1].push_back(queued);
            }
        }
    }

    metrics.update_channel_fill(receiver.len());
}

/// Processes one edit: zero-copy parse → update all three leaderboards → record metrics.
/// Applies CPU spike delay if fault injection is active.
fn process_edit(
    queued: QueuedEdit,
    metrics: &MetricsCollector,
    atomic_lb: &AtomicLeaderboard,
    mutex_lb: &MutexLeaderboard,
    rwlock_lb: &RwLockLeaderboard,
) {
    let actual_start = Instant::now();
    crate::watchdog::fault_injector::apply_cpu_spike_delay();

    if let Some(edit) = zero_copy::zero_copy_parse(&queued.raw) {
        let server_name = edit.server_name.to_string();

        atomic_lb.increment(&server_name);
        let is_hp = !queued.tier.is_bot();
        mutex_lb.increment(&server_name, is_hp, metrics);
        rwlock_lb.increment(&server_name, is_hp, metrics);

        let process_complete = Instant::now();
        let latency_us = process_complete
            .duration_since(queued.channel_exit_time)
            .as_micros() as u64;

        metrics.record_edit(EditMetrics {
            tier: queued.tier,
            ingestion_time: queued.ingestion_time,
            channel_exit_time: queued.channel_exit_time,
            expected_start: queued.expected_start,
            actual_start,
            process_complete,
            deadline_met: latency_us <= queued.tier.deadline_us(),
            server_name,
        });
    }
}

/// Flushes all remaining queued edits at shutdown.
fn drain(
    tq: &mut [VecDeque<QueuedEdit>; 5],
    metrics: &MetricsCollector,
    a: &AtomicLeaderboard,
    m: &MutexLeaderboard,
    r: &RwLockLeaderboard,
) {
    for q in tq.iter_mut() {
        while let Some(e) = q.pop_front() {
            process_edit(e, metrics, a, m, r);
        }
    }
}
