//! Unit tests for core system logic.
//!
//! Covers the pure-logic functions that do not require a live SSE stream:
//! deadline arithmetic, percentile calculation, SSE event type detection,
//! queue priority scheduling, overflow policy, degraded mode transitions,
//! and the leaderboard ranking.
//!
//! Run with: `cargo test`

#[cfg(test)]
mod tests {
    use wikipedia_rts::metrics::{percentile, PacketRecord};
    use std::time::{Duration, Instant};

    /// Creates a [`PacketRecord`] with the given drift and execution times.
    ///
    /// Constructs `arrived_at`, `dequeued_at`, and `processed_at` from
    /// `Instant::now()` plus microsecond offsets so the timing methods
    /// return the expected millisecond values.
    fn make_record(drift_ms: f64, exec_ms: f64, is_human: bool) -> PacketRecord {
        let arrived_at   = Instant::now();
        let dequeued_at  = arrived_at  + Duration::from_micros((drift_ms * 1000.0) as u64);
        let processed_at = dequeued_at + Duration::from_micros((exec_ms  * 1000.0) as u64);
        PacketRecord {
            arrived_at, dequeued_at, processed_at, is_human,
            domain: "en.wikipedia.org".to_string(),
            user:   "testuser".to_string(),
        }
    }

    // ── Deadline arithmetic ──────────────────────────────────────────────────

    /// A packet whose total time (drift + exec) is under 2 ms must meet the deadline.
    #[test]
    fn test_deadline_within_2ms_passes() {
        // total = 0.5 + 0.3 = 0.8 ms → MET
        let r = make_record(0.5, 0.3, true);
        assert!(r.deadline_met());
    }

    /// A packet whose total time exceeds 2 ms must miss the deadline.
    #[test]
    fn test_deadline_over_2ms_fails() {
        // total = 1.8 + 0.5 = 2.3 ms → MISS
        let r = make_record(1.8, 0.5, false);
        assert!(!r.deadline_met());
    }

    /// A bot edit that waits 3 ms in the queue behind higher-priority human edits
    /// will exceed the 2 ms deadline even with fast execution. This is the natural
    /// failure mode that drives miss_rate up and triggers degraded mode.
    #[test]
    fn test_bot_high_drift_misses_deadline() {
        // Bot drift = 3.0 ms (waited in queue), exec = 0.3 ms → total = 3.3 ms → MISS
        let r = make_record(3.0, 0.3, false);
        assert!(!r.deadline_met());
    }

    /// A human edit dequeued quickly due to priority scheduling will meet the deadline
    /// even with moderate execution time.
    #[test]
    fn test_human_low_drift_meets_deadline() {
        // Human drift = 0.3 ms (dequeued first), exec = 0.3 ms → total = 0.6 ms → MET
        let r = make_record(0.3, 0.3, true);
        assert!(r.deadline_met());
    }

    // ── Percentile calculation ───────────────────────────────────────────────

    /// The percentile function must return correct p0, p50, and p100 values
    /// for a simple sorted dataset.
    #[test]
    fn test_percentile_basic() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile(data.clone(), 50.0), 3.0);
        assert_eq!(percentile(data.clone(), 0.0),  1.0);
        assert_eq!(percentile(data.clone(), 100.0), 5.0);
    }

    /// An empty dataset must return 0.0 rather than panicking.
    #[test]
    fn test_percentile_empty() {
        assert_eq!(percentile(vec![], 99.0), 0.0);
    }

    // ── SSE event classification ─────────────────────────────────────────────

    /// A MinEdit with `bot: Some(true)` must not be classified as human.
    #[test]
    fn test_min_edit_bot_true_not_human() {
        use wikipedia_rts::model::MinEdit;
        let m = MinEdit { event_type: Some("edit".into()), bot: Some(true),
                          title: None, server_name: None, user: None };
        assert!(!m.is_human());
    }

    /// A MinEdit with no `bot` field must default to human to avoid silent drops.
    #[test]
    fn test_min_edit_bot_absent_defaults_human() {
        use wikipedia_rts::model::MinEdit;
        let m = MinEdit { event_type: Some("edit".into()), bot: None,
                          title: None, server_name: None, user: None };
        assert!(m.is_human());
    }

    // ── Queue priority scheduling ────────────────────────────────────────────

    /// Helper to build a QueuedEdit without a full pipeline setup.
    fn make_edit(is_human: bool, title: &str) -> wikipedia_rts::state::QueuedEdit {
        wikipedia_rts::state::QueuedEdit {
            raw:         "{}".to_string(),
            arrived_at:  Instant::now(),
            is_human,
            title:       title.to_string(),
            server_name: "en.wikipedia.org".to_string(),
            user:        "u".to_string(),
        }
    }

    /// When a bot edit is enqueued before a human edit, dequeue must return the
    /// human edit first — regardless of arrival order.
    #[test]
    fn test_dequeue_human_before_bot() {
        let state  = wikipedia_rts::state::SharedState::new();
        let logger = wikipedia_rts::logger::Logger::null();
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(false, "Bot article"));
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(true,  "Human article"));
        let first = wikipedia_rts::queue::dequeue(&state).unwrap();
        assert!(first.is_human, "expected human edit to be dequeued first");
    }

    /// When the queue is full and a new human edit arrives, the oldest bot in the
    /// queue must be evicted to make room. No human edits should be dropped.
    #[test]
    fn test_enqueue_overflow_drops_bot_not_human() {
        use wikipedia_rts::state::QUEUE_CAPACITY;
        let state  = wikipedia_rts::state::SharedState::new();
        let logger = wikipedia_rts::logger::Logger::null();
        // Fill the queue with CAPACITY-1 human edits, then one bot.
        for _ in 0..(QUEUE_CAPACITY - 1) {
            wikipedia_rts::queue::enqueue(&state, &logger, make_edit(true, "human"));
        }
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(false, "bot_victim"));
        // Enqueue one more human — the bot should be evicted to make room.
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(true, "new_human"));
        let q = state.queue.lock().unwrap();
        let bot_count = q.iter().filter(|e| !e.is_human).count();
        assert_eq!(bot_count, 0, "bot should have been dropped on overflow");
        assert_eq!(q.len(), QUEUE_CAPACITY, "queue should be at capacity");
    }

    /// In degraded mode, dequeue must skip bot edits and return only human edits.
    /// A queue with only bots must return None rather than processing a bot.
    #[test]
    fn test_dequeue_degraded_skips_bots() {
        use std::sync::atomic::Ordering;
        let state  = wikipedia_rts::state::SharedState::new();
        let logger = wikipedia_rts::logger::Logger::null();
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(false, "bot1"));
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(false, "bot2"));
        wikipedia_rts::queue::enqueue(&state, &logger, make_edit(true,  "human1"));
        state.degraded.store(true, Ordering::Relaxed);
        let got = wikipedia_rts::queue::dequeue(&state).unwrap();
        assert!(got.is_human, "degraded mode must skip bot edits");
    }

    // ── Degraded mode transitions ────────────────────────────────────────────

    /// When the rolling window is filled with all misses (100% miss rate > 50%
    /// threshold), `update_mode` must set `degraded = true`.
    #[test]
    fn test_update_mode_enters_degraded() {
        use wikipedia_rts::state::ROLLING_WINDOW_SIZE;
        use std::sync::atomic::Ordering;
        let state  = wikipedia_rts::state::SharedState::new();
        let logger = wikipedia_rts::logger::Logger::null();
        for _ in 0..ROLLING_WINDOW_SIZE {
            state.push_result(false); // all misses
        }
        state.update_mode(&logger);
        assert!(state.degraded.load(Ordering::Relaxed), "should enter degraded after >50% misses");
    }

    /// When the rolling window is filled with all hits (0% miss rate < 20%
    /// threshold) while already degraded, `update_mode` must set `degraded = false`.
    #[test]
    fn test_update_mode_recovers() {
        use wikipedia_rts::state::ROLLING_WINDOW_SIZE;
        use std::sync::atomic::Ordering;
        let state  = wikipedia_rts::state::SharedState::new();
        let logger = wikipedia_rts::logger::Logger::null();
        state.degraded.store(true, Ordering::Relaxed);
        for _ in 0..ROLLING_WINDOW_SIZE {
            state.push_result(true); // all hits
        }
        state.update_mode(&logger);
        assert!(!state.degraded.load(Ordering::Relaxed), "should recover after <20% misses");
    }

    // ── Leaderboard ──────────────────────────────────────────────────────────

    /// `top3` must return at most 3 entries sorted by edit count in descending order.
    #[test]
    fn test_leaderboard_top3_correct_order() {
        let state = wikipedia_rts::state::SharedState::new();
        // en: 3 edits, de: 2 edits, fr: 1 edit
        wikipedia_rts::leaderboard::update(&state, "en.wikipedia.org");
        wikipedia_rts::leaderboard::update(&state, "en.wikipedia.org");
        wikipedia_rts::leaderboard::update(&state, "en.wikipedia.org");
        wikipedia_rts::leaderboard::update(&state, "de.wikipedia.org");
        wikipedia_rts::leaderboard::update(&state, "de.wikipedia.org");
        wikipedia_rts::leaderboard::update(&state, "fr.wikipedia.org");
        let top = wikipedia_rts::leaderboard::top3(&state);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, "en.wikipedia.org");
        assert_eq!(top[0].1, 3);
        assert_eq!(top[1].0, "de.wikipedia.org");
        assert_eq!(top[1].1, 2);
        assert_eq!(top[2].0, "fr.wikipedia.org");
        assert_eq!(top[2].1, 1);
    }
}
