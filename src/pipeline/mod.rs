// Shared edit processing used by both the async and threaded pipelines.

pub mod async_pipe;
pub mod thread_pipe;

use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use crate::state::SharedState;
use crate::metrics::PacketRecord;
use crate::model::WikiEdit;
use crate::logger::Logger;
use crate::leaderboard;

pub fn process_edit(
    raw:         &str,
    arrived_at:  Instant,
    dequeued_at: Instant,
    is_human:    bool,
    title:       &str,
    server_name: &str,
    user:        &str,
    state:       &Arc<SharedState>,
    logger:      &Arc<Logger>,
) -> Option<PacketRecord> {

    // Drop bot edits on articles last touched by a human.
    {
        let mut doc_map = state.last_doc_editor.lock().unwrap();
        if !is_human {
            if *doc_map.get(title).unwrap_or(&false) {
                state.override_count.fetch_add(1, Ordering::Relaxed);
                logger.log(&format!(
                    "[OVERRIDE]  Bot dropped (doc last human-edited) | title={}", title
                ));
                return None;
            }
        }
        // Record whether this edit was human so the next edit on the same doc
        // can check it.
        doc_map.insert(title.to_string(), is_human);
    }

    leaderboard::update(state, server_name);

    // Zero-copy parse: WikiEdit<'a> borrows &str slices from `raw` (no heap alloc).
    let _edit: WikiEdit<'_> = serde_json::from_str(raw).ok()?;

    let processed_at = Instant::now();

    let record = PacketRecord {
        arrived_at,
        dequeued_at,
        processed_at,
        is_human,
        domain: server_name.to_string(),
        user:   user.to_string(),
    };

    let deadline_met = record.deadline_met();
    let tag    = if is_human { "HUMAN" } else { "BOT  " };
    let status = if deadline_met { "DEADLINE MET " } else { "DEADLINE MISS" };

    logger.log(&format!(
        "[{}]    {} | drift={:.2}ms | exec={:.2}ms | total={:.2}ms | {}",
        tag, server_name,
        record.drift_ms(), record.exec_ms(), record.total_ms(), status
    ));

    state.total_processed.fetch_add(1, Ordering::Relaxed);
    if !deadline_met { state.total_missed.fetch_add(1, Ordering::Relaxed); }
    state.push_result(deadline_met);
    state.records.lock().unwrap().push(record.clone());

    Some(record)
}
