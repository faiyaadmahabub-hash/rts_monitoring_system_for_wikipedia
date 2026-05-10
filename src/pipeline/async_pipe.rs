//! Tokio async pipeline: ingestion, processor, and watchdog tasks.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;

use crate::state::{SharedState, QueuedEdit, WATCHDOG_TIMEOUT_S};
use crate::logger::Logger;
use crate::model::MinEdit;
use crate::queue::{enqueue, dequeue};
use crate::pipeline::process_edit;

/// Wikipedia SSE stream endpoint.
const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

pub async fn run(state: Arc<SharedState>, logger: Arc<Logger>) {
    // Clone Arc references once per task to avoid borrow conflicts.
    let state_ingest   = Arc::clone(&state);
    let logger_ingest  = Arc::clone(&logger);
    let state_proc     = Arc::clone(&state);
    let logger_proc    = Arc::clone(&logger);
    let state_watch    = Arc::clone(&state);
    let logger_watch   = Arc::clone(&logger);

    // watchdog task: checks last_data_ns every 1s, signals reconnect on timeout
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let last = state_watch.last_data_ns.load(Ordering::Relaxed);
            // Skip if no data has arrived yet (stream not yet connected).
            if last == 0 { continue; }
            let now     = now_ns();
            let elapsed = (now - last) / 1_000_000_000;
            if elapsed >= WATCHDOG_TIMEOUT_S {
                logger_watch.log(&format!(
                    "[WATCHDOG]  No data for {}s | Action: RECONNECT", elapsed
                ));
                state_watch.reconnect_needed.store(true, Ordering::Relaxed);
                // Reset so this tick does not re-fire until fresh data arrives.
                state_watch.last_data_ns.store(0, Ordering::Relaxed);
            }
        }
    });

    // processor task: dequeue -> process_edit -> update_mode, yield 1ms when empty
    tokio::spawn(async move {
        loop {
            match dequeue(&state_proc) {
                Some(edit) => {
                    let dequeued_at = Instant::now();
                    process_edit(
                        &edit.raw,
                        edit.arrived_at,
                        dequeued_at,
                        edit.is_human,
                        &edit.title,
                        &edit.server_name,
                        &edit.user,
                        &state_proc,
                        &logger_proc,
                    );
                    state_proc.update_mode(&logger_proc);
                }
                None => {
                    // Queue empty — yield to avoid busy-waiting.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    });

    // ingestion: outer loop reconnects on error/watchdog, inner loop processes chunks
    let client = reqwest::Client::new();

    loop {
        // Clear the reconnect flag before each connection attempt so the inner
        // loop is not immediately broken on the first received line.
        state_ingest.reconnect_needed.store(false, Ordering::Relaxed);

        logger_ingest.log("[INGEST]    Connecting to Wikipedia SSE stream...");

        let resp = match client
            .get(SSE_URL)
            .header("Accept", "text/event-stream")       // required for SSE streaming
            .header("Cache-Control", "no-cache")          // prevent proxy buffering
            .header("User-Agent", "wikipedia-rts/0.1 (student project)")
            .send()
            .await
        {
            Ok(r)  => r,
            Err(e) => {
                logger_ingest.log(&format!("[INGEST] Connection error: {e}"));
                // Brief pause before retrying to avoid hammering the server.
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let mut stream = resp.bytes_stream();
        let mut line_buf = String::new();

        // Count this as a new connection (first connect or reconnect).
        state_ingest.reset_count.fetch_add(1, Ordering::Relaxed);

        'chunk_loop: loop {
            // Check the watchdog reconnect flag before reading the next chunk.
            // This is the mechanism by which the watchdog forces reconnection
            // without needing to kill or interrupt the async task.
            if state_ingest.reconnect_needed.load(Ordering::Relaxed) {
                logger_ingest.log("[INGEST]    Reconnect flag set — reconnecting...");
                break 'chunk_loop;
            }

            let chunk = match stream.next().await {
                Some(Ok(c))  => c,
                Some(Err(e)) => {
                    logger_ingest.log(&format!("[INGEST] Stream error: {e}"));
                    break 'chunk_loop;
                }
                None => {
                    // Stream closed cleanly by the server — reconnect.
                    logger_ingest.log("[INGEST] Stream ended — reconnecting");
                    break 'chunk_loop;
                }
            };

            // Update last_data_ns on every chunk so the watchdog measures
            // true inactivity, not just edit frequency.
            state_ingest.last_data_ns.store(now_ns(), Ordering::Relaxed);

            // The SSE stream sends UTF-8 text. Decode the chunk and accumulate
            // characters into line_buf, flushing on each newline.
            let text = match std::str::from_utf8(&chunk) {
                Ok(t)  => t,
                Err(_) => continue,
            };

            for ch in text.chars() {
                if ch == '\n' {
                    process_sse_line(&line_buf, &state_ingest, &logger_ingest);
                    line_buf.clear();
                } else {
                    line_buf.push(ch);
                }
            }
        }
    }
}

fn process_sse_line(line: &str, state: &Arc<SharedState>, logger: &Arc<Logger>) {
    // SSE protocol: blank lines are heartbeats/separators — skip them.
    if line.is_empty() { return; }
    // SSE protocol: `event:` lines declare the event type — skip them.
    if line.starts_with("event:") { return; }

    // Only process `data:` lines; anything else (e.g. `:ok` comments) is ignored.
    let json = match line.strip_prefix("data: ") {
        Some(j) => j,
        None    => return,
    };

    // Lightweight parse to check the event type and extract metadata.
    // Errors (malformed JSON) are silently skipped — stream corruption is transient.
    let min: MinEdit = match serde_json::from_str(json) {
        Ok(m)  => m,
        Err(_) => return,
    };

    // Filter out non-edit events (log actions, patrol events, categorization, etc.).
    if !min.is_edit() { return; }

    let edit = QueuedEdit {
        raw:         json.to_string(),
        // arrived_at is stamped here, at the earliest possible moment after the
        // edit has been identified as valid, to minimise artificial drift inflation.
        arrived_at:  Instant::now(),
        is_human:    min.is_human(),
        title:       min.title.unwrap_or_default(),
        server_name: min.server_name.unwrap_or_default(),
        user:        min.user.unwrap_or_default(),
    };

    enqueue(state, logger, edit);
}
