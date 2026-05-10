//! std::thread pipeline: ingestion, processor, and watchdog threads.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::io::BufRead;

use crate::state::{SharedState, QueuedEdit};
use crate::logger::Logger;
use crate::model::MinEdit;
use crate::queue::{enqueue, dequeue};
use crate::pipeline::process_edit;
use crate::watchdog;

/// Wikipedia SSE stream endpoint.
const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

pub fn run(state: Arc<SharedState>, logger: Arc<Logger>) {
    let state_ingest  = Arc::clone(&state);
    let logger_ingest = Arc::clone(&logger);
    let state_proc    = Arc::clone(&state);
    let logger_proc   = Arc::clone(&logger);

    // Start the std::thread watchdog — monitors last_data_ns and signals reconnect.
    watchdog::start(Arc::clone(&state), Arc::clone(&logger));

    // ingestion thread: connects to SSE stream, reconnects on error or watchdog signal
    std::thread::spawn(move || {
        // connect_timeout: max time to establish the TCP connection.
        // timeout: max total request duration (acts as a read deadline for blocking
        // reqwest — there is no separate read_timeout on the blocking client).
        // A 3-second timeout means the inner loop wakes at least every 3s to
        // check the reconnect_needed flag, even during stream quiet periods.
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(3))
            .build()
            .expect("Failed to build blocking reqwest client");

        loop {
            // Clear the reconnect flag before each connection attempt.
            state_ingest.reconnect_needed.store(false, Ordering::Relaxed);

            logger_ingest.log("[INGEST]    Connecting to Wikipedia SSE stream (threaded)...");

            let resp = match client
                .get(SSE_URL)
                .header("Accept", "text/event-stream")      // required for SSE streaming
                .header("Cache-Control", "no-cache")         // prevent proxy buffering
                .header("User-Agent", "wikipedia-rts/0.1 (student project)")
                .send()
            {
                Ok(r)  => r,
                Err(e) => {
                    logger_ingest.log(&format!("[INGEST] Connection error: {e}"));
                    std::thread::sleep(Duration::from_secs(3));
                    continue;
                }
            };

            let reader = std::io::BufReader::new(resp);

            // Count this as a new connection (first connect or reconnect).
            state_ingest.reset_count.fetch_add(1, Ordering::Relaxed);

            'line_loop: for line_result in reader.lines() {
                // Check the watchdog reconnect flag before processing each line.
                if state_ingest.reconnect_needed.load(Ordering::Relaxed) {
                    logger_ingest.log("[INGEST]    Reconnect flag set — reconnecting...");
                    break 'line_loop;
                }

                let line = match line_result {
                    Ok(l)  => l,
                    Err(e) => {
                        let err_str = e.to_string();
                        // A read timeout from the 3-second request timeout causes a
                        // TimedOut error — treat it as a chance to check the reconnect
                        // flag (already done above) rather than a fatal error.
                        if err_str.contains("timed out") || err_str.contains("os error 10060") {
                            continue 'line_loop;
                        }
                        logger_ingest.log(&format!("[INGEST] Read error: {e}"));
                        break 'line_loop;
                    }
                };

                // Update last_data_ns on every received line (including non-edit events)
                // so the watchdog measures true stream inactivity, not edit frequency.
                state_ingest.last_data_ns.store(now_ns(), Ordering::Relaxed);

                if line.is_empty() { continue; }
                if line.starts_with("event:") { continue; }

                // Only process `data:` lines containing a JSON payload.
                let json = match line.strip_prefix("data: ") {
                    Some(j) => j.to_string(),
                    None    => continue,
                };

                // Lightweight parse to check event type and extract metadata.
                // Errors (malformed JSON) are silently skipped — transient corruption.
                let min: MinEdit = match serde_json::from_str(&json) {
                    Ok(m)  => m,
                    Err(_) => continue,
                };

                // Filter out non-edit events (log, patrol, categorize, etc.).
                if !min.is_edit() { continue; }

                let edit = QueuedEdit {
                    raw:         json,
                    // arrived_at stamped here, at the earliest possible moment after
                    // the edit is identified as valid, to minimise drift inflation.
                    arrived_at:  Instant::now(),
                    is_human:    min.is_human(),
                    title:       min.title.unwrap_or_default(),
                    server_name: min.server_name.unwrap_or_default(),
                    user:        min.user.unwrap_or_default(),
                };

                enqueue(&state_ingest, &logger_ingest, edit);
            }
        }
    });

    // processor thread: dequeue -> process_edit -> update_mode, sleep 1ms when empty
    std::thread::spawn(move || {
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
                    // Queue empty — sleep briefly to avoid busy-waiting.
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    });
}
