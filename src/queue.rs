//! Bounded priority queue. On overflow: evict oldest bot, or drop incoming if queue is all-human.

use std::sync::Arc;
use crate::state::{SharedState, QueuedEdit, QUEUE_CAPACITY};
use crate::logger::Logger;

pub fn enqueue(state: &Arc<SharedState>, logger: &Arc<Logger>, edit: QueuedEdit) {
    let mut q = state.queue.lock().unwrap();
    if q.len() >= QUEUE_CAPACITY {
        match q.iter().position(|e| !e.is_human) {
            Some(bot_pos) => {
                // Remove the oldest bot edit and make room for the new arrival.
                if let Some(dropped) = q.remove(bot_pos) {
                    logger.log(&format!(
                        "[OVERFLOW]  ts={}  dropped=bot  title={}  queue={}/{}",
                        chrono::Local::now().format("%H:%M:%S%.3f"),
                        dropped.title,
                        QUEUE_CAPACITY, QUEUE_CAPACITY,
                    ));
                    state.overflow_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                q.push_back(edit);
            }
            None => {
                // All-human queue: drop the incoming edit to preserve already-queued human work.
                logger.log(&format!(
                    "[OVERFLOW]  ts={}  dropped=incoming  title={}  queue={}/{} (all-human queue)",
                    chrono::Local::now().format("%H:%M:%S%.3f"),
                    edit.title,
                    QUEUE_CAPACITY, QUEUE_CAPACITY,
                ));
                state.overflow_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    } else {
        q.push_back(edit);
    }
}

pub fn dequeue(state: &Arc<SharedState>) -> Option<QueuedEdit> {
    let mut q = state.queue.lock().unwrap();
    if q.is_empty() { return None; }

    if state.degraded.load(std::sync::atomic::Ordering::Relaxed) {
        // Degraded mode: skip bots, return None if only bots remain.
        let pos = q.iter().position(|e| e.is_human)?;
        return Some(q.remove(pos).unwrap());
    }

    let pos = q.iter().position(|e| e.is_human).unwrap_or(0);
    Some(q.remove(pos).unwrap())
}
