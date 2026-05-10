//! Watchdog thread used by the threaded pipeline (async pipeline has its own inline task).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::state::{SharedState, WATCHDOG_TIMEOUT_S};
use crate::logger::Logger;

pub fn start(state: Arc<SharedState>, logger: Arc<Logger>) {
    std::thread::spawn(move || {
        loop {
            // Sleep first so the stream has time to connect before the first check.
            std::thread::sleep(Duration::from_secs(1));

            let last = state.last_data_ns.load(Ordering::Relaxed);
            if last == 0 { continue; }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
            let elapsed = (now - last) / 1_000_000_000;

            if elapsed >= WATCHDOG_TIMEOUT_S {
                logger.log(&format!(
                    "[WATCHDOG]  No data for {}s | Action: RECONNECT", elapsed
                ));
                state.reconnect_needed.store(true, Ordering::Relaxed);
                state.last_data_ns.store(0, Ordering::Relaxed);
            }
        }
    });
}
