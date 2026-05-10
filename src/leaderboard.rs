//! Top-3 domain leaderboard backed by SharedState::leaderboard (Mutex<HashMap>).

use std::sync::Arc;
use crate::state::SharedState;

pub fn update(state: &Arc<SharedState>, domain: &str) {
    let mut board = state.leaderboard.lock().unwrap();
    *board.entry(domain.to_string()).or_insert(0) += 1;
}

pub fn top3(state: &Arc<SharedState>) -> Vec<(String, u64)> {
    let board = state.leaderboard.lock().unwrap();
    let mut v: Vec<(String, u64)> = board.iter()
        .map(|(k, v)| (k.clone(), *v)).collect();
    v.sort_by(|a, b| b.1.cmp(&a.1));
    v.truncate(3);
    v
}
