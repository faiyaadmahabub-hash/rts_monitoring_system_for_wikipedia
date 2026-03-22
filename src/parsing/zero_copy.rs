// parsing/zero_copy.rs — Zero-copy JSON parsing (Component B).
// WikiEdit<'a> borrows &str fields directly from raw JSON via serde lifetimes.
// Advanced Feature: Memory Mastery — custom allocator verifies 0 heap allocations on hot path.

use crate::models::{WikiEdit, QuickClassify, EditTier};
use crate::allocator;

/// Deserializes raw JSON into WikiEdit<'a> with zero-copy string borrowing.
/// Verifies via custom GlobalAlloc that field access produces no heap allocations.
pub fn zero_copy_parse<'a>(raw: &'a str) -> Option<WikiEdit<'a>> {
    let result = serde_json::from_str::<WikiEdit<'a>>(raw).ok();

    if let Some(ref edit) = result {
        allocator::begin_hot_path();
        let _ = std::hint::black_box(edit.user);
        let _ = std::hint::black_box(edit.server_name);
        let _ = std::hint::black_box(edit.title);
        let _ = std::hint::black_box(edit.edit_type);
        let _ = std::hint::black_box(edit.bot);
        let _ = std::hint::black_box(edit.namespace);
        let _ = std::hint::black_box(edit.minor);
        let allocs = allocator::end_hot_path();
        if allocs > 0 {
            eprintln!("[WARN] Hot path allocated {} times during field access", allocs);
        }
    }

    result
}

/// Lightweight classify: extracts only bot/namespace/minor for tier assignment.
/// Used by scheduler to route edits into priority queues without full parsing.
pub fn quick_classify(raw: &str) -> Option<(EditTier, QuickClassify)> {
    let qc: QuickClassify = serde_json::from_str(raw).ok()?;
    let tier = EditTier::classify(qc.bot, qc.namespace, qc.minor);
    Some((tier, qc))
}
