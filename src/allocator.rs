// allocator.rs — Custom GlobalAlloc wrapper that counts heap allocations
// on the hot path, proving zero-copy parsing produces no heap activity.
// Advanced Feature: Memory Mastery — proof via custom allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub static HOT_PATH_ACTIVE: AtomicBool = AtomicBool::new(false);
pub static HOT_PATH_ALLOCS: AtomicU64 = AtomicU64::new(0);
pub static TOTAL_HOT_PATH_CHECKS: AtomicU64 = AtomicU64::new(0);

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    /// Intercepts every heap allocation. Increments counter if hot path is active.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if HOT_PATH_ACTIVE.load(Ordering::Relaxed) {
            HOT_PATH_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    /// Delegates deallocation to the system allocator.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Marks the start of a hot-path section. Resets alloc counter.
pub fn begin_hot_path() {
    TOTAL_HOT_PATH_CHECKS.fetch_add(1, Ordering::Relaxed);
    HOT_PATH_ALLOCS.store(0, Ordering::Relaxed);
    HOT_PATH_ACTIVE.store(true, Ordering::Release);
}

/// Marks the end of a hot-path section. Returns number of allocations observed.
pub fn end_hot_path() -> u64 {
    HOT_PATH_ACTIVE.store(false, Ordering::Release);
    HOT_PATH_ALLOCS.load(Ordering::Relaxed)
}

/// Returns total number of hot-path verification checks performed.
pub fn total_checks() -> u64 {
    TOTAL_HOT_PATH_CHECKS.load(Ordering::Relaxed)
}
