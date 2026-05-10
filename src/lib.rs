pub mod allocator;
pub mod model;
pub mod metrics;
pub mod state;
pub mod logger;
pub mod queue;
pub mod leaderboard;
pub mod watchdog;
pub mod pipeline;
pub mod ui;

#[global_allocator]
static ALLOCATOR: allocator::TrackingAllocator = allocator::TrackingAllocator;
