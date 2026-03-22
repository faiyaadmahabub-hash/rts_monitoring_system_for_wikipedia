// models.rs — Core data types shared across all pipeline components.

use serde::Deserialize;
use std::time::Instant;
use std::fmt;

// Zero-copy edit: string fields borrow from the raw JSON buffer via serde lifetimes.
// Component B: Zero-Copy Parsing — WikiEdit<'a> avoids heap allocation for string fields.

#[derive(Deserialize, Debug)]
pub struct WikiEdit<'a> {
    #[serde(borrow)]
    pub user: &'a str,
    pub bot: bool,
    #[serde(borrow)]
    pub server_name: &'a str,
    #[serde(borrow)]
    pub title: &'a str,
    pub timestamp: u64,
    #[serde(borrow, rename = "type")]
    pub edit_type: &'a str,
    #[serde(default)]
    pub namespace: i32,
    #[serde(default)]
    pub minor: bool,
}

// Lightweight classification: only the 3 fields needed for tier assignment.
#[derive(Deserialize, Debug)]
pub struct QuickClassify {
    pub bot: bool,
    #[serde(default)]
    pub namespace: i32,
    #[serde(default)]
    pub minor: bool,
}

// 5-tier priority: T1 (human+main) highest → T5 (bot+minor) lowest (RMS).
// Component B+C: Priority Scheduling — human edits get higher priority than bots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EditTier {
    Tier1HumanMainNonMinor = 1,
    Tier2HumanMainMinor = 2,
    Tier3HumanOther = 3,
    Tier4BotNonMinor = 4,
    Tier5BotMinor = 5,
}

impl EditTier {
    /// Assigns a tier based on bot status, namespace, and minor flag.
    pub fn classify(bot: bool, namespace: i32, minor: bool) -> Self {
        match (bot, namespace, minor) {
            (false, 0, false) => EditTier::Tier1HumanMainNonMinor,
            (false, 0, true) => EditTier::Tier2HumanMainMinor,
            (false, _, _) => EditTier::Tier3HumanOther,
            (true, _, false) => EditTier::Tier4BotNonMinor,
            (true, _, true) => EditTier::Tier5BotMinor,
        }
    }

    /// Per-tier micro-deadlines (µs). Derived from 2ms base, scaled by priority.
    pub fn deadline_us(&self) -> u64 {
        match self {
            EditTier::Tier1HumanMainNonMinor => 2_000,  // 2ms
            EditTier::Tier2HumanMainMinor => 3_000,     // 3ms
            EditTier::Tier3HumanOther => 5_000,         // 5ms
            EditTier::Tier4BotNonMinor => 8_000,        // 8ms
            EditTier::Tier5BotMinor => 10_000,          // 10ms
        }
    }

    /// Returns human-readable tier label for display.
    pub fn label(&self) -> &'static str {
        match self {
            EditTier::Tier1HumanMainNonMinor => "T1 human+main",
            EditTier::Tier2HumanMainMinor => "T2 human+main(minor)",
            EditTier::Tier3HumanOther => "T3 human+other",
            EditTier::Tier4BotNonMinor => "T4 bot",
            EditTier::Tier5BotMinor => "T5 bot(minor)",
        }
    }

    /// Returns true if this tier represents a bot edit.
    pub fn is_bot(&self) -> bool {
        matches!(self, EditTier::Tier4BotNonMinor | EditTier::Tier5BotMinor)
    }

    /// Returns all 5 tiers in priority order.
    pub fn all_tiers() -> &'static [EditTier] {
        &[
            EditTier::Tier1HumanMainNonMinor,
            EditTier::Tier2HumanMainMinor,
            EditTier::Tier3HumanOther,
            EditTier::Tier4BotNonMinor,
            EditTier::Tier5BotMinor,
        ]
    }
}

impl fmt::Display for EditTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

// Queued edit: raw JSON + timing metadata for the scheduler.
#[derive(Debug)]
pub struct QueuedEdit {
    pub raw: String,
    pub tier: EditTier,
    pub ingestion_time: Instant,    // T1: SSE arrival
    pub channel_exit_time: Instant, // T3: channel exit
    pub expected_start: Instant,    // T4: tier queue entry
}

// Degradation stages: shed lower tiers progressively to protect T1.
// Component E: Fail-Safe Mode — 4-stage graceful degradation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemMode {
    Normal,
    Degraded1,  // Shed Tier 5 (minor bot edits)
    Degraded2,  // Shed Tier 4 + 5 (all bot edits)
    Degraded3,  // Shed Tier 3 + 4 + 5 (keep only human main articles)
    Degraded4,  // Shed Tier 2 + 3 + 4 + 5 (keep only T1)
}

impl SystemMode {
    /// Returns true if the given tier should be shed in this mode.
    pub fn should_shed(&self, tier: EditTier) -> bool {
        match self {
            SystemMode::Normal => false,
            SystemMode::Degraded1 => tier == EditTier::Tier5BotMinor,
            SystemMode::Degraded2 => tier.is_bot(),
            SystemMode::Degraded3 => {
                matches!(tier,
                    EditTier::Tier3HumanOther |
                    EditTier::Tier4BotNonMinor |
                    EditTier::Tier5BotMinor
                )
            }
            SystemMode::Degraded4 => tier != EditTier::Tier1HumanMainNonMinor,
        }
    }

    /// Returns display label for this mode.
    pub fn label(&self) -> &'static str {
        match self {
            SystemMode::Normal => "NORMAL",
            SystemMode::Degraded1 => "DEGRADED-1",
            SystemMode::Degraded2 => "DEGRADED-2",
            SystemMode::Degraded3 => "DEGRADED-3",
            SystemMode::Degraded4 => "DEGRADED-4",
        }
    }

    /// Moves one stage deeper into degradation.
    pub fn escalate(&self) -> Self {
        match self {
            SystemMode::Normal => SystemMode::Degraded1,
            SystemMode::Degraded1 => SystemMode::Degraded2,
            SystemMode::Degraded2 => SystemMode::Degraded3,
            SystemMode::Degraded3 => SystemMode::Degraded4,
            SystemMode::Degraded4 => SystemMode::Degraded4,
        }
    }

    /// Recovers one stage towards normal operation.
    pub fn deescalate(&self) -> Self {
        match self {
            SystemMode::Normal => SystemMode::Normal,
            SystemMode::Degraded1 => SystemMode::Normal,
            SystemMode::Degraded2 => SystemMode::Degraded1,
            SystemMode::Degraded3 => SystemMode::Degraded2,
            SystemMode::Degraded4 => SystemMode::Degraded3,
        }
    }
}

impl fmt::Display for SystemMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

// Component E: Fault Injection — scheduled faults to stress-test the pipeline.

#[derive(Debug, Clone)]
pub enum FaultType {
    NetworkDrop { duration_secs: u64 },
    CpuSpike { delay_ms: u64, duration_secs: u64 },
    ChannelFlood { duration_secs: u64 },
}

#[derive(Debug, Clone)]
pub struct ScheduledFault {
    pub trigger_at_secs: u64,
    pub fault: FaultType,
}

impl ScheduledFault {
    /// Returns the default fault schedule: network@20s, cpu@40s, flood@55s.
    pub fn default_schedule() -> Vec<Self> {
        vec![
            ScheduledFault {
                trigger_at_secs: 20,
                fault: FaultType::NetworkDrop { duration_secs: 5 },
            },
            ScheduledFault {
                trigger_at_secs: 40,
                fault: FaultType::CpuSpike {
                    delay_ms: 4,
                    duration_secs: 5,
                },
            },
            ScheduledFault {
                trigger_at_secs: 55,
                fault: FaultType::ChannelFlood { duration_secs: 3 },
            },
        ]
    }

    /// Formats the fault as a human-readable label.
    pub fn label(&self) -> String {
        match &self.fault {
            FaultType::NetworkDrop { duration_secs } => {
                format!("{}s: Network drop ({}s)", self.trigger_at_secs, duration_secs)
            }
            FaultType::CpuSpike { delay_ms, duration_secs } => {
                format!("{}s: CPU spike +{}ms ({}s)", self.trigger_at_secs, delay_ms, duration_secs)
            }
            FaultType::ChannelFlood { duration_secs } => {
                format!("{}s: Channel flood ({}s)", self.trigger_at_secs, duration_secs)
            }
        }
    }
}

// Per-edit timing record: T1 (SSE arrival) through T7 (processing complete).
// Used to compute latency, scheduling drift, and deadline compliance.
#[derive(Debug, Clone)]
pub struct EditMetrics {
    pub tier: EditTier,
    pub ingestion_time: Instant,    // T1
    pub channel_exit_time: Instant, // T3
    pub expected_start: Instant,    // T4
    pub actual_start: Instant,      // T5
    pub process_complete: Instant,  // T7
    pub deadline_met: bool,
    pub server_name: String,
}

impl EditMetrics {
    /// Scheduling drift = T5 - T4 (actual vs expected start time).
    pub fn drift_us(&self) -> u64 {
        self.actual_start
            .duration_since(self.expected_start)
            .as_micros() as u64
    }

    /// End-to-end processing latency = T7 - T3 (channel exit to completion).
    pub fn latency_us(&self) -> u64 {
        self.process_complete
            .duration_since(self.channel_exit_time)
            .as_micros() as u64
    }

    /// Total time in system = T7 - T1 (SSE arrival to completion).
    pub fn total_time_us(&self) -> u64 {
        self.process_complete
            .duration_since(self.ingestion_time)
            .as_micros() as u64
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub mode: PipelineMode,
    pub duration_secs: u64,
    pub channel_capacity: usize,
    pub faults_enabled: bool,
    pub degrade_threshold_us: u64,
    pub recover_threshold_us: u64,
    pub stability_window_secs: u64,
    pub compare_files: Option<(String, String)>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            mode: PipelineMode::Async,
            duration_secs: 90,
            channel_capacity: 100,
            faults_enabled: true,
            degrade_threshold_us: 3_000,  // 3ms (1.5x T1 deadline)
            recover_threshold_us: 2_000,  // 2ms (hysteresis gap)
            stability_window_secs: 5,
            compare_files: None,
        }
    }
}

// Component A: Dual-Pipeline — select between Async (Tokio) or Threaded (std::thread).

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PipelineMode {
    Async,
    Threaded,
}

impl fmt::Display for PipelineMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineMode::Async => write!(f, "ASYNC"),
            PipelineMode::Threaded => write!(f, "THREADED"),
        }
    }
}
