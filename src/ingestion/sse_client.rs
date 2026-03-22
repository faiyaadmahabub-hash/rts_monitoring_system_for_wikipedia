// ingestion/sse_client.rs — SSE stream client for Wikipedia Recent Changes (Component A).
// Connects to Wikimedia EventStreams, reconnects with exponential backoff.
// Captures first 500 events to data/captured_events.jsonl for Criterion benchmarks.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::io::Write;

use crate::ingestion::overflow_channel::OverflowSender;

static CAPTURE_COUNT: AtomicUsize = AtomicUsize::new(0);
const MAX_CAPTURE: usize = 500;
static CAPTURE_FILE: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

/// Saves raw JSON event to captured_events.jsonl for offline Criterion benchmarks.
fn capture_event(json: &str) {
    if CAPTURE_COUNT.load(Ordering::Relaxed) >= MAX_CAPTURE {
        return;
    }
    let file = CAPTURE_FILE.get_or_init(|| {
        let _ = std::fs::create_dir_all("data");
        Mutex::new(
            std::fs::File::create("data/captured_events.jsonl")
                .expect("Failed to create data/captured_events.jsonl"),
        )
    });
    if let Ok(mut f) = file.lock() {
        if CAPTURE_COUNT.fetch_add(1, Ordering::Relaxed) < MAX_CAPTURE {
            let _ = writeln!(f, "{}", json);
        }
    }
}

pub const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";
const SSE_USER_AGENT: &str = "wiki-rts/1.0 (student project; contact: local-dev)";

/// Shared SSE connection state — accessed by watchdog, fault injector, dashboard.
pub struct SseState {
    pub connected: AtomicBool,
    pub last_event_time: AtomicU64,
    pub should_reconnect: AtomicBool,
    pub should_stop: AtomicBool,
    pub network_blocked: AtomicBool,
}

impl SseState {
    /// Creates a new SseState wrapped in Arc for shared ownership.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            connected: AtomicBool::new(false),
            last_event_time: AtomicU64::new(0),
            should_reconnect: AtomicBool::new(false),
            should_stop: AtomicBool::new(false),
            network_blocked: AtomicBool::new(false),
        })
    }

    /// Records current time as last event timestamp (for watchdog timeout checks).
    pub fn update_last_event(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_event_time.store(now, Ordering::Release);
    }

    /// Returns milliseconds elapsed since last received SSE event.
    pub fn millis_since_last_event(&self) -> u64 {
        let last = self.last_event_time.load(Ordering::Acquire);
        if last == 0 { return 0; }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now.saturating_sub(last)
    }
}

/// Async SSE client: connects, streams events into the overflow channel, auto-reconnects.
pub async fn connect_sse_async(state: Arc<SseState>, sender: OverflowSender) {
    let mut backoff_ms: u64 = 1000;
    let max_backoff: u64 = 8000;

    loop {
        if state.should_stop.load(Ordering::Acquire) { break; }
        if state.network_blocked.load(Ordering::Acquire) {
            state.connected.store(false, Ordering::Release);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        match async_stream_loop(&state, &sender).await {
            Ok(()) => break,
            Err(e) => {
                state.connected.store(false, Ordering::Release);
                let jitter = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as u64 % (backoff_ms / 4 + 1);
                eprintln!("[SSE] reconnecting in {}ms ({})", backoff_ms + jitter, e);
                tokio::time::sleep(Duration::from_millis(backoff_ms + jitter)).await;
                backoff_ms = (backoff_ms * 2).min(max_backoff);
            }
        }
        if state.should_reconnect.load(Ordering::Acquire) {
            state.should_reconnect.store(false, Ordering::Release);
            backoff_ms = 1000;
        }
    }
}

/// Opens HTTP SSE connection, reads chunked stream, forwards JSON events to channel.
async fn async_stream_loop(
    state: &Arc<SseState>,
    sender: &OverflowSender,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::builder()
        .user_agent(SSE_USER_AGENT)
        .connect_timeout(Duration::from_secs(8))
        .tcp_keepalive(Duration::from_secs(30))
        .http1_only()
        .build()?;
    let mut resp = client
        .get(SSE_URL)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }

    state.connected.store(true, Ordering::Release);
    state.update_last_event();

    let mut buf = String::new();
    while let Some(chunk) = resp.chunk().await? {
        if state.should_stop.load(Ordering::Acquire) { return Ok(()); }
        if state.network_blocked.load(Ordering::Acquire) {
            return Err("Network blocked".into());
        }

        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buf.find('\n') {
            let line = buf[..pos].trim().to_string();
            buf = buf.split_off(pos + 1);

            if let Some(json) = line.strip_prefix("data: ") {
                if !json.is_empty() && json.starts_with('{') {
                    let t1 = Instant::now();
                    state.update_last_event();
                    capture_event(json);
                    sender.send((json.to_string(), t1));
                }
            }
        }
    }
    Ok(())
}

/// Threaded SSE client: blocking version of connect_sse_async for std::thread pipeline.
pub fn connect_sse_threaded(state: Arc<SseState>, sender: OverflowSender) {
    let mut backoff_ms: u64 = 1000;
    let max_backoff: u64 = 8000;

    loop {
        if state.should_stop.load(Ordering::Acquire) { break; }
        if state.network_blocked.load(Ordering::Acquire) {
            state.connected.store(false, Ordering::Release);
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }

        match threaded_stream_loop(&state, &sender) {
            Ok(()) => break,
            Err(e) => {
                state.connected.store(false, Ordering::Release);
                let jitter = rand::random::<u64>() % (backoff_ms / 4 + 1);
                eprintln!("[SSE] reconnecting in {}ms ({})", backoff_ms + jitter, e);
                std::thread::sleep(Duration::from_millis(backoff_ms + jitter));
                backoff_ms = (backoff_ms * 2).min(max_backoff);
            }
        }
        if state.should_reconnect.load(Ordering::Acquire) {
            state.should_reconnect.store(false, Ordering::Release);
            backoff_ms = 1000;
        }
    }
}

/// Opens blocking HTTP SSE connection, reads line-by-line, forwards JSON events.
fn threaded_stream_loop(
    state: &Arc<SseState>,
    sender: &OverflowSender,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::BufRead;

    let client = reqwest::blocking::Client::builder()
        .user_agent(SSE_USER_AGENT)
        .connect_timeout(Duration::from_secs(8))
        .tcp_keepalive(Duration::from_secs(30))
        .http1_only()
        .build()?;
    let resp = client
        .get(SSE_URL)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .send()?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }

    state.connected.store(true, Ordering::Release);
    state.update_last_event();

    let reader = std::io::BufReader::new(resp);
    for line_result in reader.lines() {
        let line = line_result?;
        if state.should_stop.load(Ordering::Acquire) { return Ok(()); }
        if state.network_blocked.load(Ordering::Acquire) {
            return Err("Network blocked".into());
        }

        if let Some(json) = line.strip_prefix("data: ") {
            if !json.is_empty() && json.starts_with('{') {
                let t1 = Instant::now();
                state.update_last_event();
                capture_event(json);
                sender.send((json.to_string(), t1));
            }
        }
    }
    Ok(())
}
