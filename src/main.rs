// Wikipedia real-time edit monitor — entry point.

use std::sync::Arc;
use wikipedia_rts::{logger, state, metrics, pipeline, ui, allocator};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: wikipedia-rts --async | --threaded | --capture <N> | --heap-proof");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "--async"      => run_async(),
        "--threaded"   => run_threaded(),
        "--heap-proof" => run_heap_proof(),
        "--capture" => {
            let n: usize = args.get(2)
                .and_then(|s| s.parse().ok())
                .unwrap_or(500);
            run_capture(n);
        }
        other => {
            eprintln!("Unknown argument: {}. Use --async, --threaded, --capture N, or --heap-proof", other);
            std::process::exit(1);
        }
    }
}

// Tokio async pipeline. Spawns the runtime on a background thread so the
// main thread can join the UI handle and exit cleanly when the user presses q.
fn run_async() {
    let shared = state::SharedState::new();
    let log    = logger::Logger::new();

    let ui_handle = ui::start(Arc::clone(&shared));

    let shared_rt = Arc::clone(&shared);
    let log_rt    = Arc::clone(&log);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Tokio runtime");
        rt.block_on(pipeline::async_pipe::run(shared_rt, log_rt));
    });

    let _ = ui_handle.join();
    log.flush();

    let records = shared.records.lock().unwrap();
    metrics::print_stats(&records);
}

// std::thread pipeline. thread_pipe::run spawns the ingestion, processor, and
// watchdog threads then returns, so the main thread can block on the UI handle.
fn run_threaded() {
    let shared = state::SharedState::new();
    let log    = logger::Logger::new();

    let ui_handle = ui::start(Arc::clone(&shared));

    pipeline::thread_pipe::run(Arc::clone(&shared), Arc::clone(&log));

    // Main thread blocks here until the user presses 'q'.
    let _ = ui_handle.join();

    // Flush buffered log events before the background pipeline threads are killed.
    log.flush();

    let records = shared.records.lock().unwrap();
    metrics::print_stats(&records);
}

/// Connects to the Wikipedia SSE stream and saves `n` raw edit JSON lines to
/// `test_data/events.jsonl`, then exits.
///
/// This file is required by Criterion Group 3 (`bench_pipeline`) to replay
/// identical events through both pipeline variants for a fair comparison.
/// Run `cargo run -- --capture 500` once before `cargo bench`.
fn run_capture(n: usize) {
    use std::io::{BufRead, Write};
    use std::fs;

    println!("[CAPTURE] Connecting to Wikipedia SSE stream...");
    println!("[CAPTURE] Will save {} edit events to test_data/events.jsonl", n);

    fs::create_dir_all("test_data").unwrap();
    let mut file = fs::File::create("test_data/events.jsonl").unwrap();

    let client = reqwest::blocking::Client::builder()
        .user_agent("wikipedia-rts/0.1 (student project)")
        .build()
        .unwrap();

    let resp = client
        .get("https://stream.wikimedia.org/v2/stream/recentchange")
        .send()
        .unwrap();

    let reader = std::io::BufReader::new(resp);
    let mut count = 0;

    for line_result in reader.lines() {
        if count >= n { break; }
        let line = match line_result {
            Ok(l) => l,
            Err(_) => break,
        };
        // Only capture `data:` lines that contain edit events.
        if let Some(json) = line.strip_prefix("data: ") {
            if json.starts_with('{') {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(json) {
                    if v.get("type").and_then(|t| t.as_str()) == Some("edit") {
                        writeln!(file, "{}", json).unwrap();
                        count += 1;
                        if count % 50 == 0 {
                            println!("[CAPTURE] {}/{} events saved...", count, n);
                        }
                    }
                }
            }
        }
    }

    println!("[CAPTURE] Done. {} events saved to test_data/events.jsonl", count);
}

fn run_heap_proof() {
    use wikipedia_rts::model::{WikiEdit, WikiEditOwned};

    let raw = r#"{"type":"edit","user":"ExampleEditor","bot":false,"server_name":"en.wikipedia.org","title":"Rust_(programming_language)","comment":"fixed typo","timestamp":1234567890}"#;

    const ITERS: u64 = 10_000;

    // zero-copy: WikiEdit<'a> borrows &str slices from `raw` via #[serde(borrow)]
    let (c0, b0) = allocator::snapshot();
    for _ in 0..ITERS {
        let _: WikiEdit<'_> = serde_json::from_str(raw).unwrap();
    }
    let (c1, b1) = allocator::snapshot();
    let zc_allocs = c1 - c0;
    let zc_bytes  = b1 - b0;

    println!("zero_copy ({} iters): {} allocs  {} bytes", ITERS, zc_allocs, zc_bytes);

    // owned: WikiEditOwned allocates a new String for each string field per parse
    let (c2, b2) = allocator::snapshot();
    for _ in 0..ITERS {
        let _: WikiEditOwned = serde_json::from_str(raw).unwrap();
    }
    let (c3, b3) = allocator::snapshot();
    let ow_allocs = c3 - c2;
    let ow_bytes  = b3 - b2;

    println!("owned     ({} iters): {} allocs  {} bytes", ITERS, ow_allocs, ow_bytes);
    println!();

    if zc_allocs == 0 {
        println!("zero_copy: 0 heap allocs -- WikiEdit<'a> borrows &str slices from the raw buffer");
    } else {
        println!("zero_copy: {} allocs ({:.3}/iter)", zc_allocs, zc_allocs as f64 / ITERS as f64);
    }
    println!("owned:     {:.1} allocs/iter -- each String field heap-allocates separately",
        ow_allocs as f64 / ITERS as f64);
    if ow_allocs > 0 {
        let reduction = (1.0 - zc_allocs as f64 / ow_allocs as f64) * 100.0;
        println!("reduction vs owned: {:.0}%", reduction);
    }
}
