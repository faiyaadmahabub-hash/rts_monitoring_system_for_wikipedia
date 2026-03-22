// main.rs — CLI entry point for Wiki-RTS monitoring engine.

use wiki_rts::allocator::CountingAllocator;
use wiki_rts::ingestion::sse_client::SseState;
use wiki_rts::models::*;
use wiki_rts::metrics::collector::MetricsCollector;
use wiki_rts::{ingestion, metrics, dashboard};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// Entry point: parses CLI args, boots the selected pipeline, generates final report.
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config = parse_args(&args);

    if let Some((ref file1, ref file2)) = config.compare_files {
        metrics::comparison::compare_runs(file1, file2);
        return;
    }

    print_boot(&config);
    let metrics_collector = MetricsCollector::new(config.channel_capacity);
    let sse_state = SseState::new();

    match config.mode {
        PipelineMode::Async => run_async_mode(config.clone(), metrics_collector.clone(), sse_state.clone()),
        PipelineMode::Threaded => run_threaded_mode(config.clone(), metrics_collector.clone(), sse_state.clone()),
    }

    println!("\n[STOP] Duration complete. Generating report...\n");
    metrics::report::print_final_report(config.mode, config.duration_secs, &metrics_collector);

    let json = metrics::report::export_json(config.mode, config.duration_secs, &metrics_collector);
    let _ = std::fs::create_dir_all("logs");
    let filename = format!("logs/run_{}.json", config.mode.to_string().to_lowercase());
    match std::fs::write(&filename, &json) {
        Ok(()) => println!("\nReport saved to: {}", filename),
        Err(e) => eprintln!("Failed to write report: {}", e),
    }
}

/// Launches the Tokio async runtime, spawns dashboard + pipeline tasks.
fn run_async_mode(
    config: AppConfig,
    metrics: MetricsCollector,
    sse_state: std::sync::Arc<SseState>,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");

    rt.block_on(async {
        let dash_state = sse_state.clone();
        let dash_metrics = metrics.clone();
        let dash_mode = config.mode;
        let dash_dur = config.duration_secs;
        let dash_faults = config.faults_enabled;
        let dash_handle = tokio::spawn(async move {
            dashboard::terminal::run_dashboard_async(
                dash_state, dash_metrics, dash_mode, dash_dur, dash_faults,
            ).await;
        });

        ingestion::async_pipeline::run(config, metrics, sse_state).await;
        let _ = dash_handle.await;
    });
}

/// Launches OS threads for dashboard + pipeline (std::thread concurrency model).
fn run_threaded_mode(
    config: AppConfig,
    metrics: MetricsCollector,
    sse_state: std::sync::Arc<SseState>,
) {
    let dash_state = sse_state.clone();
    let dash_metrics = metrics.clone();
    let dash_mode = config.mode;
    let dash_dur = config.duration_secs;
    let dash_faults = config.faults_enabled;
    let dash_thread = std::thread::Builder::new()
        .name("dashboard".into())
        .spawn(move || {
            dashboard::terminal::run_dashboard_threaded(
                dash_state, dash_metrics, dash_mode, dash_dur, dash_faults,
            );
        })
        .expect("Failed to spawn dashboard thread");

    ingestion::threaded_pipeline::run(config, metrics, sse_state);
    let _ = dash_thread.join();
}

/// Prints boot configuration summary to stderr.
fn print_boot(config: &AppConfig) {
    eprintln!("[BOOT] Wiki-RTS v1.0 | Mode: {} | Duration: {}s", config.mode, config.duration_secs);
    eprintln!("[BOOT] Channel: bounded({}) | Priority: 5-tier RMS (2/3/5/8/10ms deadlines)",
        config.channel_capacity);
    eprintln!("[BOOT] Degradation: 4-stage hysteresis (degrade>{:.1}ms, recover<{:.1}ms, stable {}s)",
        config.degrade_threshold_us as f64 / 1000.0,
        config.recover_threshold_us as f64 / 1000.0,
        config.stability_window_secs);
    eprintln!("[BOOT] Faults: {} | Leaderboards: Atomic+Mutex+RwLock | Heap counter: ACTIVE",
        if config.faults_enabled { "network@20s, cpu@40s, flood@55s" } else { "disabled" });
    eprintln!("[BOOT] Connecting to stream.wikimedia.org...\n");
}

/// Parses command-line arguments into AppConfig.
fn parse_args(args: &[String]) -> AppConfig {
    let mut config = AppConfig::default();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                i += 1;
                if i < args.len() {
                    config.mode = match args[i].as_str() {
                        "async" => PipelineMode::Async,
                        "threaded" => PipelineMode::Threaded,
                        other => {
                            eprintln!("Unknown mode '{}'. Use 'async' or 'threaded'.", other);
                            std::process::exit(1);
                        }
                    };
                }
            }
            "--duration" => {
                i += 1;
                if i < args.len() {
                    config.duration_secs = args[i].parse().unwrap_or(60);
                }
            }
            "--faults" => {
                i += 1;
                if i < args.len() {
                    config.faults_enabled = args[i] != "none";
                }
            }
            "--channel-capacity" => {
                i += 1;
                if i < args.len() {
                    config.channel_capacity = args[i].parse().unwrap_or(100);
                }
            }
            "--degrade-threshold" => {
                i += 1;
                if i < args.len() {
                    let ms: f64 = args[i].parse().unwrap_or(3.0);
                    config.degrade_threshold_us = (ms * 1000.0) as u64;
                }
            }
            "--recover-threshold" => {
                i += 1;
                if i < args.len() {
                    let ms: f64 = args[i].parse().unwrap_or(2.0);
                    config.recover_threshold_us = (ms * 1000.0) as u64;
                }
            }
            "--stability-window" => {
                i += 1;
                if i < args.len() {
                    config.stability_window_secs = args[i].parse().unwrap_or(5);
                }
            }
            "--compare" => {
                if i + 2 < args.len() {
                    config.compare_files = Some((args[i + 1].clone(), args[i + 2].clone()));
                    i += 2;
                } else {
                    eprintln!("--compare requires two file paths");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }
    config
}

/// Prints CLI usage instructions.
fn print_usage() {
    println!("Wiki-RTS — Real-time Wikipedia Edit Monitoring Engine\n");
    println!("USAGE:");
    println!("  wiki-rts --mode <async|threaded> [OPTIONS]");
    println!("  wiki-rts --compare <file1.json> <file2.json>\n");
    println!("OPTIONS:");
    println!("  --mode <async|threaded>       Pipeline concurrency model");
    println!("  --duration <seconds>          Run duration (default: 60)");
    println!("  --faults <default|none>       Fault injection schedule");
    println!("  --channel-capacity <n>        Bounded channel size (default: 100)");
    println!("  --degrade-threshold <ms>      p99 jitter to trigger degradation (default: 3.0)");
    println!("  --recover-threshold <ms>      p99 jitter to start recovery (default: 2.0)");
    println!("  --stability-window <secs>     Stable time before recovery (default: 5)");
    println!("  --compare <f1> <f2>           Compare two JSON run reports");
}
