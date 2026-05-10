//! ratatui terminal dashboard. Press 'q' to quit.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

use crate::metrics::percentile;
use crate::state::SharedState;
use crate::leaderboard;

pub fn start(state: Arc<SharedState>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        if run_dashboard(&state).is_err() {
            // Silent exit on terminal error so the pipeline threads keep running.
        }
    })
}

fn run_dashboard(state: &Arc<SharedState>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        terminal.draw(|f| render(f, state))?;

        // Poll for up to 200 ms. If no event arrives we re-render with fresh data.
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

fn render(f: &mut ratatui::Frame, state: &Arc<SharedState>) {
    let area = f.size();

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(10),   // packets table
            Constraint::Length(6), // drift stats + leaderboard
            Constraint::Length(3), // mode bar
            Constraint::Length(3), // counters bar
        ])
        .split(area);

    render_packets(f, state, rows[0]);
    render_middle(f, state, rows[1]);
    render_mode_bar(f, state, rows[2]);
    render_counters(f, state, rows[3]);
}

fn render_packets(
    f: &mut ratatui::Frame,
    state: &Arc<SharedState>,
    area: ratatui::layout::Rect,
) {
    let records = state.records.lock().unwrap();
    // Collect the last 8 records in chronological order.
    let tail: Vec<_> = records.iter().rev().take(8).rev().cloned().collect();
    // Drop the lock immediately — rendering does not need it.
    drop(records);

    let header_cells = ["domain", "user", "type", "drift", "exec", "total", "deadline"]
        .iter()
        .map(|h| {
            Cell::from(*h).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
        });
    let header = Row::new(header_cells).height(1).bottom_margin(0);

    let data_rows: Vec<Row> = tail
        .iter()
        .map(|r| {
            let type_str = if r.is_human { "HUMAN" } else { "BOT" };
            let deadline_str = if r.deadline_met() { "✓ MET" } else { "✗ MISS" };
            let deadline_style = if r.deadline_met() {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Red)
            };
            let type_style = if r.is_human {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::Magenta)
            };

            Row::new(vec![
                Cell::from(r.domain.clone()),
                Cell::from(r.user.clone()),
                Cell::from(type_str).style(type_style),
                Cell::from(format!("{:.2}ms", r.drift_ms())),
                Cell::from(format!("{:.2}ms", r.exec_ms())),
                Cell::from(format!("{:.2}ms", r.total_ms())),
                Cell::from(deadline_str).style(deadline_style),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(22),
        Constraint::Length(18),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(10),
    ];

    let table = Table::new(data_rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" live packets (last 8 rows) "),
        )
        .column_spacing(1);

    f.render_widget(table, area);
}

fn render_middle(
    f: &mut ratatui::Frame,
    state: &Arc<SharedState>,
    area: ratatui::layout::Rect,
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    render_drift(f, state, cols[0]);
    render_leaderboard(f, state, cols[1]);
}

fn render_drift(
    f: &mut ratatui::Frame,
    state: &Arc<SharedState>,
    area: ratatui::layout::Rect,
) {
    let records = state.records.lock().unwrap();
    let human_drift: Vec<f64> = records
        .iter()
        .filter(|r| r.is_human)
        .map(|r| r.drift_ms())
        .collect();
    let bot_drift: Vec<f64> = records
        .iter()
        .filter(|r| !r.is_human)
        .map(|r| r.drift_ms())
        .collect();
    drop(records);

    let h50 = percentile(human_drift.clone(), 50.0);
    let h90 = percentile(human_drift.clone(), 90.0);
    let h99 = percentile(human_drift, 99.0);

    let b50 = percentile(bot_drift.clone(), 50.0);
    let b90 = percentile(bot_drift.clone(), 90.0);
    let b99 = percentile(bot_drift, 99.0);

    let text = vec![
        Line::from(vec![Span::styled(
            format!("human p50={:.2}  p90={:.2}  p99={:.2}ms", h50, h90, h99),
            Style::default().fg(Color::Cyan),
        )]),
        Line::from(vec![Span::styled(
            format!("bot   p50={:.2}  p90={:.2}  p99={:.2}ms", b50, b90, b99),
            Style::default().fg(Color::Magenta),
        )]),
    ];

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" scheduling drift "),
    );

    f.render_widget(paragraph, area);
}

fn render_leaderboard(
    f: &mut ratatui::Frame,
    state: &Arc<SharedState>,
    area: ratatui::layout::Rect,
) {
    let top = leaderboard::top3(state);

    let lines: Vec<Line> = top
        .iter()
        .enumerate()
        .map(|(i, (domain, count))| {
            Line::from(vec![Span::styled(
                format!("#{} {:<26} {}", i + 1, domain, count),
                Style::default().fg(Color::Yellow),
            )])
        })
        .collect();

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" leaderboard "),
    );

    f.render_widget(paragraph, area);
}

fn render_mode_bar(
    f: &mut ratatui::Frame,
    state: &Arc<SharedState>,
    area: ratatui::layout::Rect,
) {
    let degraded     = state.degraded.load(Ordering::Relaxed);
    let reconnecting = state.reconnect_needed.load(Ordering::Relaxed);

    let (label, color) = if reconnecting {
        ("mode: WATCHDOG — reconnecting...", Color::Yellow)
    } else if degraded {
        ("mode: DEGRADED  (bot edits skipped)", Color::Red)
    } else {
        ("mode: NORMAL", Color::Green)
    };

    let paragraph = Paragraph::new(Line::from(vec![Span::styled(
        label,
        Style::default()
            .fg(color)
            .add_modifier(Modifier::BOLD),
    )]))
    .block(Block::default().borders(Borders::ALL));

    f.render_widget(paragraph, area);
}

fn render_counters(
    f: &mut ratatui::Frame,
    state: &Arc<SharedState>,
    area: ratatui::layout::Rect,
) {
    let processed = state.total_processed.load(Ordering::Relaxed);
    let missed    = state.total_missed.load(Ordering::Relaxed);
    let overflow  = state.overflow_count.load(Ordering::Relaxed);
    let override_ = state.override_count.load(Ordering::Relaxed);
    let resets    = state.reset_count.load(Ordering::Relaxed);

    let text = format!(
        "processed:{}  missed:{}  overflow:{}  override:{}  resets:{}",
        processed, missed, overflow, override_, resets
    );

    let paragraph = Paragraph::new(text).block(Block::default().borders(Borders::ALL));

    f.render_widget(paragraph, area);
}
