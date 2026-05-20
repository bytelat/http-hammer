// src/ui.rs

//use chrono::Local;
//use core::num;
use crossbeam_channel::{Receiver};
//use ncurses::*;
use std::{
    io::stdout,
    sync::Arc,
    time::{Duration, Instant},
};
//use types::config::RunTimeSettings;
use crate::types::{Config, LocalStats, MsgStats, RuntimeSettings, WorkerStats};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    symbols,
    text::Span,
    widgets::{Block, Borders, Gauge, Row, Sparkline, Table},
};
use std::sync::atomic::{Ordering};

fn draw_ui(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    local_stats: &[LocalStats],
    worker_stats: &[Arc<WorkerStats>],
    global_stats: &LocalStats,
    p99_history: &[u64],
    start_time: Instant,
) {
    let now = chrono::Local::now();
    let uptime = start_time.elapsed().as_secs_f32();

    // aggregate global counters
    let g_sent_rps: u64 = worker_stats
        .iter()
        .map(|w| w.sent_rps.load(Ordering::Relaxed))
        .sum();
    let g_recv_rps: u64 = worker_stats
        .iter()
        .map(|w| w.recv_rps.load(Ordering::Relaxed))
        .sum();
    let g_ok: u64 = worker_stats
        .iter()
        .map(|w| w.total_ok.load(Ordering::Relaxed))
        .sum();
    let g_err: u64 = worker_stats
        .iter()
        .map(|w| w.total_err.load(Ordering::Relaxed))
        .sum();

    // choose a "max" for RPS gauge (tune as you like)
    let rps_max = 10_000u64.max(g_sent_rps);
    let rps_ratio = if rps_max == 0 {
        0.0
    } else {
        g_sent_rps as f64 / rps_max as f64
    };

    terminal
        .draw(|f| {
            let size = f.size();

            // top: header + controls
            let main_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(5),
                    Constraint::Length(5),
                    Constraint::Min(10),
                    Constraint::Length(7),
                ])
                .split(size);

            // HEADER
            let header_block = Block::default().borders(Borders::ALL).title(Span::styled(
                format!(
                    "Time: {}   Uptime: {:.1}s   Controls: [q] quit  [+] inc  [-] dec",
                    now.format("%H:%M:%S"),
                    uptime
                ),
                Style::default().fg(Color::Cyan),
            ));
            f.render_widget(header_block, main_chunks[0]);

            // RPS GAUGE
            let gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("RPS Load"))
                .gauge_style(
                    Style::default()
                        .fg(Color::Green)
                        .bg(Color::Black)
                        .add_modifier(ratatui::style::Modifier::BOLD),
                )
                .ratio(rps_ratio)
                .label(Span::styled(
                    format!("{} req/s", g_sent_rps),
                    Style::default().fg(Color::White),
                ));
            f.render_widget(gauge, main_chunks[1]);

            // LATENCY SPARKLINE (p99)
            let spark_data: Vec<u64> = if p99_history.is_empty() {
                vec![0]
            } else {
                p99_history.to_vec()
            };

            let spark_color = match global_stats.p99 {
                v if v < 50_000 => Color::Green,   // < 50ms
                v if v < 150_000 => Color::Yellow, // 50–150ms
                _ => Color::Red,                   // > 150ms
            };

            let spark = Sparkline::default()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Latency p99 Trend (µs)"),
                )
                .data(&spark_data)
                .style(Style::default().fg(spark_color))
                .bar_set(symbols::bar::NINE_LEVELS);
            f.render_widget(spark, main_chunks[2]);

            // PER-WORKER + GLOBAL tables stacked
            let table_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(7), Constraint::Length(7)])
                .split(main_chunks[3]);

            // PER-WORKER TABLE
            let worker_rows: Vec<Row> = local_stats
                .iter()
                .enumerate()
                .map(|(i, ls)| {
                    let ws = &worker_stats[i];

                    let p99_color = match ls.p99 {
                        v if v < 50_000 => Color::Green,
                        v if v < 150_000 => Color::Yellow,
                        _ => Color::Red,
                    };

                    Row::new(vec![
                        ls.worker_id.to_string(),
                        ls.p50.to_string(),
                        ls.p55.to_string(),
                        ls.p90.to_string(),
                        Span::styled(ls.p99.to_string(), Style::default().fg(p99_color))
                            .to_string(),
                        ls.max.to_string(),
                        ws.sent_rps.load(Ordering::Relaxed).to_string(),
                        ws.recv_rps.load(Ordering::Relaxed).to_string(),
                        ws.total_ok.load(Ordering::Relaxed).to_string(),
                        ws.total_err.load(Ordering::Relaxed).to_string(),
                    ])
                })
                .collect();

            let worker_table = Table::new(
                worker_rows,
                [
                    Constraint::Length(6),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(10),
                ],
            )
            .header(
                Row::new(vec![
                    "WID", "p50", "p55", "p90", "p99(µs)", "max(µs)", "sent/s", "recv/s", "OK",
                    "ERR",
                ])
                .style(Style::default().fg(Color::Cyan)),
            )
            .block(
                Block::default()
                    .title("Per-Worker Latency & Counters")
                    .borders(Borders::ALL),
            )
            .style(Style::default().fg(Color::White));
            f.render_widget(worker_table, table_chunks[0]);

            // GLOBAL TABLE
            let global_row = Row::new(vec![
                global_stats.p50.to_string(),
                global_stats.p55.to_string(),
                global_stats.p90.to_string(),
                global_stats.p99.to_string(),
                global_stats.max.to_string(),
                g_sent_rps.to_string(),
                g_recv_rps.to_string(),
                g_ok.to_string(),
                g_err.to_string(),
            ]);

            let global_table = Table::new(
                vec![global_row],
                [
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(12),
                ],
            )
            .header(
                Row::new(vec![
                    "p50", "p55", "p90", "p99(µs)", "max(µs)", "sent/s", "recv/s", "OK", "ERR",
                ])
                .style(Style::default().fg(Color::Cyan)),
            )
            .block(
                Block::default()
                    .title("Global Latency & Counters")
                    .borders(Borders::ALL),
            );
            f.render_widget(global_table, table_chunks[1]);

            // FOOTER (reserved for future controls/help)
            let footer_block = Block::default()
                .borders(Borders::ALL)
                .title("Footer / Extra Controls (reserved)");
            f.render_widget(footer_block, main_chunks[4]);
        })
        .unwrap();
}
pub fn init_local_stats(num_workers: usize) -> (Vec<LocalStats>, LocalStats) {
    let local_stats = (0..num_workers)
        .map(|id| LocalStats {
            worker_id: id,
            histogram: hdrhistogram::Histogram::<u64>::new(3).unwrap(),
            p50: 0,
            p55: 0,
            p90: 0,
            p99: 0,
            max: 0,
        })
        .collect::<Vec<_>>();

    let total_stats = LocalStats {
        worker_id: usize::MAX,
        histogram: hdrhistogram::Histogram::<u64>::new(3).unwrap(),
        p50: 0,
        p55: 0,
        p90: 0,
        p99: 0,
        max: 0,
    };
    (local_stats, total_stats)
}

pub fn run_ui(
    rx: Receiver<MsgStats>,
    worker_stats: Vec<Arc<WorkerStats>>,
    settings: Arc<RuntimeSettings>,
) {
    let start_time = Instant::now(); // <‑‑ record test start
    //let mut last_update = Instant::now();
    let num_workers = worker_stats.len();
    let (mut local_stats, mut global_stats) = init_local_stats(num_workers);

    // simple rolling buffer for p99 sparkline
    let mut p99_history: Vec<u64> = Vec::with_capacity(120);

    enable_raw_mode().unwrap();
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen).unwrap();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();

    terminal.clear().unwrap();

    let target_frame_time = Duration::from_millis(33); // ~30 FPS
    let mut last_frame = Instant::now();
     
    loop {
        // 1. Drain all stats messages immediately (non-blocking)
        while let Ok(msg) = rx.try_recv() {
            let id = msg.worker_id;

            let ls = &mut local_stats[id];
            ls.histogram = msg.histogram_request.clone();
            ls.p50 = ls.histogram.value_at_quantile(0.50);
            ls.p55 = ls.histogram.value_at_quantile(0.55);
            ls.p90 = ls.histogram.value_at_quantile(0.90);
            ls.p99 = ls.histogram.value_at_quantile(0.99);
            ls.max = ls.histogram.max();

            global_stats.histogram.add(&msg.histogram_request).unwrap();
            global_stats.p50 = global_stats.histogram.value_at_quantile(0.50);
            global_stats.p55 = global_stats.histogram.value_at_quantile(0.55);
            global_stats.p90 = global_stats.histogram.value_at_quantile(0.90);
            global_stats.p99 = global_stats.histogram.value_at_quantile(0.99);
            global_stats.max = global_stats.histogram.max();

            p99_history.push(global_stats.p99);
            if p99_history.len() > 120 {
                p99_history.remove(0);
            }
        }

        // 2. Handle keyboard input (non-blocking)
        if event::poll(Duration::from_millis(1)).unwrap() {
            if let Event::Key(key) = event::read().unwrap() {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('+') => settings.inc_rps(1),
                    KeyCode::Char('-') => settings.dec_rps(1),
                    _ => {}
                }
            }
        }

        // 3. Draw UI at 30 FPS
        if last_frame.elapsed() >= target_frame_time {
            draw_ui(
                &mut terminal,
                &local_stats,
                &worker_stats,
                &global_stats,
                &p99_history,
                start_time,
            );
            last_frame = Instant::now();
        }

        // 4. Tiny sleep to avoid 100% CPU
        std::thread::sleep(Duration::from_micros(200));
    }
    disable_raw_mode().unwrap();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).unwrap();
}

pub fn start_ui_thread(
    rx_stats: Receiver<MsgStats>,
    stats: Vec<Arc<WorkerStats>>,
    settings: Arc<RuntimeSettings>,
    //cli_opts: Arc<CliOptions>,
    _config: Arc<Config>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // Initialize ncurses
        run_ui(rx_stats, stats, settings);
    })
}
