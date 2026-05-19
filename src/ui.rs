// src/ui.rs


use ncurses::*;
use core::num;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};
use crossbeam_channel::{select, Receiver};
use chrono::Local;
//use types::config::RunTimeSettings;
use crate::types::{Config, MsgStats, WorkerStats, LocalStats, RuntimeSettings};

pub fn draw_ui(
    local_stats: &[LocalStats],
    worker_stats: &[Arc<WorkerStats>],
    global_stats: &LocalStats,
    start_time: Instant,
) {
    clear();

    let now = chrono::Local::now();
    let time_str = now.format("%H:%M:%S").to_string();
    let uptime = start_time.elapsed().as_secs_f32();
    // Line 0: Time + Uptime
    mvprintw(
        0,
        0,
        &format!(
          "Time: {}    Uptime: {:.1}s",
          time_str,
         uptime
        ),
    );
    mvprintw(
        1,
        0,
        &format!(
            "Controls: [q] quit    [+] increase load    [-] decrease load    [ ] (reserved)"
        )
    );

// Separator
    mvprintw(2, 0, "---------------------------------------------------------------------------------------------------");
    // ------------------------------------------------------------
    // Table 1: Per‑Worker Stats (ASCII)
    // ------------------------------------------------------------
    mvprintw(2, 0,  "+--------------------------------------------------------------------------------------------------+");
    mvprintw(3, 0,  "|                     Per-Worker Latency & Counters                                                |");
    mvprintw(4, 0,  "+--------+---------+---------+---------+---------+---------+---------+---------+---------+---------+");
    mvprintw(5, 0,  "| WID    | p50     | p55     | p90     | p99     |  max    | sent/s  | recv/s  |  OK     | ERR     |");
    mvprintw(6, 0,  "+--------+---------+---------+---------+---------+---------+---------+---------+---------+---------+");

    let mut row = 7;

    for (i, ls) in local_stats.iter().enumerate() {
        let ws = &worker_stats[i];

        let sent_rps  = ws.sent_rps.load(Ordering::Relaxed);
        let recv_rps  = ws.recv_rps.load(Ordering::Relaxed);
        let total_ok  = ws.total_ok.load(Ordering::Relaxed);
        let total_err = ws.total_err.load(Ordering::Relaxed);

        mvprintw(
            row,
            0,
            &format!(
                "| {:<6} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} |",
                ls.worker_id,
                ls.p50,
                ls.p55,
                ls.p90,
                ls.p99,
                ls.max,
                sent_rps,
                recv_rps,
                total_ok,
                total_err,
            ),
        );

        row += 1;
    }

    mvprintw(row, 0, "+--------+---------+---------+---------+---------+---------+---------+---------+---------+---------+");

    // ------------------------------------------------------------
    // Table 2: Global Stats (ASCII)
    // ------------------------------------------------------------
    row += 2;
    mvprintw(row, 0, "+----------------------------------------------------------------------------------------+");
    row += 1;
    mvprintw(row, 0, "|                         Global Latency & Counters                                      |");
    row += 1;
    mvprintw(row, 0, "+--------+---------+---------+---------+---------+---------+---------+---------+---------+");
    row += 1;
    mvprintw(row, 0, "| p50    | p55     | p90     | p99     |  max    | sent/s  | recv/s  |   OK    |  ERR    |");
    row += 1;
    mvprintw(row, 0, "+--------+---------+---------+---------+---------+---------+---------+---------+---------+");

    // aggregate counters
    let mut g_sent_rps = 0;
    let mut g_recv_rps = 0;
    let mut g_ok = 0;
    let mut g_err = 0;

    for ws in worker_stats {
        g_sent_rps += ws.sent_rps.load(Ordering::Relaxed);
        g_recv_rps += ws.recv_rps.load(Ordering::Relaxed);
        g_ok       += ws.total_ok.load(Ordering::Relaxed);
        g_err      += ws.total_err.load(Ordering::Relaxed);
    }

    row += 1;
    mvprintw(
        row,
        0,
        &format!(
            "| {:<6} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7} |",
            global_stats.p50,
            global_stats.p55,
            global_stats.p90,
            global_stats.p99,
            global_stats.max,
            g_sent_rps,
            g_recv_rps,
            g_ok,
            g_err,
        ),
    );

    row += 1;
    mvprintw(row, 0, "+--------+---------+---------+---------+---------+---------+---------+---------+---------+");

    refresh();
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

pub fn run_ui(rx: Receiver<MsgStats>, worker_stats: Vec<Arc<WorkerStats>>, settings: Arc<RuntimeSettings>) {
    let start_time = Instant::now();   // <‑‑ record test start
    let mut last_update = Instant::now();
    let num_workers = worker_stats.len();
    let (mut local_stats, mut global_stats) = init_local_stats(num_workers);

    initscr();
    noecho();
    curs_set(CURSOR_VISIBILITY::CURSOR_INVISIBLE);
    timeout(50);

    loop {
        select! {
            recv(rx) -> msg => {
                if let Ok(msg) = msg {
                    let id = msg.worker_id;

                    // Merge worker histogram into UI's histogram
                    let ls = &mut local_stats[id];
                    ls.histogram = msg.histogram.clone();
                    // Compute percentiles
                    ls.p50 = ls.histogram.value_at_quantile(0.50);
                    ls.p55 = ls.histogram.value_at_quantile(0.55);
                    ls.p90 = ls.histogram.value_at_quantile(0.90);
                    ls.p99 = ls.histogram.value_at_quantile(0.99);
                    ls.max = ls.histogram.max();
                    // Store them (non-atomic since UI thread owns them)
                    global_stats.histogram.add(&msg.histogram).unwrap();
                    global_stats.p50 = global_stats.histogram.value_at_quantile(0.50);
                    global_stats.p55 = global_stats.histogram.value_at_quantile(0.55);
                    global_stats.p90 = global_stats.histogram.value_at_quantile(0.90);
                    global_stats.p99 = global_stats.histogram.value_at_quantile(0.99);
                    global_stats.max = global_stats.histogram.max();
                } else {
                    // Channel closed, likely workers have exited. We can choose to exit or just continue showing the last stats.
                    // For now, we'll just break the loop and exit the UI.
                    break;
                }
            }

            default(Duration::from_secs(1)) => {
                draw_ui(&local_stats, &worker_stats, &global_stats, start_time);
                last_update = Instant::now();
            }
        }

        if last_update.elapsed().as_secs() >= 1 {
            draw_ui(&local_stats, &worker_stats, &global_stats, start_time);
            last_update = Instant::now();
        }

        match getch() {
            113 => { endwin(); std::process::exit(0); }
            43 => { // '+' key pressed - increase load
                settings.inc_rps(1);
            }
            45 => { // '-' key pressed - decrease load
                settings.dec_rps(1);
            }
            _ => {}
        }
    }
}



pub fn start_ui_thread(
    rx_stats: Receiver<MsgStats>,
    stats: Vec<Arc<WorkerStats>>,
    settings: Arc<RuntimeSettings>,
    //cli_opts: Arc<CliOptions>,
    config: Arc<Config>
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // Initialize ncurses
        run_ui(rx_stats, stats, settings);
    })
}