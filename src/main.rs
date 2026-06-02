mod injector;
mod metrics;
mod types;
mod ui;
mod workers;

use metrics::MetricsCollector;
use types::{
    CliOptions, Config, MsgRequest, RequestOpcode, RuntimeSettings, WorkerConfig, WorkerStats,
};

use arrow2::array::Utf8Array;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{self, FileReader, read_metadata};
use crossbeam_channel::unbounded;
use flexi_logger::{FileSpec, Logger, WriteMode};
use memmap2::MmapOptions;
use std::fs::File;
use std::io::{self, Write};
use std::time::{Duration, Instant};
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use crate::types::MsgStats;

fn build_url(host: &str, path: &str) -> String {
    format!("http://{}{}", host, path)
}

fn inject_template(mut body: String, template: &str) -> String {
    if let Some(pos) = body.rfind('}') {
        body.reserve(template.len() + 2);
        body.insert_str(pos, ",\n");
        body.insert_str(pos + 2, template);
    }
    body
}

fn load_dataset_file(
    file_path: Option<String>,
    template: &str,
    model: &str,
) -> anyhow::Result<Vec<String>> {
    // 1. Open file with mmap
    let path = file_path.ok_or_else(|| anyhow::anyhow!("file_path is None"))?;
    let file = File::open(&path)?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let mut reader = std::io::Cursor::new(&mmap[..]);

    // 2. Read Parquet metadata
    let metadata = read_metadata(&mut reader)?;
    let total_row_groups = metadata.row_groups.len();
    print!(
        "\r\x1b[2KLoading dataset | row group 0/{} | 0/{} requests",
        total_row_groups, metadata.num_rows
    );
    io::stdout().flush()?;
    //    println!("Total rows: {}", metadata.num_rows);
    //    println!("Row groups: {}", metadata.row_groups.len());

    // 3. Convert Parquet schema → Arrow schema
    let arrow_fields = read::schema::parquet_to_arrow_schema(metadata.schema().fields());
    let schema = Schema::from(arrow_fields);
    // Find the messages column index ONCE
    let messages_col_idx = schema
        .fields
        .iter()
        .position(|f| f.name == "messages")
        .expect("messages column not found");

    let mut http_requests: Vec<String> = Vec::with_capacity(metadata.num_rows as usize);

    // Print schema so you know the real column order
    /*
    println!("\n=== Schema ===");
    for (i, f) in schema.fields.iter().enumerate() {
        println!("{}: {} ({:?})", i, f.name, f.data_type);
    }
    println!("{:#?}", schema);
    */
    // 4. Read row groups
    let mut file_reader = FileReader::new(
        reader,
        metadata.row_groups.clone(),
        schema.clone(),
        None,
        None,
        None,
    );

    //let start = Instant::now();
    // 6. Iterate over row groups
    let mut last_progress = Instant::now();
    let mut spinner_idx = 0;
    let spinner = ['|', '/', '-', '\\'];
    for (row_group_idx, maybe_chunk) in file_reader.by_ref().enumerate() {
        let chunk = maybe_chunk?;

        // Get the messages column once per chunk
        let col = &chunk.columns()[messages_col_idx];

        let arr = col.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        // 7. Iterate rows
        for row in 0..chunk.len() {
            let msg = arr.value(row); // &str, no allocation

            // Build HTTP body
            let base_body = format!(r#"{{"model":"{}","messages":{}}}"#, model, msg);

            http_requests.push(inject_template(base_body, template));

            if last_progress.elapsed() >= Duration::from_millis(200) {
                spinner_idx = (spinner_idx + 1) % spinner.len();
                print!(
                    "\r\x1b[2KLoading dataset {} row group {}/{} | {}/{} requests",
                    spinner[spinner_idx],
                    row_group_idx + 1,
                    total_row_groups,
                    http_requests.len(),
                    metadata.num_rows
                );
                io::stdout().flush()?;
                last_progress = Instant::now();
            }
        }

        print!(
            "\r\x1b[2KLoading dataset {} row group {}/{} | {}/{} requests",
            spinner[spinner_idx],
            row_group_idx + 1,
            total_row_groups,
            http_requests.len(),
            metadata.num_rows
        );
        io::stdout().flush()?;
    }
    //let elapsed = start.elapsed();

    //   println!("Extracted {} HTTP messages", http_requests.len());
    //   println!("Total time: {:.2?}", elapsed);

    Ok(http_requests)
}

fn parse_cli_args() -> anyhow::Result<CliOptions> {
    let mut args = env::args().skip(1);
    let mut rps = None;
    let mut file = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                eprintln!(
                    "Usage: http-hammer [-r <rps>] [-f <file>]\n\nOptions:\n  -r <rps>    Set requests-per-second\n  -f <file>   Set requests file path\n  -h, --help  Show this help message"
                );
                std::process::exit(0);
            }
            "-r" => {
                let rps_str = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("-r flag requires a value"))?;
                rps = Some(
                    rps_str
                        .parse::<usize>()
                        .map_err(|e| anyhow::anyhow!("Invalid rps value '{}': {}", rps_str, e))?,
                );
            }
            "-f" => {
                let file_path = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("-f flag requires a value"))?;
                file = Some(file_path);
            }

            other => {
                anyhow::bail!("Unknown argument: {}", other);
            }
        }
    }

    Ok(CliOptions { rps, file })
}

fn json_object_to_fragment(obj: &serde_json::Value) -> String {
    let map = obj.as_object().unwrap();
    map.iter()
        .map(|(k, v)| format!("\"{}\": {}", k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_cfg() -> anyhow::Result<Config> {
    let mut cfg = Config::load("config.json")?;

    cfg.template_str = json_object_to_fragment(&cfg.template);
    // Pick the first upstream for now

    Ok(cfg)
}

pub fn init_stats(num_workers: usize) -> Vec<Arc<WorkerStats>> {
    let mut v = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        v.push(Arc::new(WorkerStats {
            total_sent: AtomicU64::new(0),
            total_ok: AtomicU64::new(0),
            total_err: AtomicU64::new(0),
            sent_rps: AtomicU64::new(0),
            recv_rps: AtomicU64::new(0),
        }));
    }

    v
}

fn main() -> anyhow::Result<()> {
    let cli_opts = parse_cli_args()?;
    let cfg = load_cfg()?;
    Logger::try_with_str(cfg.log_level.as_str())?
        .log_to_file(FileSpec::default().directory("logs"))
        .write_mode(WriteMode::BufferAndFlush)
        .start()?;
    let file_label = cli_opts.file.as_deref().unwrap_or("<none>");
    print!("Loading dataset: {} ...", file_label);
    io::stdout().flush()?;
    let http_requests = Arc::new(load_dataset_file(
        cli_opts.file.clone(),
        &cfg.template_str,
        &cfg.model,
    )?);
    println!(
        "\r\x1b[2KLoaded dataset: {} requests from {}",
        http_requests.len(),
        file_label
    );
    std::thread::sleep(Duration::from_millis(750));
    // Wrap CLI options + config in Arc so they can be shared
    let cli_opts = Arc::new(cli_opts);
    let cfg = Arc::new(cfg);

    // Create the shared runtime settings
    let settings = Arc::new(RuntimeSettings::new(cli_opts.rps.unwrap_or(0) as u64));
    let cont = Arc::new(AtomicBool::new(true));

    // === Initialize stats and settings ===
    //let stats = Arc::new(RwLock::new(Stats::default()));

    //settings.rps = cli_opts.rps.unwrap_or(0);

    // === Create a separate channel for each worker ===
    let mut req_senders = Vec::new(); // main → workers
    //let mut resp_receivers = Vec::new(); // workers → main

    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::new();

    let (tx_stats, rx_stats) = unbounded::<MsgStats>();
    let stats = init_stats(cfg.concurrency);
    //  let (metrics_tx, metrics_rx) = std::sync::mpsc::channel();
    // let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    for worker_id in 0..cfg.concurrency {
        // One request channel per worker
        let (tx_req, rx_req) = unbounded::<MsgRequest>(); // unique channel for each worker
        req_senders.push(tx_req);

        // One response channel per worker

        let worker_specific = WorkerConfig {
            worker_id: worker_id,
            cfg: Arc::clone(&cfg),
            rx_req,
            tx_stats_q: tx_stats.clone(),
            http_requests: http_requests.clone(),
            stats: Arc::clone(&stats[worker_id]),
        };

        let handle = std::thread::spawn(move || {
            workers::worker_loop(worker_specific);
        });

        handles.push(handle);
    }
    let collector = MetricsCollector::new(
        cfg.concurrency,
        tx_stats.clone(),
        // shutdown_rx,
        build_url(&cfg.upstreams[0], &cfg.routes.metrics), //"http://localhost:8007/metrics",
        Arc::clone(&cont),
    );

    let metrics_thread = collector.start();

    let ui_handle = ui::start_ui_thread(
        rx_stats,
        stats.clone(),
        settings.clone(),
        //cli_opts.clone(),
        Arc::clone(&cfg),
    );
    let thread_cont = Arc::clone(&cont);
    let injector_handle = injector::start_injector_thread(
        thread_cont,
        req_senders.clone(),
        //resp_receivers.clone(),
        // stats.clone(),
        settings.clone(),
        //cli_opts.clone(),
        http_requests.clone(),
    )?;

    ui_handle.join().unwrap();
    cont.store(false, std::sync::atomic::Ordering::Relaxed); // signal injector to shutdown
    println!("Main thread: waiting for injector to shutdown...");
    //let _ = shutdown_tx.send(());

    injector_handle.join().unwrap();
    metrics_thread.join().unwrap();

    for tx in req_senders {
        tx.send(MsgRequest {
            opcode: RequestOpcode::Shutdown,
            body_index: 0,
            _request_id: 0,
            enqueue_time: Instant::now(),
        })
        .ok(); // signal workers to shutdown
        //drop(tx); // close all worker channels to signal them to exit
    }

    for h in handles {
        h.join().unwrap();
    }

    println!("\n=== Simulation complete ===");
    //  stats.print();

    Ok(())
}
