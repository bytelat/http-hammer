use crate::types::{MsgRequest, RequestOpcode, RuntimeSettings};
use crossbeam_channel::Sender;
use std::sync::{Arc, atomic::AtomicBool};
use std::time::{Duration, Instant};

pub fn start_injector_thread(
    cont: Arc<AtomicBool>,
    req_sender: Vec<Sender<MsgRequest>>,
    //  _resp_receiver: Vec<Receiver<MsgResponse>>,
    //stats: Arc<RwLock<Stats>>,
    settings: Arc<RuntimeSettings>,
    //cli_opts: Arc<CliOptions>,
    http_requests: Arc<Vec<String>>,
) -> anyhow::Result<std::thread::JoinHandle<()>> {
    //  let http_requests = load_dataset_file(Some(file_path))?;
    let total = http_requests.len();
    println!(
        "Starting injector thread with dataset of {} HTTP messages",
        total
    );

    let handle = std::thread::spawn(move || {
        let mut current = 0;
        let mut next_send = Instant::now();
        let num_workers = req_sender.len();
        let mut next_ping = Instant::now() + Duration::from_secs(1);
        loop {
            if cont.load(std::sync::atomic::Ordering::Relaxed) == false {
                println!("Injector: received shutdown signal, exiting");
                return;
            }
            // 1. Read current RPS (updated by UI)
            let rps = settings.rps();

            // If RPS is zero, idle briefly
            if rps == 0 {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }

            let interval = Duration::from_nanos((1_000_000_000 / rps).max(1));
            let now = Instant::now();
            let active_total = settings.active_requests().min(total).max(1);

            // 2. Send request when it's time
            let mut sent_this_tick = 0;
            while now >= next_send && sent_this_tick < 1024 {
                let body_index = current % active_total;
                let _ = req_sender[current % num_workers].send(MsgRequest {
                    opcode: RequestOpcode::Request,
                    body_index,
                    _request_id: current,
                    enqueue_time: Instant::now(),
                });
                current = (current + 1) % active_total;

                next_send += interval;
                sent_this_tick += 1;
                //  print!("\rInjector: sent request {}/{} at RPS: {} ", current, total, rps);
            }

            // 3. Send one ping every second
            let now = Instant::now();
            if now >= next_ping {
                let _ = req_sender[0].send(MsgRequest {
                    opcode: RequestOpcode::Ping,
                    body_index: 0,
                    _request_id: 0,
                    enqueue_time: Instant::now(),
                });
                next_ping = now + Duration::from_secs(1);
            }

            // 4. Tiny sleep to avoid 100% CPU spin
            let sleep_for = next_send
                .saturating_duration_since(Instant::now())
                .min(Duration::from_micros(50));
            if !sleep_for.is_zero() {
                std::thread::sleep(sleep_for);
            }
        }
    });

    Ok(handle)
}
