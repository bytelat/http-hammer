use std::sync::{Arc};
use std::time::{Duration, Instant};
use crossbeam_channel::{Sender, Receiver};
use crate::types::{RuntimeSettings, MsgRequest, MsgResponse};

pub fn start_injector_thread(
    req_sender: Vec<Sender<MsgRequest>>,
    _resp_receiver: Vec<Receiver<MsgResponse>>,
    //stats: Arc<RwLock<Stats>>,
    settings: Arc<RuntimeSettings>,
    //cli_opts: Arc<CliOptions>,
    http_requests: Arc<Vec<String>>
) -> anyhow::Result<std::thread::JoinHandle<()>> {
    
  //  let http_requests = load_dataset_file(Some(file_path))?;
    let total = http_requests.len();
    println!("Starting injector thread with dataset of {} HTTP messages", total);

    let handle = std::thread::spawn(move || {
        let mut current = 0;
        let mut next_send = Instant::now();
        let num_workers = req_sender.len();
        let mut next_ping = Instant::now() + Duration::from_secs(1);
        loop {
            // 1. Read current RPS (updated by UI)
            let rps = settings.rps();

            // If RPS is zero, idle briefly
            if rps == 0 {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }

            let interval = Duration::from_micros(1_000_000 / rps);
            let now = Instant::now();

            // 2. Send request when it's time
            if now >= next_send {
                 
                let _ = req_sender[ current % num_workers ].send(MsgRequest { 
                    opcode: "request".to_string(), 
                    body_index:current, 
                    request_id: current,
                    enqueue_time: Instant::now(),
                });
                current = (current + 1) % total;
               
                next_send = now + interval;
              //  print!("\rInjector: sent request {}/{} at RPS: {} ", current, total, rps);
            }

            // 3. Send one ping every second
            let now = Instant::now();
            if now >= next_ping {
                for n in 0..num_workers {
                    let _ = req_sender[n].send(MsgRequest {
                        opcode: "ping".to_string(),
                        body_index: 0,
                        request_id: 0,
                        enqueue_time: Instant::now(),
                    });
                }
                next_ping = now + Duration::from_secs(1);
            }
             
            // 4. Tiny sleep to avoid 100% CPU spin
            std::thread::sleep(Duration::from_micros(50));
        }
    });

    Ok(handle)
}
