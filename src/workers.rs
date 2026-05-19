//use std::net::TcpStream;
use std::time::Duration;
//use std::collections::HashMap;
use std::io::{Read, Write};
use crossbeam_channel::Receiver;
//use ncurses::FALSE;
use crate::types::worker::ConnPool;
use crate::types::{MsgRequest, WorkerConfig, MsgStats};
//use socket2::Socket;
use hdrhistogram::Histogram;

pub fn parse_http_response(buf: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    // 1. Find end of headers
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let body_start = header_end + 4;

    // 2. Extract headers
    let headers = &buf[..header_end];
    let headers_str = String::from_utf8_lossy(headers);

    // 3. Find Content-Length
    let mut content_length = None;
    for line in headers_str.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("content-length:") {
            content_length = rest.trim().parse::<usize>().ok();
        }
    }

    let content_length = content_length?;

    // 4. Compute how many body bytes we have
    let body_len = buf.len().saturating_sub(body_start);

    // DEBUG
    /* 
    println!(
        "PARSER DEBUG: total={} header_end={} body_start={} body_len={} content_length={}",
        buf.len(),
        header_end,
        body_start,
        body_len,
        content_length
    );*/

    // 5. Not enough body yet → incomplete
    if body_len < content_length {
        return None;
    }

    // 6. We have a full response
    let end = body_start + content_length;
    let full = buf[..end].to_vec();
    let remaining = &buf[end..];

    Some((full, remaining))
}

//
fn poll_channel(rx: &Receiver<MsgRequest>) -> Option<MsgRequest> {
    match rx.try_recv() {
        Ok(req) => Some(req),
        Err(crossbeam_channel::TryRecvError::Empty) => None,
        Err(_) => None, // channel closed
    }
}

fn inject_template(mut body: String, template: &str) -> String {
    if let Some(pos) = body.rfind('}') {
        // Insert comma + template before the final }
        let insertion = format!(",\n{}", template);
        body.insert_str(pos, &insertion);
    }
    body
}

fn handle_request(
    worker_id: usize, 
    req: &MsgRequest,
    http_requests: &Vec<String>,
    pool: &mut ConnPool,
    upstream_path: &String,
    template: &String)  -> bool
{
   
    if let Some(idx) = pool.alloc()  {
        let slot = &mut pool.get_slot(idx);

        // Build HTTP request with template injection    
        let base = http_requests[req.body_index].clone();
        let body = inject_template(base, &template);     
        let content_len = body.len();

        // Build HTTP/1.1 request using the path from config
        let request = format!(
            "POST {path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: application/json\r\n\
             X-Request-Id: 77-770-AB\r\n\
             Content-Length: {len}\r\n\
             Connection: keep-alive\r\n\
             \r\n\
             {body}",
            path = upstream_path,
            host = slot.get_upstream(),
            len = content_len,
            body = body
        );
        slot.set_enqueue_time(req.enqueue_time);
        slot.set_send_time();

       // println!("Worker {worker_id}: sending request to {upstream} {}",  request);
        match slot.get_stream().write(request.as_bytes()) {
            Ok(_) => {
                //println!("Worker {worker_id}: request sent to {upstream}");
                return true;
            }
            Err(e) => {
                println!("Worker {worker_id}: failed to send request to {}: {}", slot.get_upstream(), e);
                pool.free(idx); // free the slot on error
                return false;
            }
        }
    } else {
        println!("Worker {worker_id}: ALLOC FAILED — no free connections available");
        return false;        
    }
     
}


pub fn worker_loop(worker_specific: WorkerConfig) 
{
    //println!("Worker {} started with upstreams: {:?}", worker_specific.worker_id, worker_specific.cfg.upstreams);

    // Build epoll-based connection pool
    let mut pool = ConnPool::new(
        &worker_specific.cfg.upstreams,
        worker_specific.cfg.keep_alive,
    );

   // Each worker owns its own connections
   // let mut conns = open_connections(worker_specific.worker_id, &worker_specific.cfg.upstreams, worker_specific.cfg.keep_alive);
   // let mut buffers: HashMap<String, Vec<u8>> = HashMap::new();

    let mut sent_count: u64 = 0;
    let mut recv_count: u64 = 0;
    let mut last_print = std::time::Instant::now();

    let mut buf = [0u8; 64 * 1024];

    let mut current_req: Option<MsgRequest> = None;
    let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    //let mut last_poll_end = std::time::Instant::now();
    loop {
        //let time_until_poll = last_poll_end.elapsed();
    // 1. Try to send a request ONLY if we don't have one pending
        if current_req.is_none() {
            current_req = poll_channel(&worker_specific.rx_req);
        }

        // 2. If we have a request, try to send it
        if let Some(req) = &current_req {
            if handle_request(
                worker_specific.worker_id,
                req,
                &worker_specific.http_requests,
                &mut pool,
                &worker_specific.cfg.upstream_path,
            &worker_specific.cfg.template_str){
                worker_specific.stats.total_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                sent_count += 1;
                current_req = None; // request sent successfully → now we can pull another
            } else {
                // Could not send → do NOT pull another request
                // Just continue to polling sockets
            }
        }

        
        // 3. Poll upstream sockets
        let before = pool.get_free_count();
        let resp =  pool.poll_events(&mut buf, parse_http_response, &mut hist);
        //last_poll_end = std::time::Instant::now(); 
        let after = pool.get_free_count();

        worker_specific.stats.total_ok.fetch_add(resp as u64, std::sync::atomic::Ordering::Relaxed);
        if after > before {
            recv_count += (after - before) as u64;
        }

        // 4. Print stats
        if last_print.elapsed().as_secs() >= 1 {
            worker_specific.stats.sent_rps.store(sent_count, std::sync::atomic::Ordering::Relaxed);
            worker_specific.stats.recv_rps.store(recv_count, std::sync::atomic::Ordering::Relaxed);
            worker_specific.tx_stats_q.send(MsgStats {
                opcode: "worker_stats".to_string(),
                worker_id: worker_specific.worker_id,
                histogram: hist.clone(),
            }).unwrap();
            hist.reset();
            /* 
            println!(
                "Worker {} send_rate={} recv_rate={} free={} time_until_poll={} ms",
                worker_specific.worker_id,
                sent_count,
                recv_count,
                pool.get_free_count(),
                time_until_poll.as_millis()
            );*/
            sent_count = 0;
            recv_count = 0;
            last_print = std::time::Instant::now();
        }

        // 5. Prevent busy spinning
        std::thread::sleep(Duration::from_micros(1));
    }
}