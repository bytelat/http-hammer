//use std::net::TcpStream;
use std::time::Duration;
//use std::collections::HashMap;
use crossbeam_channel::Receiver;
use std::io::Write;
//use ncurses::FALSE;
use crate::types::worker::ConnPool;
use crate::types::{MsgBody, MsgRequest, MsgStats, RequestOpcode, Routes, WorkerConfig, WorkerMsg};
use log::{debug, error, info, trace};
//use socket2::Socket;
//use hdrhistogram::Histogram;

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    for line in headers.split(|b| *b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|b| *b == b':') else {
            continue;
        };
        let (name, value) = line.split_at(colon);

        if eq_ignore_ascii_case(trim_ascii(name), b"content-length") {
            return std::str::from_utf8(trim_ascii(&value[1..]))
                .ok()?
                .parse()
                .ok();
        }
    }
    None
}

pub fn parse_http_response(buf: &[u8]) -> Option<usize> {
    // 1. Find end of headers
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let body_start = header_end + 4;

    // 2. Extract headers
    let headers = &buf[..header_end];

    // 3. Find Content-Length
    let content_length = parse_content_length(headers)?;

    // 4. Compute how many body bytes we have
    let body_len = buf.len().saturating_sub(body_start);

    // DEBUG
    debug!(
        "PARSER DEBUG: total={} header_end={} body_start={} body_len={} content_length={}",
        buf.len(),
        header_end,
        body_start,
        body_len,
        content_length
    );

    // 5. Not enough body yet → incomplete
    if body_len < content_length {
        return None;
    }

    // 6. We have a full response
    let end = body_start + content_length;

    Some(end)
}

//
fn poll_channel(rx: &Receiver<MsgRequest>) -> Option<MsgRequest> {
    match rx.try_recv() {
        Ok(req) => Some(req),
        Err(crossbeam_channel::TryRecvError::Empty) => None,
        Err(_) => None, // channel closed
    }
}

fn handle_msg_ping(
    _worker_id: usize,
    _req: &MsgRequest,
    upstream_path: &String,
    upstream: &String,
) -> String {
    // Build simple HTTP GET request for ping
    format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Connection: keep-alive\r\n\
         \r\n",
        path = upstream_path,
        host = upstream,
    )
}

fn handle_msg_request(
    _worker_id: usize,
    req: &MsgRequest,
    http_requests: &Vec<String>,
    upstream_path: &String,
    upstream: &String,
) -> String {
    let body = &http_requests[req.body_index];
    let content_len = body.len();

    // Build HTTP/1.1 request using the path from config
    format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Content-Type: application/json\r\n\
         X-Request-Id: 77-770-AB\r\n\
         Content-Length: {len}\r\n\
         Connection: keep-alive\r\n\
         \r\n\
         {body}",
        path = upstream_path,
        host = upstream,
        len = content_len,
        body = body
    )
}

fn handle_message(
    worker_id: usize,
    req: &MsgRequest,
    http_requests: &Vec<String>,
    pool: &mut ConnPool,
    routs: &Routes,
) -> bool {
    if let Some(idx) = pool.alloc() {
        let slot = &mut pool.get_slot(idx);

        let request = match req.opcode {
            RequestOpcode::Request => {
                slot.set_opcode(RequestOpcode::Request);
                handle_msg_request(
                    worker_id,
                    req,
                    http_requests,
                    &routs.request,
                    slot.get_upstream(),
                )

                // proceed to build and send request
            }
            RequestOpcode::Ping => {
                slot.set_opcode(RequestOpcode::Ping);
                handle_msg_ping(worker_id, req, &routs.ping, slot.get_upstream())
            }
            RequestOpcode::Shutdown => {
                error!("Worker {worker_id}: shutdown cannot be sent upstream");
                pool.free(idx); // free the slot if we won't use it
                return false;
            }
        };
        slot.set_enqueue_time(req.enqueue_time);
        slot.set_send_time();

        // println!("Worker {worker_id}: sending request {}",  request);
        match slot.get_stream().write(request.as_bytes()) {
            Ok(_) => {
                //println!("Worker {worker_id}: request sent to {upstream}");
                return true;
            }
            Err(e) => {
                error!(
                    "Worker {worker_id}: failed to send request to {}: {}",
                    slot.get_upstream(),
                    e
                );
                pool.free(idx); // free the slot on error
                return false;
            }
        }
    } else {
        //println!("Worker {worker_id}: ALLOC FAILED — no free connections available");
        return false;
    }
}

pub fn worker_loop(worker_specific: WorkerConfig) {
    //println!("Worker {} started with upstreams: {:?}", worker_specific.worker_id, worker_specific.cfg.upstreams);
    info!(
        "Worker {} started with upstreams: {:?}",
        worker_specific.worker_id, worker_specific.cfg.upstreams
    );
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
    let mut hist_request = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    let mut hist_ping = hdrhistogram::Histogram::<u64>::new(3).unwrap();

    //let mut last_poll_end = std::time::Instant::now();
    loop {
        //let time_until_poll = last_poll_end.elapsed();
        // 1. Try to send a request ONLY if we don't have one pending
        if current_req.is_none() {
            current_req = poll_channel(&worker_specific.rx_req);
        }

        // 2. If we have a request, try to send it
        if let Some(req) = &current_req {
            match req.opcode {
                RequestOpcode::Shutdown => {
                    info!(
                        "Worker {}: received shutdown signal, exiting",
                        worker_specific.worker_id
                    );
                    return;
                }
                RequestOpcode::Request | RequestOpcode::Ping => {
                    if handle_message(
                        worker_specific.worker_id,
                        req,
                        &worker_specific.http_requests,
                        &mut pool,
                        &worker_specific.cfg.routes,
                    ) {
                        worker_specific
                            .stats
                            .total_sent
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        sent_count += 1;
                        current_req = None; // request sent successfully → now we can pull another
                    } else {
                        // Could not send → do NOT pull another request
                        // Just continue to polling sockets
                    }
                }
            }
        }

        // 3. Poll upstream sockets
        let before = pool.get_free_count();
        let resp = pool.poll_events(
            &mut buf,
            parse_http_response,
            &mut hist_request,
            &mut hist_ping,
        );
        //last_poll_end = std::time::Instant::now();
        let after = pool.get_free_count();

        worker_specific
            .stats
            .total_ok
            .fetch_add(resp as u64, std::sync::atomic::Ordering::Relaxed);
        if after > before {
            recv_count += (after - before) as u64;
        }

        // 4. Print stats
        if last_print.elapsed().as_secs() >= 1 {
            worker_specific
                .stats
                .sent_rps
                .store(sent_count, std::sync::atomic::Ordering::Relaxed);
            worker_specific
                .stats
                .recv_rps
                .store(recv_count, std::sync::atomic::Ordering::Relaxed);
            if worker_specific
                .tx_stats_q
                .send(MsgStats {
                    opcode: "worker_stats".to_string(),
                    worker_id: worker_specific.worker_id,
                    body: MsgBody::Worker(WorkerMsg {
                        free_conns: pool.get_free_count(),
                        histogram_request: hist_request.clone(),
                        histogram_ping: hist_ping.clone(),
                    }),
                })
                .is_err()
            {
                return;
            }
            hist_request.reset();
            hist_ping.reset();

            trace!(
                "Worker {} send_rate={} recv_rate={} free={}",
                worker_specific.worker_id,
                sent_count,
                recv_count,
                pool.get_free_count()
            );
            sent_count = 0;
            recv_count = 0;
            last_print = std::time::Instant::now();
        }

        // 5. Prevent busy spinning
        std::thread::sleep(Duration::from_micros(1));
    }
}

#[cfg(test)]
mod tests {
    use super::parse_http_response;

    #[test]
    fn parses_complete_response_length() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloextra";

        assert_eq!(parse_http_response(response), Some(43));
    }

    #[test]
    fn parses_content_length_case_insensitively() {
        let response = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";

        assert_eq!(parse_http_response(response), Some(response.len()));
    }

    #[test]
    fn waits_for_incomplete_body() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhel";

        assert_eq!(parse_http_response(response), None);
    }
}
