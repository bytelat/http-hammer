use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use super::msgs::{MsgRequest, MsgStats};
use crate::types::config::{Config};
use crate::types::stats::WorkerStats;
use std::net::TcpStream;
use std::os::fd::{RawFd, AsRawFd};
use std::io::{Read};
use nix::sys::socket::{setsockopt, sockopt::RcvBuf};


use nix::sys::epoll::{
    epoll_create1, epoll_ctl, epoll_wait,
    EpollCreateFlags, EpollEvent, EpollFlags, EpollOp
};


pub struct WorkerConfig {
    pub worker_id: usize,
    pub cfg: Arc<Config>,
    pub rx_req: Receiver<MsgRequest>,
    pub tx_stats_q: Sender<MsgStats>,
    pub http_requests: Arc<Vec<String>>,
    pub stats: Arc<WorkerStats>, 
}

pub struct ConnSlot {
    upstream: String,
    opcode: String,
    stream: TcpStream,
    buffer: Vec<u8>,
    busy: bool,
    enqueue_time: Option<std::time::Instant>,
    send_time: Option<std::time::Instant>,
    recv_time: Option<std::time::Instant>,
}

pub struct ConnPool {
    conns: Vec<ConnSlot>,
    free: Vec<usize>,     // stack of free indices
    epoll_fd:RawFd,
}


impl ConnSlot {
    pub fn get_upstream(&self) -> &String {
        &self.upstream
    }

    pub fn get_stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn set_enqueue_time(&mut self, time: std::time::Instant) {
        self.enqueue_time = Some(time);
    }

    pub fn set_send_time(&mut self) {
        self.send_time = Some(std::time::Instant::now());
    }

    pub fn set_recv_time(&mut self ) {
        self.recv_time = Some(std::time::Instant::now());
    }

    pub fn set_opcode(&mut self, opcode: String) {
        self.opcode = opcode;
    }

    pub fn get_opcode(&self) -> &String {
        &self.opcode
    }
}

impl ConnPool {
    pub fn new(upstreams: &[String], keep_alive: usize) -> Self {
        let epoll_fd = epoll_create1(EpollCreateFlags::EPOLL_CLOEXEC)
        .expect("epoll_create1 failed");

    let mut conns = Vec::new();
    let mut idx = 0;

    for up in upstreams {
        for _ in 0..keep_alive {
            let stream = TcpStream::connect(up).expect("connect failed");
            stream.set_nonblocking(true).unwrap();

            let fd = stream.as_raw_fd();
            let _ = setsockopt(fd, RcvBuf, & (1024 * 1024) );

            let mut event = EpollEvent::new(
                EpollFlags::EPOLLIN,
                idx as u64,
            );

            epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, fd, &mut event)
                .expect("epoll_ctl ADD failed");

            conns.push(ConnSlot {
                upstream: up.clone(),
                stream,
                buffer: Vec::with_capacity(64 * 1024),
                busy: false,
                opcode: String::new(),
                enqueue_time: None.into(), 
                send_time: None.into(),
                recv_time: None.into(),
            });

            idx += 1;
        }
    }

    let free = (0..conns.len()).collect();

    Self { conns, free, epoll_fd }
    }

    /// Allocate a free connection (O(1))
    pub fn alloc(&mut self) -> Option<usize> {
        let idx = self.free.pop()?;
        self.conns[idx].busy = true;
        Some(idx)
    }

    /// Free a connection (O(1))
    pub fn free(&mut self, idx: usize) {
        let slot = &mut self.conns[idx];
        slot.buffer.clear();
        self.conns[idx].busy = false;
        self.free.push(idx);
    }

fn reset_slot(&mut self, idx: usize) {
        let upstream = self.conns[idx].upstream.clone();

        
        // create new stream
        let stream = TcpStream::connect(&upstream).expect("reconnect failed");
        stream.set_nonblocking(true).unwrap();

        let fd = stream.as_raw_fd();
        let _ = setsockopt(fd, RcvBuf, &(1024 * 1024));

        let mut event = EpollEvent::new(
            EpollFlags::EPOLLIN,
            idx as u64,
        );

        epoll_ctl(self.epoll_fd, EpollOp::EpollCtlAdd, fd, &mut event)
            .expect("epoll_ctl ADD failed on reconnect");

        // replace slot
        self.conns[idx].stream = stream;
        self.conns[idx].buffer.clear();
        self.conns[idx].busy = false;

        
    }

    /// Poll epoll for readable sockets
    pub fn poll_events(&mut self, 
                        buf: &mut [u8], 
                        parser: fn(&[u8]) -> Option<(Vec<u8>, &[u8])>, 
                        hist_request: &mut hdrhistogram::Histogram<u64>,
                        hist_ping: &mut hdrhistogram::Histogram<u64>
                        ) -> usize {
        let mut events = [EpollEvent::empty(); 128];

        let n = epoll_wait(self.epoll_fd, &mut events, 0)
            .expect("epoll_wait failed");

        let mut completed = 0;

        for i in 0..n {
            let idx = events[i].data() as usize;
            /*   
            println!(
                    "EPOLL EVENT: idx={} busy={} buffer_len={}",
                    idx,
                    self.conns[idx].busy,
                    self.conns[idx].buffer.len());
            */
            // read_from returns true when a full HTTP response is parsed
            let (done, closed) = self.read_from(idx, buf, parser, hist_request, hist_ping);
            match (done, closed) {
                (true, false) => {
                // normal keep-alive response
                    //println!("✓ RESPONSE COMPLETE on conn {}", idx);
                    self.free(idx);
                    completed += 1;
                }

                (true, true) => {
                // response complete but server closed (Connection: close)
                    self.reset_slot(idx);
                    self.free(idx);
                    completed += 1;
                }

                (false, true) => {
                // server closed early (truncated response)
                    //println!("✗ CLOSED EARLY on conn {}", idx);
                    self.reset_slot(idx);
                }

                (false, false) => {
                    // partial response, keep waiting
                    //println!("… partial response on conn {}", idx);
                }
            }

        }
        completed
    }

    /// Read from a specific connection
    fn read_from(&mut self, 
                idx: usize, 
                buf: &mut [u8], 
                parser: fn(&[u8]) -> Option<(Vec<u8>, &[u8])>,
                hist_request: &mut hdrhistogram::Histogram<u64>,
                hist_ping: &mut hdrhistogram::Histogram<u64>) -> (bool /*complete */, bool /* closed */) 
    {

        let slot = &mut self.conns[idx];
        let mut response_complete = false;
        let mut closed = false;


        loop {
            match slot.stream.read(buf) {
                Ok(0) => {
                    closed = true;
                    break;
                },
                Ok(n) => {
                    slot.buffer.extend_from_slice(&buf[..n]);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock =>  {
                    break;
                },
                Err(_) => {
                    return (false, true);
                }
            }
        }

        if slot.buffer.is_empty() {
            return (false, closed);
        }
        loop {
            match parser(&slot.buffer) {
                Some((_, remaining)) => {
                // full response parsed
                    //slot.set_recv_time();
                    response_complete = true;
                    slot.buffer = remaining.to_vec();
                    break;
                }
                None => {
                    println!("PARTIAL BUFFER ({} bytes):\n\n---",
                            slot.buffer.len());
                            //String::from_utf8_lossy(&self.conns[idx].buffer));    
                    break;
                }
            }
        }
        if response_complete {
            slot.set_recv_time();
            match slot.get_opcode().as_str() {
                "request" => { 
                    hist_request.record(slot.recv_time.unwrap().duration_since(slot.send_time.unwrap()).as_millis() as u64).unwrap();
                }
                "ping" => {
                    hist_ping.record(slot.recv_time.unwrap().duration_since(slot.send_time.unwrap()).as_millis() as u64).unwrap();
                }
                _ => (),
            }
        }
        (response_complete, closed)
    }


    pub fn get_slot(&mut self, idx: usize) -> &mut ConnSlot {
        &mut self.conns[idx]
    }

    pub fn get_free_count(&self) -> usize {
        self.free.len()
    }
}