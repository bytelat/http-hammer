

#[derive(Debug, Clone)]
pub struct MsgRequest {
    pub opcode: String,
    pub body_index: usize,
    pub _request_id: usize,
    pub enqueue_time: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct _MsgResponse {
    pub opcode: String,
    pub worker_id: usize,
    pub status: String,
}

pub struct MsgStats {
    pub opcode: String,
    pub worker_id: usize,
    pub free_conns: usize,
    pub histogram_request: hdrhistogram::Histogram<u64>,
    pub histogram_ping: hdrhistogram::Histogram<u64>,
}

