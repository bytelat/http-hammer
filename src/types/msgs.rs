

#[derive(Debug, Clone)]
pub struct MsgRequest {
    pub opcode: String,
    pub body_index: usize,
    pub request_id: usize,
    pub enqueue_time: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct MsgResponse {
    pub opcode: String,
    pub worker_id: usize,
    pub status: String,
}

pub struct MsgStats {
    pub opcode: String,
    pub worker_id: usize,
    pub histogram: hdrhistogram::Histogram<u64>,
}

