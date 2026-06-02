#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOpcode {
    Request,
    Ping,
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct MsgRequest {
    pub opcode: RequestOpcode,
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
    #[allow(dead_code)]
    pub opcode: String,
    pub worker_id: usize,
    pub body: MsgBody,
}

pub enum MsgBody {
    Worker(WorkerMsg),
    Metrics(MetricsMsg),
}

pub struct WorkerMsg {
    pub free_conns: usize,
    pub histogram_request: hdrhistogram::Histogram<u64>,
    pub histogram_ping: hdrhistogram::Histogram<u64>,
}

pub struct MetricsMsg {
    pub running: f64,
    pub waiting: f64,

    pub kv_cache_frac: f64,
    pub kv_cache_pct: f64,

    pub prefix_hits: f64,
    pub prefix_queries: f64,
    pub prefix_hit_rate: f64,

    pub prompt_tokens_total: f64,
    pub gen_tokens_total: f64,

    pub prompt_tps: f64,
    pub gen_tps: f64,

    pub ttft_avg: f64,
    pub e2e_avg: f64,
}
