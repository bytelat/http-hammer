use std::sync::atomic::{AtomicU64};

#[repr(align(64))]
pub struct WorkerStats {
    pub total_sent: AtomicU64,
    pub total_ok: AtomicU64,
    pub total_err: AtomicU64,
    pub sent_rps: AtomicU64,
    pub recv_rps: AtomicU64,
}
pub struct LocalStats {
    pub worker_id: usize,

    // merged histogram from worker messages
    pub histogram: hdrhistogram::Histogram<u64>,

    // computed percentiles
    pub p50: u64,
    pub p55: u64,
    pub p90: u64,
    pub p99: u64,
    pub max: u64,   
}

/* 
#[derive(Debug, Clone)]
pub struct Settings {
    pub rps: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Settings { rps: 0 }
    }
}
*/
