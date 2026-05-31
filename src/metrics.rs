use crate::types::msgs::{MetricsMsg, MsgBody, MsgStats};
//use std::sync::mpsc::{Receiver, Sender};
use crossbeam_channel::Sender;
use std::sync::{Arc, atomic::AtomicBool};
use std::time::Duration;

pub struct MetricsCollector {
    worker_id: usize,
    stats_tx: Sender<MsgStats>,
    //shutdown_rx: Receiver<()>,
    route: String,
    interval: Duration,
    cont: Arc<AtomicBool>,
}

impl MetricsCollector {
    pub fn new(
        worker_id: usize,
        stats_tx: Sender<MsgStats>,
        //shutdown_rx: Receiver<()>,
        route: impl Into<String>,
        cont: Arc<AtomicBool>,
    ) -> Self {
        Self {
            worker_id,
            stats_tx,
            //shutdown_rx,
            route: route.into(),
            interval: Duration::from_secs(1),
            cont,
        }
    }

    pub fn start(self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();

            let route = self.route.clone();
            let interval = self.interval;

            let mut last_prompt_tokens = 0.0;
            let mut last_gen_tokens = 0.0;
            let mut last_time = std::time::Instant::now();

            rt.block_on(async move {
                loop {
                    if self.cont.load(std::sync::atomic::Ordering::Relaxed) == false {
                        println!("metrics: received shutdown signal, exiting");
                        return;
                    }
                    if let Ok(resp) = reqwest::get(&route).await {
                        if let Ok(text) = resp.text().await {
                            let mut m = parse_vllm_metrics(&text);

                            let now = std::time::Instant::now();
                            let dt = now.duration_since(last_time).as_secs_f64();

                            m.compute(last_prompt_tokens, last_gen_tokens, dt);

                            last_prompt_tokens = m.prompt_tokens_total;
                            last_gen_tokens = m.gen_tokens_total;
                            last_time = now;
                            if self
                                .stats_tx
                                .send(MsgStats {
                                    opcode: "metrics_stats".to_string(),
                                    worker_id: self.worker_id,
                                    body: MsgBody::Metrics(MetricsMsg {
                                        running: m.running,
                                        waiting: m.waiting,
                                        kv_cache_frac: m.kv_cache_frac,
                                        kv_cache_pct: m.kv_cache_pct,
                                        prefix_hits: m.prefix_hits,
                                        prefix_queries: m.prefix_queries,
                                        prefix_hit_rate: m.prefix_hit_rate,
                                        prompt_tokens_total: m.prompt_tokens_total,
                                        gen_tokens_total: m.gen_tokens_total,
                                        prompt_tps: m.prompt_tps,
                                        gen_tps: m.gen_tps,
                                        ttft_avg: m.ttft_avg,
                                        e2e_avg: m.e2e_avg,
                                    }),
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                    }

                    tokio::time::sleep(interval).await;
                }
            });
        })
    }
}

// -----------------------------
// Parser (kept separate)
// -----------------------------
#[derive(Debug, Default)]
pub struct VllmMetrics {
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

    pub ttft_sum: f64,
    pub ttft_count: f64,
    pub ttft_avg: f64,

    pub e2e_sum: f64,
    pub e2e_count: f64,
    pub e2e_avg: f64,
}

impl VllmMetrics {
    pub fn compute(&mut self, last_prompt_tokens: f64, last_gen_tokens: f64, dt: f64) {
        // Throughput (tokens/s)
        if dt > 0.0 {
            let prompt_delta = self.prompt_tokens_total - last_prompt_tokens;
            let gen_delta = self.gen_tokens_total - last_gen_tokens;

            self.prompt_tps = if prompt_delta > 0.0 {
                prompt_delta / dt
            } else {
                0.0
            };

            self.gen_tps = if gen_delta > 0.0 { gen_delta / dt } else { 0.0 };
        }

        // KV cache %
        self.kv_cache_pct = self.kv_cache_frac * 100.0;

        // Prefix cache hit rate %
        if self.prefix_queries > 0.0 {
            self.prefix_hit_rate = (self.prefix_hits / self.prefix_queries) * 100.0;
        }
        //ttft
        if self.ttft_count > 0.0 {
            self.ttft_avg = self.ttft_sum / self.ttft_count;
        }
        //e2e
        if self.e2e_count > 0.0 {
            self.e2e_avg = self.e2e_sum / self.e2e_count;
        }
    }
}

pub fn parse_vllm_metrics(text: &str) -> VllmMetrics {
    let mut m = VllmMetrics::default();

    for line in text.lines() {
        if let Some(v) = line.strip_prefix("vllm:num_requests_running") {
            m.running = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:num_requests_waiting") {
            m.waiting = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:kv_cache_usage_perc") {
            m.kv_cache_frac = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:prefix_cache_hits_total") {
            m.prefix_hits = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:prefix_cache_queries_total") {
            m.prefix_queries = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:prompt_tokens_total") {
            m.prompt_tokens_total = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:generation_tokens_total") {
            m.gen_tokens_total = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:time_to_first_token_seconds_sum") {
            m.ttft_sum = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:time_to_first_token_seconds_count") {
            m.ttft_count = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:e2e_request_latency_seconds_sum") {
            m.e2e_sum = extract_value(v);
        } else if let Some(v) = line.strip_prefix("vllm:e2e_request_latency_seconds_count") {
            m.e2e_count = extract_value(v);
        }
    }

    m
}

fn extract_value(line: &str) -> f64 {
    line.split_whitespace()
        .last()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0.0)
}
