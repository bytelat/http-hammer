use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct RuntimeSettings {
    pub rps: AtomicUsize,
    active_requests: AtomicUsize,
    pub max_requests: usize,
}

impl RuntimeSettings {
    pub fn new(initial_rps: u64, max_requests: usize) -> Self {
        Self {
            rps: AtomicUsize::new(initial_rps as usize),
            active_requests: AtomicUsize::new(max_requests),
            max_requests,
        }
    }

    pub fn rps(&self) -> u64 {
        self.rps.load(Ordering::Relaxed) as u64
    }

    pub fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::Relaxed)
    }

    pub fn set_active_requests(&self, requested: usize) {
        let active = if requested == 0 {
            self.max_requests
        } else {
            requested.min(self.max_requests).max(1)
        };
        self.active_requests.store(active, Ordering::Relaxed);
    }

    pub fn _set_rps(&self, value: u64) {
        // Optional: validation
        if value == 0 {
            println!("Warning: RPS set to zero, injector will idle");
        }
        self.rps.store(value as usize, Ordering::Relaxed);
    }
    pub fn inc_rps(&self, value: u64) {
        let current = self.rps.load(Ordering::Relaxed);
        self.rps.store(current + value as usize, Ordering::Relaxed);
    }
    pub fn dec_rps(&self, value: u64) {
        let current = self.rps.load(Ordering::Relaxed);
        let new_value = if current >= value as usize {
            current - value as usize
        } else {
            0
        };
        self.rps.store(new_value, Ordering::Relaxed);
    }
}
