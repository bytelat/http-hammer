use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct RuntimeSettings {
    pub rps: AtomicUsize,
}

impl RuntimeSettings {
    pub fn new(initial_rps: u64) -> Self {
        Self {
            rps: AtomicUsize::new(initial_rps as usize),
        }
    }

    pub fn rps(&self) -> u64 {
        self.rps.load(Ordering::Relaxed) as u64
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
