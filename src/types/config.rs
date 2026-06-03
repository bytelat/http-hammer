use serde::Deserialize;
use std::collections::HashMap;
//use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub upstreams: Vec<String>,
    pub concurrency: usize,
    pub routes: Routes,
    pub log_level: String,
    #[serde(default)]
    //pub upstream_path: String,
    //pub cli_refresh_interval_ms: u64,
    pub keep_alive: usize, // Number of open tcp connections to keep alive per upstream
    pub template: serde_json::Value, // Keep as Value for flexibility
    // template_str is computed later
    #[serde(default)]
    pub template_str: String, // Store original template string for injection
    pub model: String, // Model name for requests
    #[serde(default = "default_body_fields")]
    pub body_fields: HashMap<String, String>,
    //#[serde(default)]
    //pub upstream_url: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Routes {
    pub request: String,
    pub ping: String,
    pub metrics: String,
}

fn default_body_fields() -> HashMap<String, String> {
    HashMap::from([("messages".to_string(), "messages".to_string())])
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        // Try the provided path first
        if let Ok(data) = std::fs::read_to_string(path) {
            let cfg: Config = serde_json::from_str(&data)?;
            return Ok(cfg);
        }

        // Try in manifest directory (works with debugger)
        if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
            let config_path = format!("{}/{}", manifest_dir, path);
            if let Ok(data) = std::fs::read_to_string(&config_path) {
                let cfg: Config = serde_json::from_str(&data)?;
                return Ok(cfg);
            }
        }

        // If all fail, return the original error
        let data = std::fs::read_to_string(path)?;
        let cfg: Config = serde_json::from_str(&data)?;
        Ok(cfg)
    }
}
