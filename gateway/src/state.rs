//! Application state for gateway service.

use common::config::{AppConfig, ServiceUrls};

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub service_urls: ServiceUrls,
    pub http_client: reqwest::Client,
}

impl AppState {
    /// Creates a new application state.
    pub fn new(config: AppConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            service_urls: ServiceUrls::load(),
            http_client,
        }
    }
}
