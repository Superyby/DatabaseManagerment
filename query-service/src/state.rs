//! Application state for query service.

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
        Self {
            config,
            service_urls: ServiceUrls::load(),
            http_client: reqwest::Client::new(),
        }
    }
}
