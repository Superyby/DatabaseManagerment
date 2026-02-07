//! Application state for connection service.

use std::sync::Arc;
use common::config::AppConfig;
use crate::pool_manager::PoolManager;

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub pool_manager: Arc<PoolManager>,
}

impl AppState {
    /// Creates a new application state.
    pub fn new(config: AppConfig) -> Self {
        Self {
            pool_manager: Arc::new(PoolManager::new(config.clone())),
            config,
        }
    }
}
