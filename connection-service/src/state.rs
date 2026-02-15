//! Application state for connection service.

use std::sync::Arc;
use common::config::AppConfig;
use common::errors::AppResult;
use sqlx::mysql::MySqlPoolOptions;
use crate::pool_manager::PoolManager;

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub pool_manager: Arc<PoolManager>,
}

impl AppState {
    /// Creates a new application state.
    /// Connects to the metadata MySQL database and initializes the pool manager.
    pub async fn new(config: AppConfig) -> AppResult<Self> {
        // Connect to the management MySQL database
        let meta_pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&config.database_url)
            .await
            .map_err(|e| common::errors::AppError::DatabaseConnection(
                format!("Failed to connect to metadata DB ({}): {}", config.database_url, e)
            ))?;

        tracing::info!(url = %config.database_url, "Connected to metadata MySQL database");

        let pool_manager = PoolManager::new(config.clone(), meta_pool).await?;

        Ok(Self {
            pool_manager: Arc::new(pool_manager),
            config,
        })
    }
}
