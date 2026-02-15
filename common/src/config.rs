//! Application configuration module.
//!
//! Handles loading and managing server configuration from environment variables.

use serde::Deserialize;

/// Application configuration.
///
/// Configuration values can be set via environment variables:
/// - `SERVER_HOST` - Server bind address (default: "0.0.0.0")
/// - `SERVER_PORT` - Server port (default: 8080)
/// - `RUST_LOG` - Log level (default: "info")
/// - `MAX_CONNECTIONS` - Maximum connections per pool (default: 10)
/// - `CONNECT_TIMEOUT` - Connection timeout in seconds (default: 30)
/// - `DATA_DIR` - Data directory for persistence (default: "./data")
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// Server host address.
    #[serde(default = "default_host")]
    pub host: String,

    /// Server port number.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Log level.
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Maximum connections per database pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Connection timeout in seconds.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// Data directory for persistence.
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// MySQL database URL for metadata persistence.
    #[serde(default = "default_database_url")]
    pub database_url: String,

    /// Service name for identification.
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

impl AppConfig {
    /// Loads configuration from environment variables.
    ///
    /// Falls back to default values if environment variables are not set.
    pub fn load() -> Self {
        Self {
            host: std::env::var("SERVER_HOST").unwrap_or_else(|_| default_host()),
            port: std::env::var("SERVER_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_port),
            log_level: std::env::var("RUST_LOG").unwrap_or_else(|_| default_log_level()),
            max_connections: std::env::var("MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_max_connections),
            connect_timeout_secs: std::env::var("CONNECT_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_connect_timeout),
            data_dir: std::env::var("DATA_DIR").unwrap_or_else(|_| default_data_dir()),
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| default_database_url()),
            service_name: std::env::var("SERVICE_NAME").unwrap_or_else(|_| default_service_name()),
        }
    }

    /// Loads configuration with a specific service name.
    pub fn load_with_service(service_name: impl Into<String>) -> Self {
        let mut config = Self::load();
        config.service_name = service_name.into();
        config
    }

    /// Returns the full server address string (host:port).
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Default server host address.
fn default_host() -> String {
    "0.0.0.0".to_string()
}

/// Default server port.
fn default_port() -> u16 {
    8080
}

/// Default log level.
fn default_log_level() -> String {
    "info".to_string()
}

/// Default max connections per pool.
fn default_max_connections() -> u32 {
    10
}

/// Default connection timeout.
fn default_connect_timeout() -> u64 {
    30
}

/// Default data directory.
fn default_data_dir() -> String {
    "./data".to_string()
}

/// Default MySQL database URL for metadata.
fn default_database_url() -> String {
    "mysql://root:123456@localhost:3306/db_management".to_string()
}

/// Default service name.
fn default_service_name() -> String {
    "unknown".to_string()
}

/// Service discovery configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceUrls {
    /// Gateway service URL.
    #[serde(default = "default_gateway_url")]
    pub gateway: String,

    /// Connection service URL.
    #[serde(default = "default_connection_service_url")]
    pub connection_service: String,

    /// Query service URL.
    #[serde(default = "default_query_service_url")]
    pub query_service: String,

    /// AI service URL.
    #[serde(default = "default_ai_service_url")]
    pub ai_service: String,
}

impl ServiceUrls {
    /// Loads service URLs from environment variables.
    pub fn load() -> Self {
        Self {
            gateway: std::env::var("GATEWAY_URL").unwrap_or_else(|_| default_gateway_url()),
            connection_service: std::env::var("CONNECTION_SERVICE_URL")
                .unwrap_or_else(|_| default_connection_service_url()),
            query_service: std::env::var("QUERY_SERVICE_URL")
                .unwrap_or_else(|_| default_query_service_url()),
            ai_service: std::env::var("AI_SERVICE_URL")
                .unwrap_or_else(|_| default_ai_service_url()),
        }
    }
}

fn default_gateway_url() -> String {
    "http://localhost:8080".to_string()
}

fn default_connection_service_url() -> String {
    "http://localhost:8081".to_string()
}

fn default_query_service_url() -> String {
    "http://localhost:8082".to_string()
}

fn default_ai_service_url() -> String {
    "http://localhost:8083".to_string()
}
