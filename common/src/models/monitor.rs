//! Monitoring and performance metrics models.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Database server statistics.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseStats {
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Total queries executed since startup.
    pub total_queries: u64,
    /// Total active connections.
    pub active_connections: u32,
    /// Maximum allowed connections.
    pub max_connections: u32,
    /// Total slow queries since startup.
    pub slow_queries: u64,
    /// Queries per second (computed).
    pub queries_per_second: f64,
    /// Bytes received total.
    pub bytes_received: u64,
    /// Bytes sent total.
    pub bytes_sent: u64,
    /// Buffer pool size in bytes (MySQL InnoDB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_pool_size: Option<u64>,
    /// Database server version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
    /// Additional key-value metrics.
    #[serde(default)]
    pub extra: std::collections::HashMap<String, String>,
}

impl Default for DatabaseStats {
    fn default() -> Self {
        Self {
            uptime_seconds: 0,
            total_queries: 0,
            active_connections: 0,
            max_connections: 0,
            slow_queries: 0,
            queries_per_second: 0.0,
            bytes_received: 0,
            bytes_sent: 0,
            buffer_pool_size: None,
            server_version: None,
            extra: std::collections::HashMap::new(),
        }
    }
}

/// Active database process information.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProcessInfo {
    /// Process ID.
    pub id: u64,
    /// User running the process.
    pub user: String,
    /// Client host.
    pub host: String,
    /// Database being accessed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,
    /// Command type (Query, Sleep, etc.).
    pub command: String,
    /// Time in seconds the process has been running.
    pub time: u64,
    /// Current state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// SQL info (truncated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

/// Database information on the server.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseInfo {
    /// Database name.
    pub name: String,
    /// Number of tables.
    pub tables_count: u32,
    /// Size in megabytes.
    pub size_mb: f64,
}

/// Connection pool statistics.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConnectionPoolStats {
    /// Number of active (in-use) connections.
    pub active: u32,
    /// Number of idle connections.
    pub idle: u32,
    /// Maximum pool size configured.
    pub max_size: u32,
    /// Whether the pool is connected.
    pub is_connected: bool,
}

/// Aggregated monitoring overview for a single connection.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MonitorOverview {
    /// Connection ID.
    pub connection_id: String,
    /// Connection name.
    pub connection_name: String,
    /// Database type.
    pub db_type: String,
    /// Database server statistics.
    pub stats: DatabaseStats,
    /// Connection pool statistics.
    pub pool: ConnectionPoolStats,
    /// Timestamp of this snapshot.
    pub timestamp: String,
}
