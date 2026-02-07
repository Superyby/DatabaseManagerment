//! Connection configuration models.
//!
//! Contains models for database connection management.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

/// Database type enumeration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DbType {
    /// MySQL database.
    MySQL,
    /// PostgreSQL database.
    Postgres,
    /// SQLite database.
    SQLite,
    /// Redis key-value store.
    Redis,
}

impl DbType {
    /// Returns the default port for this database type.
    pub fn default_port(&self) -> Option<u16> {
        match self {
            DbType::MySQL => Some(3306),
            DbType::Postgres => Some(5432),
            DbType::SQLite => None,
            DbType::Redis => Some(6379),
        }
    }
}

impl std::fmt::Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbType::MySQL => write!(f, "mysql"),
            DbType::Postgres => write!(f, "postgres"),
            DbType::SQLite => write!(f, "sqlite"),
            DbType::Redis => write!(f, "redis"),
        }
    }
}

/// Full connection configuration (stored internally).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConnectionConfig {
    /// Unique connection identifier.
    pub id: String,
    /// Connection display name.
    pub name: String,
    /// Database type.
    pub db_type: DbType,
    /// Database host (for network databases).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Database port (for network databases).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Database username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Database password (not serialized in responses).
    #[serde(skip_serializing, default)]
    pub password: Option<String>,
    /// Default database name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// SQLite file path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Creation timestamp.
    pub created_at: String,
}

/// Request body for creating a new connection.
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateConnectionRequest {
    /// Connection display name.
    #[validate(length(min = 1, max = 100, message = "Name must be 1-100 characters"))]
    pub name: String,
    /// Database type.
    pub db_type: DbType,
    /// Database host (required for mysql/postgres/redis).
    pub host: Option<String>,
    /// Database port (uses default if not specified).
    pub port: Option<u16>,
    /// Database username.
    pub username: Option<String>,
    /// Database password.
    pub password: Option<String>,
    /// Default database name.
    pub database: Option<String>,
    /// SQLite file path (required for sqlite).
    pub file_path: Option<String>,
}

impl CreateConnectionRequest {
    /// Converts the request into a ConnectionConfig.
    pub fn into_config(self, id: String, created_at: String) -> ConnectionConfig {
        ConnectionConfig {
            id,
            name: self.name,
            db_type: self.db_type.clone(),
            host: self.host,
            port: self.port.or_else(|| self.db_type.default_port()),
            username: self.username,
            password: self.password,
            database: self.database,
            file_path: self.file_path,
            created_at,
        }
    }
}

/// Connection item for API responses (excludes sensitive data).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConnectionItem {
    /// Unique connection identifier.
    pub id: String,
    /// Connection display name.
    pub name: String,
    /// Database type.
    pub db_type: DbType,
    /// Database host.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Database port.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Database username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Default database name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// SQLite file path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Creation timestamp.
    pub created_at: String,
}

impl From<ConnectionConfig> for ConnectionItem {
    fn from(config: ConnectionConfig) -> Self {
        Self {
            id: config.id,
            name: config.name,
            db_type: config.db_type,
            host: config.host,
            port: config.port,
            username: config.username,
            database: config.database,
            file_path: config.file_path,
            created_at: config.created_at,
        }
    }
}
