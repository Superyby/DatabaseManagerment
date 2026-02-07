//! Database entity models.
//!
//! Contains models for database listing and management.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request parameters for listing databases.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ListDatabasesRequest {
    /// Filter by database type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,
    /// Search keyword for name filtering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
}

/// Database item representing a database instance.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseItem {
    /// Unique database identifier.
    pub id: u32,
    /// Database display name.
    pub name: String,
    /// Database type (mysql, postgres, sqlite).
    #[serde(rename = "type")]
    pub db_type: String,
    /// Database host address.
    pub host: String,
    /// Database port number.
    pub port: u16,
}

impl DatabaseItem {
    /// Creates a new database item.
    pub fn new(id: u32, name: &str, db_type: &str, host: &str, port: u16) -> Self {
        Self {
            id,
            name: name.to_string(),
            db_type: db_type.to_string(),
            host: host.to_string(),
            port,
        }
    }
}
