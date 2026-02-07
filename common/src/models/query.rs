//! SQL query models.
//!
//! Contains models for SQL query execution.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

/// Request body for executing a SQL query.
#[derive(Debug, Serialize, Deserialize, Validate, ToSchema)]
pub struct QueryRequest {
    /// ID of the connection to use.
    #[validate(length(min = 1, message = "Connection ID is required"))]
    pub connection_id: String,

    /// SQL statement to execute.
    #[validate(length(min = 1, message = "SQL statement is required"))]
    pub sql: String,

    /// Maximum number of rows to return (default: 1000).
    #[serde(default = "default_limit")]
    pub limit: Option<u32>,
}

fn default_limit() -> Option<u32> {
    Some(1000)
}

/// Result of a SQL query execution.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct QueryResult {
    /// Column information.
    pub columns: Vec<ColumnInfo>,

    /// Row data (each row is a vector of JSON values).
    pub rows: Vec<Vec<serde_json::Value>>,

    /// Number of rows returned.
    #[serde(default)]
    pub row_count: usize,

    /// Number of rows affected (for INSERT/UPDATE/DELETE).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected_rows: Option<u64>,

    /// Query execution time in milliseconds.
    #[serde(default)]
    pub execution_time_ms: u64,
}

/// Column information in query result.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,

    /// Column data type.
    pub data_type: String,

    /// Whether the column is nullable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
}

impl QueryResult {
    /// Creates a new empty query result.
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            affected_rows: None,
            execution_time_ms: 0,
        }
    }

    /// Creates a query result with affected rows count (for non-SELECT queries).
    pub fn affected(affected: u64, execution_time_ms: u64) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            affected_rows: Some(affected),
            execution_time_ms,
        }
    }
}
