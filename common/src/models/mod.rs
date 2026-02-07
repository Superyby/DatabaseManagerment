//! Shared data models for all microservices.

pub mod connection;
pub mod database;
pub mod query;

// Re-export commonly used types
pub use connection::{ConnectionConfig, ConnectionItem, CreateConnectionRequest, DbType};
pub use database::{DatabaseItem, ListDatabasesRequest};
pub use query::{ColumnInfo, QueryRequest, QueryResult};
