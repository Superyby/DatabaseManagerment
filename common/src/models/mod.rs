//! Shared data models for all microservices.

pub mod connection;
pub mod database;
pub mod monitor;
pub mod query;

// Re-export commonly used types
pub use connection::{ConnectionConfig, ConnectionItem, CreateConnectionRequest, DbType};
pub use database::{ColumnDetail, DatabaseItem, ListDatabasesRequest, TableInfo, TableSchema};
pub use monitor::{ConnectionPoolStats, DatabaseInfo, DatabaseStats, MonitorOverview, ProcessInfo};
pub use query::{ColumnInfo, QueryRequest, QueryResult};
