//! Common library for database manager microservices.
//!
//! This crate provides shared functionality including:
//! - Error handling and result types
//! - API response models
//! - Configuration management
//! - Middleware components
//! - Utility functions

pub mod config;
pub mod errors;
pub mod middleware;
pub mod models;
pub mod response;
pub mod utils;

// Re-export commonly used types
pub use config::AppConfig;
pub use errors::{AppError, AppResult};
pub use response::{ApiResponse, ApiError, ResponseMeta, Pagination, PaginatedData};
