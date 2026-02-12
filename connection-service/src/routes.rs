//! 路由模块

use axum::{
    routing::{get, post},
    Router,
};
use crate::handlers;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/connections", get(handlers::list_connections).post(handlers::create_connection))
        .route("/api/connections/{id}", get(handlers::get_connection).delete(handlers::delete_connection))
        .route("/api/connections/{id}/test", get(handlers::test_connection))
        .route("/api/health", get(handlers::health_check))
        .route("/internal/pools/{id}", get(handlers::get_pool_info))
}
