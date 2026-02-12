//! 路由模块

use axum::{
    routing::get,
    Router,
};
use crate::handlers;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(handlers::health_check))
        .route("/api/health/all", get(handlers::aggregated_health))
}
