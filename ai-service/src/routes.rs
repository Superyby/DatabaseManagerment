//! 路由模块

use axum::{
    routing::{get, post},
    Router,
};

use crate::handlers;
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        // AI 查询核心接口
        .route("/api/ai/query", post(handlers::natural_query))
        .route("/api/ai/clarify", post(handlers::clarify))
        .route("/api/ai/validate", post(handlers::validate_sql))
        // 健康检查
        .route("/api/health", get(handlers::health_check))
}
