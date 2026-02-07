//! 查询服务路由模块

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use common::errors::AppError;
use common::models::query::{QueryRequest, QueryResult};
use common::response::ApiResponse;
use crate::service::QueryService;
use crate::state::AppState;

/// 创建查询路由
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/query", post(execute_query))
        .route("/api/health", get(health_check))
}

/// 执行 SQL 查询
#[utoipa::path(
    post,
    path = "/api/query",
    tag = "query",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "查询执行成功", body = ApiResponse<QueryResult>),
        (status = 400, description = "SQL 无效或校验错误"),
        (status = 404, description = "连接未找到")
    )
)]
pub async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResult>>, AppError> {
    let service = QueryService::new(
        state.service_urls.connection_service.clone(),
        state.http_client.clone(),
    );
    
    let result = service.execute(req).await?;
    Ok(Json(ApiResponse::ok_with_service(result, "query-service")))
}

/// 健康检查端点
#[utoipa::path(
    get,
    path = "/api/health",
    tag = "health",
    responses(
        (status = 200, description = "服务运行正常", body = HealthResponse)
    )
)]
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        service: "query-service".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: Utc::now(),
    })
}

/// 健康检查响应
#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    /// 服务状态
    pub status: String,
    /// 服务名称
    pub service: String,
    /// 服务版本
    pub version: String,
    /// 当前时间戳
    pub timestamp: DateTime<Utc>,
}

