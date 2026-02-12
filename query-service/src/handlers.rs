//! Handler模块

use axum::{
    extract::State,
    Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use common::errors::AppError;
use common::models::query::{QueryRequest, QueryResult};
use common::response::ApiResponse;
use crate::service::QueryService;
use crate::state::AppState;

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

/// 测试端点
#[utoipa::path(
    get,
    path = "/api/test",
    tag = "test",
    responses(
        (status = 200, description = "测试成功", body = ApiResponse<String>)
    )
)]
pub async fn hello_test() -> Json<ApiResponse<String>> {
    Json(ApiResponse::ok("hello test".to_string()))
}

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub timestamp: DateTime<Utc>,
}
