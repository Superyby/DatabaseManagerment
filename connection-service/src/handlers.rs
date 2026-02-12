//! Handler模块

use axum::{
    extract::{Path, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use common::errors::AppError;
use common::models::connection::{ConnectionItem, CreateConnectionRequest};
use common::response::ApiResponse;
use crate::service::ConnectionService;
use crate::state::AppState;

/// 列出所有已保存的数据库连接
#[utoipa::path(
    get,
    path = "/api/connections",
    tag = "connections",
    responses(
        (status = 200, description = "连接列表", body = ApiResponse<Vec<ConnectionItem>>)
    )
)]
pub async fn list_connections(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<ConnectionItem>>>, AppError> {
    let service = ConnectionService::new(state.pool_manager);
    let data = service.list().await;
    Ok(Json(ApiResponse::ok_with_service(data, "connection-service")))
}

/// 创建新的数据库连接
#[utoipa::path(
    post,
    path = "/api/connections",
    tag = "connections",
    request_body = CreateConnectionRequest,
    responses(
        (status = 200, description = "连接已创建", body = ApiResponse<ConnectionItem>)
    )
)]
pub async fn create_connection(
    State(state): State<AppState>,
    Json(req): Json<CreateConnectionRequest>,
) -> Result<Json<ApiResponse<ConnectionItem>>, AppError> {
    let service = ConnectionService::new(state.pool_manager);
    let data = service.create(req).await?;
    Ok(Json(ApiResponse::ok_with_service(data, "connection-service")))
}

/// 根据 ID 获取连接
#[utoipa::path(
    get,
    path = "/api/connections/{id}",
    tag = "connections",
    params(
        ("id" = String, Path, description = "连接 ID")
    ),
    responses(
        (status = 200, description = "连接详情", body = ApiResponse<ConnectionItem>),
        (status = 404, description = "连接未找到")
    )
)]
pub async fn get_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<ConnectionItem>>, AppError> {
    let service = ConnectionService::new(state.pool_manager);
    let data = service.get(&id).await?;
    Ok(Json(ApiResponse::ok_with_service(data, "connection-service")))
}

/// 根据 ID 删除数据库连接
#[utoipa::path(
    delete,
    path = "/api/connections/{id}",
    tag = "connections",
    params(
        ("id" = String, Path, description = "连接 ID")
    ),
    responses(
        (status = 200, description = "连接已删除", body = ApiResponse<bool>),
        (status = 404, description = "连接未找到")
    )
)]
pub async fn delete_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<bool>>, AppError> {
    let service = ConnectionService::new(state.pool_manager);
    service.delete(&id).await?;
    Ok(Json(ApiResponse::ok_with_service(true, "connection-service")))
}

/// 测试数据库连接
#[utoipa::path(
    get,
    path = "/api/connections/{id}/test",
    tag = "connections",
    params(
        ("id" = String, Path, description = "连接 ID")
    ),
    responses(
        (status = 200, description = "连接测试结果", body = ApiResponse<ConnectionTestResult>),
        (status = 404, description = "连接未找到")
    )
)]
pub async fn test_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<ConnectionTestResult>>, AppError> {
    let service = ConnectionService::new(state.pool_manager);
    match service.test(&id).await {
        Ok(latency_ms) => Ok(Json(ApiResponse::ok_with_service(
            ConnectionTestResult {
                id,
                success: true,
                latency_ms: Some(latency_ms),
                error: None,
            },
            "connection-service",
        ))),
        Err(e) => Ok(Json(ApiResponse::ok_with_service(
            ConnectionTestResult {
                id,
                success: false,
                latency_ms: None,
                error: Some(e.to_string()),
            },
            "connection-service",
        ))),
    }
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
pub async fn health_check(
    State(state): State<AppState>,
) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        service: "connection-service".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: Utc::now(),
        connections: state.pool_manager.connection_count().await,
    })
}

/// 内部端点，供其他服务获取连接池信息
#[utoipa::path(
    get,
    path = "/internal/pools/{id}",
    tag = "internal",
    params(
        ("id" = String, Path, description = "连接 ID")
    ),
    responses(
        (status = 200, description = "连接池信息", body = ApiResponse<PoolInfo>),
        (status = 404, description = "连接未找到")
    )
)]
pub async fn get_pool_info(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<PoolInfo>>, AppError> {
    let service = ConnectionService::new(state.pool_manager.clone());
    let conn = service.get(&id).await?;
    
    Ok(Json(ApiResponse::ok(PoolInfo {
        id: conn.id,
        db_type: conn.db_type.to_string(),
        host: conn.host,
        port: conn.port,
        database: conn.database,
    })))
}

#[derive(Serialize, ToSchema)]
pub struct ConnectionTestResult {
    pub id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub timestamp: DateTime<Utc>,
    pub connections: usize,
}

#[derive(Serialize, ToSchema)]
pub struct PoolInfo {
    pub id: String,
    pub db_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
}
