//! 连接服务路由模块

use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use common::errors::AppError;
use common::models::connection::{ConnectionItem, CreateConnectionRequest};
use common::response::ApiResponse;
use crate::service::ConnectionService;
use crate::state::AppState;

/// 创建连接管理路由
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/connections", get(list_connections).post(create_connection))
        .route("/api/connections/{id}", get(get_connection).delete(delete_connection))
        .route("/api/connections/{id}/test", get(test_connection))
        .route("/api/health", get(health_check))
        .route("/internal/pools/{id}", get(get_pool_info))
}

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

/// 连接测试结果
#[derive(Serialize, ToSchema)]
pub struct ConnectionTestResult {
    /// 连接 ID
    pub id: String,
    /// 测试是否成功
    pub success: bool,
    /// 连接延迟（毫秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// 错误信息（如果测试失败）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
    /// 活跃连接数
    pub connections: usize,
}

/// 连接池信息（用于服务间通信）
#[derive(Serialize, ToSchema)]
pub struct PoolInfo {
    /// 连接 ID
    pub id: String,
    /// 数据库类型
    pub db_type: String,
    /// 数据库主机
    pub host: Option<String>,
    /// 数据库端口
    pub port: Option<u16>,
    /// 数据库名称
    pub database: Option<String>,
}

