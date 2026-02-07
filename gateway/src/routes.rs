//! 网关路由模块

use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::AppState;

/// 创建网关路由
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/health/all", get(aggregated_health))
}

/// 网关健康检查
#[utoipa::path(
    get,
    path = "/api/health",
    tag = "health",
    responses(
        (status = 200, description = "网关运行正常", body = HealthResponse)
    )
)]
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        service: "gateway".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: Utc::now(),
    })
}

/// 聚合所有服务的健康检查
#[utoipa::path(
    get,
    path = "/api/health/all",
    tag = "health",
    responses(
        (status = 200, description = "聚合健康状态", body = AggregatedHealth)
    )
)]
pub async fn aggregated_health(
    State(state): State<AppState>,
) -> Json<AggregatedHealth> {
    let services = vec![
        check_service_health(&state.http_client, "connection-service", &state.service_urls.connection_service).await,
        check_service_health(&state.http_client, "query-service", &state.service_urls.query_service).await,
    ];

    let all_healthy = services.iter().all(|s| s.healthy);

    Json(AggregatedHealth {
        status: if all_healthy { "healthy" } else { "degraded" }.to_string(),
        timestamp: Utc::now(),
        services,
    })
}

async fn check_service_health(
    client: &reqwest::Client,
    name: &str,
    url: &str,
) -> ServiceHealth {
    let health_url = format!("{}/api/health", url);
    
    match client.get(&health_url).send().await {
        Ok(response) if response.status().is_success() => ServiceHealth {
            name: name.to_string(),
            url: url.to_string(),
            healthy: true,
            error: None,
        },
        Ok(response) => ServiceHealth {
            name: name.to_string(),
            url: url.to_string(),
            healthy: false,
            error: Some(format!("HTTP {}", response.status())),
        },
        Err(e) => ServiceHealth {
            name: name.to_string(),
            url: url.to_string(),
            healthy: false,
            error: Some(e.to_string()),
        },
    }
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

/// 聚合健康响应
#[derive(Serialize, ToSchema)]
pub struct AggregatedHealth {
    /// 整体状态
    pub status: String,
    /// 当前时间戳
    pub timestamp: DateTime<Utc>,
    /// 各服务健康状态
    pub services: Vec<ServiceHealth>,
}

/// 单个服务健康状态
#[derive(Serialize, ToSchema)]
pub struct ServiceHealth {
    /// 服务名称
    pub name: String,
    /// 服务地址
    pub url: String,
    /// 是否健康
    pub healthy: bool,
    /// 错误信息（如果不健康）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

