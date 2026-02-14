//! Handler模块

use axum::{
    extract::State,
    Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::AppState;

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

/// 聚合健康检查 - 检查所有微服务的健康状态
#[utoipa::path(
    get,
    path = "/api/health/aggregated",
    tag = "health",
    responses(
        (status = 200, description = "聚合健康检查结果", body = AggregatedHealth)
    )
)]
pub async fn aggregated_health(
    State(state): State<AppState>,
) -> Json<AggregatedHealth> {
    let services = vec![
        check_service_health(&state.http_client, "connection-service", &state.service_urls.connection_service).await,
        check_service_health(&state.http_client, "query-service", &state.service_urls.query_service).await,
        check_service_health(&state.http_client, "ai-service", &state.service_urls.ai_service).await,
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

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
pub struct AggregatedHealth {
    pub status: String,
    pub timestamp: DateTime<Utc>,
    pub services: Vec<ServiceHealth>,
}

#[derive(Serialize, ToSchema)]
pub struct ServiceHealth {
    pub name: String,
    pub url: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
