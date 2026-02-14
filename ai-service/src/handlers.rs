//! Handler 模块

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use common::errors::AppError;
use common::response::ApiResponse;

use crate::models::{
    ClarifyRequest, ClarifyResponse, NaturalQueryRequest, NaturalQueryResponse,
    ValidateSqlRequest, ValidateSqlResponse,
};
use crate::service::AiQueryService;
use crate::state::AppState;

/// 自然语言查询
///
/// 将用户的自然语言问题转换为 SQL 查询
#[utoipa::path(
    post,
    path = "/api/ai/query",
    tag = "ai-query",
    request_body = NaturalQueryRequest,
    responses(
        (status = 200, description = "查询处理成功", body = ApiResponse<NaturalQueryResponse>),
        (status = 400, description = "请求参数无效"),
        (status = 500, description = "服务内部错误")
    )
)]
pub async fn natural_query(
    State(state): State<AppState>,
    Json(req): Json<NaturalQueryRequest>,
) -> Result<Json<ApiResponse<NaturalQueryResponse>>, AppError> {
    let service = AiQueryService::new(
        state.ai_config.clone(),
        state.service_urls.clone(),
        state.http_client.clone(),
    );

    let result = service.process_natural_query(req).await?;
    Ok(Json(ApiResponse::ok_with_service(result, "ai-service")))
}

/// 澄清回复
///
/// 处理用户对澄清问题的回复，继续生成 SQL
#[utoipa::path(
    post,
    path = "/api/ai/clarify",
    tag = "ai-query",
    request_body = ClarifyRequest,
    responses(
        (status = 200, description = "澄清处理成功", body = ApiResponse<ClarifyResponse>),
        (status = 400, description = "请求参数无效"),
        (status = 404, description = "原始请求未找到")
    )
)]
pub async fn clarify(
    State(state): State<AppState>,
    Json(req): Json<ClarifyRequest>,
) -> Result<Json<ApiResponse<ClarifyResponse>>, AppError> {
    let service = AiQueryService::new(
        state.ai_config.clone(),
        state.service_urls.clone(),
        state.http_client.clone(),
    );

    let result = service.process_clarification(req).await?;
    Ok(Json(ApiResponse::ok_with_service(result, "ai-service")))
}

/// SQL 校验
///
/// 校验 SQL 语句的安全性和执行风险
#[utoipa::path(
    post,
    path = "/api/ai/validate",
    tag = "ai-query",
    request_body = ValidateSqlRequest,
    responses(
        (status = 200, description = "校验完成", body = ApiResponse<ValidateSqlResponse>),
        (status = 400, description = "请求参数无效")
    )
)]
pub async fn validate_sql(
    State(state): State<AppState>,
    Json(req): Json<ValidateSqlRequest>,
) -> Result<Json<ApiResponse<ValidateSqlResponse>>, AppError> {
    let service = AiQueryService::new(
        state.ai_config.clone(),
        state.service_urls.clone(),
        state.http_client.clone(),
    );

    let result = service.validate_sql(req).await?;
    Ok(Json(ApiResponse::ok_with_service(result, "ai-service")))
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
pub async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        service: "ai-service".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: Utc::now(),
        llm_configured: !state.ai_config.llm_api_key.is_empty(),
    })
}

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub timestamp: DateTime<Utc>,
    /// LLM API Key 是否已配置
    pub llm_configured: bool,
}
