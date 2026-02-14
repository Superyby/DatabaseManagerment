//! AI 智能查询服务
//!
//! 提供 AI 驱动的数据库查询功能，包括：
//! - Text2SQL：自然语言转 SQL 查询
//! - 语义理解：指标、维度、口径的统一建模
//! - RAG 增强：历史 SQL、FAQ、指标定义的检索增强
//! - 安全治理：权限控制、数据脱敏、只读执行、审计追溯

mod handlers;
mod models;
mod routes;
mod service;
mod state;

use axum::{middleware, routing::get, Json, Router};
use common::config::AppConfig;
use common::middleware::request_id::request_id_middleware;
use state::AppState;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::OpenApi;

const SERVICE_NAME: &str = "ai-service";
const DEFAULT_PORT: u16 = 8083;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "AI 智能查询服务 API",
        version = "0.1.0",
        description = "Text2SQL、语义理解、RAG 增强的智能查询微服务"
    ),
    paths(
        handlers::natural_query,
        handlers::clarify,
        handlers::validate_sql,
        handlers::health_check,
    ),
    components(schemas(
        models::NaturalQueryRequest,
        models::NaturalQueryResponse,
        models::ClarifyRequest,
        models::ClarifyResponse,
        models::ValidateSqlRequest,
        models::ValidateSqlResponse,
        models::SqlReference,
        handlers::HealthResponse,
    )),
    tags(
        (name = "ai-query", description = "AI 智能查询端点"),
        (name = "health", description = "健康检查端点")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() {
    // 初始化日志追踪
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // 加载配置
    let mut config = AppConfig::load_with_service(SERVICE_NAME);
    config.port = std::env::var("SERVER_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    // 创建应用状态
    let state = AppState::new(config.clone());

    // 创建路由
    let app = create_router(state);

    // 启动服务
    let addr = format!("{}:{}", config.host, config.port);
    info!(service = SERVICE_NAME, address = %addr, "启动服务");

    let listener = TcpListener::bind(&addr).await.expect("绑定地址失败");
    axum::serve(listener, app).await.expect("服务启动失败");
}

fn create_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .merge(routes::router())
        .route("/api-docs/openapi.json", get(openapi_json))
        .layer(middleware::from_fn(request_id_middleware))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}
