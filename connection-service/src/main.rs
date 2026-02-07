//! 数据库连接管理服务
//!
//! 提供数据库连接管理功能，包括：
//! - 数据库连接的增删改查
//! - 连接池管理
//! - 连接测试

mod pool_manager;
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

const SERVICE_NAME: &str = "connection-service";
const DEFAULT_PORT: u16 = 8081;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "连接服务 API",
        version = "0.1.0",
        description = "数据库连接管理微服务"
    ),
    paths(
        routes::list_connections,
        routes::create_connection,
        routes::get_connection,
        routes::delete_connection,
        routes::test_connection,
        routes::health_check,
    ),
    components(schemas(
        common::models::ConnectionConfig,
        common::models::ConnectionItem,
        common::models::CreateConnectionRequest,
        common::models::DbType,
        routes::ConnectionTestResult,
        routes::HealthResponse,
    )),
    tags(
        (name = "connections", description = "连接管理端点"),
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
