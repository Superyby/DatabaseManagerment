//健康检查接口,验证能否前后端成功通信
use axum::{ routing::{ get, post }, Router, Json };
use serde::{ Deserialize, Serialize };
use tracing_subscriber::{ layer::SubscriberExt, util::SubscriberInitExt };
use std::net::SocketAddr;
use tokio::net::TcpListener;
mod routes;

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber
        ::registry()
        .with(
            tracing_subscriber::EnvFilter::new(
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
            )
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    tracing::info!("🚀 启动 DatabaseManager 后端");

    let app = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/databases", post(list_databases));

    let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    tracing::info!("📡 监听地址: http://{}", addr);

    // 使用 TcpListener + axum::serve
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.into_make_service()).await.unwrap();
}

// 健康检查
async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
    timestamp: String,
}

// 占位：数据库列表接口（返回模拟数据）
#[derive(Deserialize)]
struct ListDatabasesRequest {
    // db_type: Option<String>, // "mysql", "postgres", "sqlite"
}

#[derive(Serialize)]
struct DatabaseItem {
    id: u32,
    name: String,
    r#type: String,
    host: String,
    port: u16,
}

async fn list_databases(_req: Json<ListDatabasesRequest>) -> Json<Vec<DatabaseItem>> {
    // 模拟数据（后续替换为真实 SQL 查询）
    Json(
        vec![
            DatabaseItem {
                id: 1,
                name: "production_db".to_string(),
                r#type: "mysql".to_string(),
                host: "localhost".to_string(),
                port: 3306,
            },
            DatabaseItem {
                id: 2,
                name: "analytics".to_string(),
                r#type: "postgres".to_string(),
                host: "192.168.31.36".to_string(),
                port: 5432,
            }
        ]
    )
}
