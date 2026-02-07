//! 请求代理模块，用于路由转发到后端服务

use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Router,
};
use common::middleware::request_id::REQUEST_ID_HEADER;

use crate::state::AppState;

/// 创建代理路由
pub fn router() -> Router<AppState> {
    Router::new()
        // 连接服务路由
        .route("/api/connections", get(proxy_to_connection_service).post(proxy_to_connection_service))
        .route("/api/connections/{*path}", any(proxy_to_connection_service))
        // 查询服务路由
        .route("/api/query", post(proxy_to_query_service))
        .route("/api/databases", post(proxy_to_query_service))
}

/// 转发请求到连接服务
async fn proxy_to_connection_service(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Response {
    proxy_request(&state, &state.service_urls.connection_service, req).await
}

/// 转发请求到查询服务
async fn proxy_to_query_service(
    State(state): State<AppState>,
    req: Request<Body>,
) -> Response {
    proxy_request(&state, &state.service_urls.query_service, req).await
}

/// 转发请求到目标服务
async fn proxy_request(
    state: &AppState,
    target_base: &str,
    req: Request<Body>,
) -> Response {
    let (parts, body) = req.into_parts();
    
    // 构建目标 URL
    let path = parts.uri.path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let target_url = format!("{}{}", target_base, path);

    // 从原始请求获取请求 ID
    let request_id = parts.headers
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // 将请求体转换为字节
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!(error = %e, "读取请求体失败");
            return (StatusCode::BAD_REQUEST, "读取请求体失败").into_response();
        }
    };

    // 构建代理请求
    let mut proxy_req = state.http_client
        .request(parts.method.clone(), &target_url);

    // 复制请求头（排除 host）
    for (name, value) in parts.headers.iter() {
        if name != "host" {
            proxy_req = proxy_req.header(name.clone(), value.clone());
        }
    }

    // 添加请求 ID 头
    if !request_id.is_empty() {
        proxy_req = proxy_req.header(REQUEST_ID_HEADER.as_str(), request_id);
    }

    // 发送请求
    let response = match proxy_req.body(body_bytes.to_vec()).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!(error = %e, target = %target_url, "代理请求失败");
            return (
                StatusCode::BAD_GATEWAY,
                format!("服务不可用: {}", e),
            ).into_response();
        }
    };

    // 转换响应
    let status = response.status();
    let headers = response.headers().clone();
    
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!(error = %e, "读取响应体失败");
            return (StatusCode::BAD_GATEWAY, "读取响应体失败").into_response();
        }
    };

    // 构建响应
    let mut builder = Response::builder().status(status);
    
    for (name, value) in headers.iter() {
        builder = builder.header(name, value);
    }

    builder
        .body(Body::from(body_bytes.to_vec()))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "构建响应失败").into_response())
}

