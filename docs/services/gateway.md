# Gateway 服务

API 网关服务，负责请求路由、中间件处理、聚合健康检查。

## 1. 服务信息

| 项目 | 值 |
|------|-----|
| 服务名 | gateway |
| 端口 | 8080 |
| 入口 | `gateway/src/main.rs` |

## 2. 职责

- 统一 API 入口
- 请求路由转发
- 中间件处理（CORS、Trace、RequestID）
- 聚合健康检查

## 3. 目录结构

```
gateway/
├── Cargo.toml
└── src/
    ├── main.rs         # 服务入口
    ├── routes.rs       # 路由定义
    ├── handlers.rs     # 健康检查处理器
    ├── proxy.rs        # 请求代理
    └── state.rs        # 应用状态
```

## 4. 路由规则

| 路径模式 | 目标服务 | 说明 |
|----------|----------|------|
| `/api/connections/**` | connection-service | 连接管理 |
| `/api/query` | query-service | SQL 查询 |
| `/api/ai/**` | ai-service | AI 智能查询 |
| `/api/health` | 本地处理 | 网关健康检查 |
| `/api/health/all` | 本地处理 | 聚合健康检查 |

## 5. 中间件链

```rust
Router::new()
    .merge(routes::router())
    .merge(proxy::router())
    .layer(middleware::from_fn(request_id_middleware))
    .layer(TraceLayer::new_for_http())
    .layer(cors)
    .with_state(state)
```

执行顺序（从外到内）：
1. CORS 处理
2. HTTP Trace 日志
3. Request ID 注入
4. 路由匹配
5. 请求处理

## 6. 代理实现

```rust
async fn proxy_request(
    state: &AppState,
    target_base: &str,
    req: Request<Body>,
) -> Response {
    // 1. 解析请求
    let (parts, body) = req.into_parts();

    // 2. 构建目标 URL
    let target_url = format!("{}{}", target_base, parts.uri.path_and_query());

    // 3. 转发请求（保留 headers）
    let response = state.http_client
        .request(parts.method, &target_url)
        .headers(parts.headers)
        .body(body)
        .send()
        .await?;

    // 4. 返回响应
    response.into_response()
}
```

## 7. 聚合健康检查

检查所有后端服务状态：

```rust
pub async fn aggregated_health(State(state): State<AppState>) -> Json<AggregatedHealth> {
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
```

## 8. 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `8080` | 监听端口 |
| `CONNECTION_SERVICE_URL` | `http://localhost:8081` | 连接服务地址 |
| `QUERY_SERVICE_URL` | `http://localhost:8082` | 查询服务地址 |
| `AI_SERVICE_URL` | `http://localhost:8083` | AI 服务地址 |
| `RUST_LOG` | `info` | 日志级别 |

## 9. API 文档

服务启动后访问：
- OpenAPI JSON: `http://localhost:8080/api-docs/openapi.json`
