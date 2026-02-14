# 微服务设计

## 1. 服务划分

### 1.1 服务清单

| 服务 | 端口 | 职责 | 依赖 |
|------|------|------|------|
| Gateway | 8080 | API 入口、路由转发、聚合健康检查 | 所有后端服务 |
| Connection Service | 8081 | 连接管理、连接池、连接测试 | 无 |
| Query Service | 8082 | SQL 执行、结果解析、SQL 校验 | Connection Service |
| AI Service | 8083 | Text2SQL、RAG、语义理解 | Connection Service, Query Service |

### 1.2 服务依赖关系

```
                    ┌─────────────┐
                    │   Gateway   │
                    │   (8080)    │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │ Connection  │ │   Query     │ │    AI       │
    │  Service    │ │  Service    │ │  Service    │
    │   (8081)    │ │   (8082)    │ │   (8083)    │
    └─────────────┘ └──────┬──────┘ └──────┬──────┘
                           │               │
                           │               │
                           ▼               ▼
                    ┌─────────────┐ ┌─────────────┐
                    │ Connection  │ │   Query     │
                    │  Service    │ │  Service    │
                    └─────────────┘ └─────────────┘
```

## 2. 服务通信

### 2.1 通信模式

| 模式 | 使用场景 | 实现方式 |
|------|----------|----------|
| 同步 HTTP | 服务间实时调用 | reqwest Client |
| 请求代理 | Gateway 转发 | axum + reqwest |

### 2.2 服务发现

当前采用静态配置方式，通过环境变量配置服务地址：

```rust
pub struct ServiceUrls {
    pub gateway: String,           // GATEWAY_URL
    pub connection_service: String, // CONNECTION_SERVICE_URL
    pub query_service: String,      // QUERY_SERVICE_URL
    pub ai_service: String,         // AI_SERVICE_URL
}
```

### 2.3 请求追踪

全链路请求 ID 透传：

```
Client → Gateway → Service
   │        │         │
   └── X-Request-Id ──┘
```

中间件实现：
```rust
// common/src/middleware/request_id.rs
pub async fn request_id_middleware(
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let request_id = req.headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // 注入请求 ID
    req.headers_mut().insert(
        &REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id).unwrap(),
    );

    next.run(req).await
}
```

## 3. 公共模块设计

### 3.1 common 模块结构

```
common/src/
├── lib.rs              # 模块导出
├── config.rs           # 配置管理
├── errors.rs           # 统一错误类型
├── response.rs         # API 响应格式
├── middleware/
│   ├── mod.rs
│   ├── auth.rs         # 认证中间件
│   └── request_id.rs   # 请求 ID 中间件
├── models/
│   ├── mod.rs
│   ├── connection.rs   # 连接配置模型
│   ├── database.rs     # 数据库操作模型
│   └── query.rs        # 查询请求/结果模型
└── utils/
    ├── mod.rs
    ├── id_generator.rs # ID 生成器
    └── sql_validator.rs# SQL 校验器
```

### 3.2 统一响应格式

```rust
#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub code: i32,
    pub message: String,
    pub data: Option<T>,
    pub timestamp: DateTime<Utc>,
    pub service: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self { ... }
    pub fn ok_with_service(data: T, service: &str) -> Self { ... }
    pub fn error(code: i32, message: &str) -> Self { ... }
}
```

### 3.3 统一错误处理

```rust
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("数据库连接失败: {0}")]
    DatabaseConnection(String),

    #[error("连接未找到: {0}")]
    ConnectionNotFound(String),

    #[error("SQL 校验失败: {0}")]
    SqlValidation(String),

    #[error("外部服务错误: {0}")]
    ExternalService(String),

    #[error("配置错误: {0}")]
    Configuration(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            AppError::ConnectionNotFound(_) => (StatusCode::NOT_FOUND, 404),
            AppError::SqlValidation(_) => (StatusCode::BAD_REQUEST, 400),
            // ...
        };

        let body = Json(ApiResponse::<()>::error(code, &self.to_string()));
        (status, body).into_response()
    }
}
```

## 4. 部署架构

### 4.1 Docker 部署

```yaml
# docker-compose.yml
services:
  gateway:
    build:
      context: .
      target: gateway
    ports:
      - "8080:8080"
    depends_on:
      - connection-service
      - query-service
      - ai-service

  connection-service:
    build:
      context: .
      target: connection-service
    ports:
      - "8081:8081"

  query-service:
    build:
      context: .
      target: query-service
    ports:
      - "8082:8082"
    depends_on:
      - connection-service

  ai-service:
    build:
      context: .
      target: ai-service
    ports:
      - "8083:8083"
    depends_on:
      - connection-service
      - query-service
```

### 4.2 多阶段构建

```dockerfile
# Build stage
FROM rust:1.75-alpine AS builder
RUN apk add --no-cache musl-dev openssl-dev
WORKDIR /app
COPY . .
RUN cargo build --release

# Gateway image
FROM alpine:3.19 AS gateway
COPY --from=builder /app/target/release/gateway /usr/local/bin/
CMD ["gateway"]

# Connection service image
FROM alpine:3.19 AS connection-service
COPY --from=builder /app/target/release/connection-service /usr/local/bin/
CMD ["connection-service"]

# ... 其他服务
```

## 5. 扩展性设计

### 5.1 水平扩展

各服务无状态设计，支持水平扩展：

```
                    Load Balancer
                          │
            ┌─────────────┼─────────────┐
            │             │             │
            ▼             ▼             ▼
      ┌──────────┐  ┌──────────┐  ┌──────────┐
      │Gateway-1 │  │Gateway-2 │  │Gateway-3 │
      └──────────┘  └──────────┘  └──────────┘
```

### 5.2 新服务接入

添加新服务的步骤：

1. 创建服务目录和代码
2. 更新 `Cargo.toml` workspace members
3. 更新 `Dockerfile` 添加构建目标
4. 更新 `docker-compose.yml` 添加服务配置
5. 更新 `common/src/config.rs` 添加服务 URL
6. 更新 `gateway/src/proxy.rs` 添加路由代理

## 6. 故障处理

### 6.1 健康检查

各服务提供健康检查端点：

```
GET /api/health

{
  "status": "healthy",
  "service": "ai-service",
  "version": "0.1.0",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### 6.2 聚合健康检查

Gateway 聚合所有服务状态：

```
GET /api/health/all

{
  "status": "healthy",  // 或 "degraded"
  "timestamp": "2024-01-15T10:30:00Z",
  "services": [
    {"name": "connection-service", "healthy": true},
    {"name": "query-service", "healthy": true},
    {"name": "ai-service", "healthy": true}
  ]
}
```

### 6.3 超时与重试

```rust
let client = reqwest::Client::builder()
    .timeout(Duration::from_secs(30))
    .build()?;
```

### 6.4 熔断机制（规划中）

```
正常 → 失败累计 → 熔断开启 → 半开探测 → 恢复
```
