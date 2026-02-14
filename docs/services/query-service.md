# Query Service

SQL 查询执行服务，负责 SQL 校验、执行、结果解析。

## 1. 服务信息

| 项目 | 值 |
|------|-----|
| 服务名 | query-service |
| 端口 | 8082 |
| 入口 | `query-service/src/main.rs` |

## 2. 职责

- SQL 语句校验
- SQL 查询执行
- 结果解析与格式化
- 执行超时控制

## 3. 目录结构

```
query-service/
├── Cargo.toml
└── src/
    ├── main.rs         # 服务入口
    ├── routes.rs       # 路由定义
    ├── handlers.rs     # HTTP 处理器
    ├── service.rs      # 查询执行逻辑
    └── state.rs        # 应用状态
```

## 4. API 端点

### 4.1 执行查询

```http
POST /api/query
Content-Type: application/json

{
  "connection_id": "conn_001",
  "sql": "SELECT * FROM users WHERE status = 'active' LIMIT 100",
  "timeout_ms": 30000
}

Response:
{
  "code": 0,
  "data": {
    "columns": [
      {"name": "id", "type": "INT"},
      {"name": "name", "type": "VARCHAR"},
      {"name": "status", "type": "VARCHAR"}
    ],
    "rows": [
      [1, "Alice", "active"],
      [2, "Bob", "active"]
    ],
    "row_count": 2,
    "execution_time_ms": 15
  }
}
```

### 4.2 健康检查

```http
GET /api/health

Response:
{
  "status": "healthy",
  "service": "query-service",
  "version": "0.1.0"
}
```

## 5. 数据模型

### 5.1 查询请求

```rust
#[derive(Deserialize, Validate)]
pub struct QueryRequest {
    /// 连接 ID
    #[validate(length(min = 1))]
    pub connection_id: String,

    /// SQL 语句
    #[validate(length(min = 1, max = 65535))]
    pub sql: String,

    /// 执行超时（毫秒）
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}
```

### 5.2 查询结果

```rust
#[derive(Serialize)]
pub struct QueryResult {
    /// 列信息
    pub columns: Vec<ColumnInfo>,

    /// 数据行
    pub rows: Vec<Vec<serde_json::Value>>,

    /// 行数
    pub row_count: usize,

    /// 影响的行数（UPDATE/DELETE）
    pub affected_rows: Option<u64>,

    /// 执行耗时（毫秒）
    pub execution_time_ms: u64,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}
```

## 6. SQL 校验

使用 `common/src/utils/sql_validator.rs`：

```rust
impl SqlValidator {
    pub fn validate(sql: &str) -> AppResult<()> {
        // 1. 检查 SQL 非空
        if sql.trim().is_empty() {
            return Err(AppError::SqlValidation("SQL 语句不能为空".into()));
        }

        // 2. 检查危险关键词
        let dangerous = ["DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE"];
        for keyword in dangerous {
            if sql.to_uppercase().contains(keyword) {
                return Err(AppError::SqlValidation(
                    format!("不允许执行 {} 操作", keyword)
                ));
            }
        }

        // 3. 检查多语句
        if sql.matches(';').count() > 1 {
            return Err(AppError::SqlValidation("不允许多语句执行".into()));
        }

        Ok(())
    }
}
```

## 7. 执行流程

```
接收请求
    │
    ▼
┌─────────────┐
│ SQL 校验    │ ← 危险关键词、多语句检查
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ 获取连接池   │ ← 调用 connection-service
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ 执行查询    │ ← 带超时控制
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ 解析结果    │ ← 列信息、行数据
└──────┬──────┘
       │
       ▼
返回响应
```

## 8. 服务间调用

从 connection-service 获取连接池信息：

```rust
async fn get_pool_info(&self, connection_id: &str) -> AppResult<serde_json::Value> {
    let url = format!("{}/internal/pools/{}",
        self.connection_service_url,
        connection_id
    );

    let response = self.http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| AppError::ExternalService(format!("连接服务不可用: {}", e)))?;

    if !response.status().is_success() {
        return Err(AppError::ConnectionNotFound(connection_id.to_string()));
    }

    response.json().await
        .map_err(|e| AppError::ExternalService(format!("响应解析失败: {}", e)))
}
```

## 9. 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `8082` | 监听端口 |
| `CONNECTION_SERVICE_URL` | `http://localhost:8081` | 连接服务地址 |
| `RUST_LOG` | `info` | 日志级别 |

## 10. 实现状态

| 功能 | 状态 | 说明 |
|------|------|------|
| SQL 校验 | ✅ 完成 | 基础校验已实现 |
| 查询执行 | 🚧 进行中 | 框架已搭建，执行逻辑待完善 |
| 结果解析 | 🚧 进行中 | 数据模型已定义 |
| 超时控制 | 📋 规划 | 待实现 |
