# 编码规范

本文档定义项目的编码标准和最佳实践。

## 1. Rust 版本

- Edition: 2021
- 最低版本: 1.75

## 2. 代码格式

### 2.1 格式化工具

使用 `rustfmt` 进行代码格式化：

```bash
cargo fmt --all
```

### 2.2 Clippy 检查

使用 `clippy` 进行代码质量检查：

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

## 3. 命名约定

| 类型 | 风格 | 示例 |
|------|------|------|
| 文件/模块 | snake_case | `pool_manager.rs` |
| 函数/方法 | snake_case | `execute_query()` |
| 变量 | snake_case | `connection_id` |
| 常量 | SCREAMING_SNAKE_CASE | `DEFAULT_PORT` |
| 结构体/枚举 | PascalCase | `QueryResult` |
| 类型参数 | PascalCase | `<T>`, `<E>` |
| 生命周期 | 小写字母 | `'a`, `'static` |

## 4. 项目结构

### 4.1 服务结构

```
service/
├── Cargo.toml
└── src/
    ├── main.rs         # 入口点（保持简洁）
    ├── routes.rs       # 路由定义
    ├── handlers.rs     # HTTP 处理器
    ├── service.rs      # 业务逻辑
    ├── state.rs        # 应用状态
    └── models.rs       # 数据模型（如需要）
```

### 4.2 模块导出

```rust
// lib.rs 或 mod.rs
pub mod config;
pub mod errors;
pub mod models;

// 重新导出常用类型
pub use config::AppConfig;
pub use errors::{AppError, AppResult};
```

## 5. 代码组织

### 5.1 Handler 保持简洁

Handler 只负责请求解析和响应构建，业务逻辑放在 Service 层：

```rust
// Good ✓
pub async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResult>>, AppError> {
    let service = QueryService::new(state.config.clone());
    let result = service.execute(req).await?;
    Ok(Json(ApiResponse::ok(result)))
}

// Bad ✗
pub async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResult>>, AppError> {
    // 不要在 handler 中放业务逻辑
    let pool = state.pool_manager.get_pool(&req.connection_id)?;
    let result = sqlx::query(&req.sql).fetch_all(&pool).await?;
    // ...处理结果
}
```

### 5.2 错误处理

使用 `thiserror` 定义错误类型：

```rust
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("数据库连接失败: {0}")]
    DatabaseConnection(String),

    #[error("连接未找到: {0}")]
    ConnectionNotFound(String),

    #[error("SQL 校验失败: {0}")]
    SqlValidation(String),
}
```

使用 `?` 操作符传播错误：

```rust
pub async fn get_connection(&self, id: &str) -> AppResult<ConnectionConfig> {
    self.pool_manager
        .get_config(id)
        .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))
}
```

### 5.3 异步代码

使用 `async/await`，避免阻塞操作：

```rust
// Good ✓
pub async fn fetch_data(&self) -> AppResult<Data> {
    let response = self.client.get(&url).send().await?;
    response.json().await.map_err(Into::into)
}

// Bad ✗ - 阻塞调用
pub async fn fetch_data(&self) -> AppResult<Data> {
    let response = reqwest::blocking::get(&url)?;  // 阻塞！
    response.json().map_err(Into::into)
}
```

## 6. 文档注释

### 6.1 模块文档

```rust
//! SQL 查询执行服务模块
//!
//! 提供 SQL 查询执行功能，包括：
//! - SQL 校验
//! - 查询执行
//! - 结果解析
```

### 6.2 函数文档

```rust
/// 执行 SQL 查询
///
/// # Arguments
///
/// * `req` - 查询请求，包含连接 ID 和 SQL 语句
///
/// # Returns
///
/// 查询结果，包含列信息和数据行
///
/// # Errors
///
/// * `ConnectionNotFound` - 连接不存在
/// * `SqlValidation` - SQL 校验失败
pub async fn execute(&self, req: QueryRequest) -> AppResult<QueryResult> {
    // ...
}
```

## 7. 测试

### 7.1 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_validator_rejects_drop() {
        let result = SqlValidator::validate("DROP TABLE users");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_query_service() {
        let service = QueryService::new_for_test();
        let result = service.execute(mock_request()).await;
        assert!(result.is_ok());
    }
}
```

### 7.2 测试命名

使用描述性名称：

```rust
#[test]
fn rejects_drop_statement() { ... }

#[test]
fn accepts_select_with_limit() { ... }

#[test]
fn returns_error_when_connection_not_found() { ... }
```

## 8. 日志

使用 `tracing` 进行结构化日志：

```rust
use tracing::{info, warn, error, debug, instrument};

#[instrument(skip(self))]
pub async fn execute(&self, req: QueryRequest) -> AppResult<QueryResult> {
    info!(connection_id = %req.connection_id, "执行查询");

    match self.do_execute(&req).await {
        Ok(result) => {
            info!(rows = result.row_count, "查询成功");
            Ok(result)
        }
        Err(e) => {
            error!(error = %e, "查询失败");
            Err(e)
        }
    }
}
```

## 9. 安全

### 9.1 敏感信息

不要在日志中记录敏感信息：

```rust
// Good ✓
info!(host = %config.host, database = %config.database, "连接数据库");

// Bad ✗
info!(password = %config.password, "连接数据库");  // 泄露密码！
```

### 9.2 SQL 注入防护

使用参数化查询：

```rust
// Good ✓
sqlx::query("SELECT * FROM users WHERE id = ?")
    .bind(user_id)
    .fetch_one(&pool)
    .await?;

// Bad ✗
let sql = format!("SELECT * FROM users WHERE id = {}", user_id);
sqlx::query(&sql).fetch_one(&pool).await?;
```

## 10. 依赖管理

### 10.1 Workspace 依赖

在根 `Cargo.toml` 中定义共享依赖：

```toml
[workspace.dependencies]
axum = "0.8"
tokio = { version = "1.44", features = ["full"] }
```

服务中引用：

```toml
[dependencies]
axum = { workspace = true }
tokio = { workspace = true }
```

### 10.2 特性选择

只启用需要的特性：

```toml
# Good ✓
sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "mysql"] }

# Bad ✗
sqlx = { version = "0.8", features = ["all"] }
```

## 11. Git 提交

### 11.1 提交消息

使用简短的中文描述：

```
✓ 添加 AI 服务骨架
✓ 修复连接池泄漏问题
✓ 重构查询执行逻辑

✗ update code
✗ fix bug
✗ WIP
```

### 11.2 提交粒度

- 每个提交完成一个逻辑变更
- 提交应能独立编译通过
- 避免大型提交，拆分为多个小提交
