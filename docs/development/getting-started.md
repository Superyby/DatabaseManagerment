# 快速开始

本指南帮助你快速搭建开发环境并运行项目。

## 1. 环境要求

| 工具 | 版本 | 说明 |
|------|------|------|
| Rust | 1.75+ | 编程语言 |
| Cargo | 1.75+ | 包管理器 |
| Docker | 20.10+ | 容器运行时（可选） |
| Docker Compose | 2.0+ | 容器编排（可选） |

## 2. 安装 Rust

```bash
# macOS / Linux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 验证安装
rustc --version
cargo --version
```

## 3. 克隆项目

```bash
git clone <repository-url>
cd DatabaseManagerment
```

## 4. 本地开发

### 4.1 编译检查

```bash
# 检查所有服务编译
cargo check --workspace

# 检查单个服务
cargo check -p ai-service
```

### 4.2 构建项目

```bash
# Debug 构建
cargo build --workspace

# Release 构建
cargo build --workspace --release
```

### 4.3 运行服务

在不同终端中分别启动：

```bash
# 终端 1 - Connection Service
cargo run -p connection-service

# 终端 2 - Query Service
cargo run -p query-service

# 终端 3 - AI Service
export LLM_API_KEY=your-api-key
cargo run -p ai-service

# 终端 4 - Gateway
cargo run -p gateway
```

### 4.4 验证服务

```bash
# 检查网关健康
curl http://localhost:8080/api/health

# 检查所有服务
curl http://localhost:8080/api/health/all

# 检查 AI 服务
curl http://localhost:8083/api/health
```

## 5. Docker 部署

### 5.1 构建并启动

```bash
# 构建并启动所有服务
docker compose up --build

# 后台运行
docker compose up -d --build
```

### 5.2 配置 AI 服务

创建 `.env` 文件：

```bash
# .env
LLM_API_KEY=your-openai-api-key
LLM_BASE_URL=https://api.openai.com/v1
LLM_DEFAULT_MODEL=gpt-4o-mini
```

### 5.3 查看日志

```bash
# 查看所有服务日志
docker compose logs -f

# 查看单个服务
docker compose logs -f ai-service
```

### 5.4 停止服务

```bash
docker compose down
```

## 6. 测试 API

### 6.1 创建数据库连接

```bash
curl -X POST http://localhost:8080/api/connections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "测试数据库",
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "password",
    "database": "test"
  }'
```

### 6.2 测试 AI 查询

```bash
curl -X POST http://localhost:8080/api/ai/query \
  -H "Content-Type: application/json" \
  -d '{
    "request_id": "test_001",
    "question": "查询所有用户",
    "connection_id": "conn_001"
  }'
```

### 6.3 SQL 校验

```bash
curl -X POST http://localhost:8080/api/ai/validate \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM users",
    "connection_id": "conn_001"
  }'
```

## 7. 开发工具

### 7.1 代码格式化

```bash
cargo fmt --all
```

### 7.2 代码检查

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

### 7.3 运行测试

```bash
cargo test --workspace
```

### 7.4 监视模式

安装 cargo-watch：

```bash
cargo install cargo-watch

# 文件变化时自动编译
cargo watch -x check

# 文件变化时自动运行
cargo watch -x 'run -p ai-service'
```

## 8. IDE 配置

### 8.1 VS Code

推荐插件：
- rust-analyzer
- Even Better TOML
- Error Lens

### 8.2 settings.json

```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.check.command": "clippy"
}
```

## 9. 常见问题

### 9.1 编译错误

```bash
# 清理并重新构建
cargo clean
cargo build --workspace
```

### 9.2 端口占用

```bash
# 查看端口占用
lsof -i :8080

# 修改端口
export SERVER_PORT=9080
cargo run -p gateway
```

### 9.3 Docker 构建慢

```bash
# 使用缓存构建
docker compose build --no-cache
```

## 10. 下一步

- 阅读 [编码规范](./coding-standards.md)
- 了解 [部署指南](./deployment.md)
- 查看 [API 文档](../api/api-reference.md)
