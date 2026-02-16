# DatabaseManager

高性能数据库管理后端，基于 Rust + Axum + SQLx 微服务架构。

## 项目结构

```text
DatabaseManager/
├── Cargo.toml                 # Workspace 配置
├── Dockerfile                 # 多阶段构建
├── docker-compose.yml         # 容器编排
│
├── common/                    # 共享模块
│   └── src/
│       ├── config.rs          # 配置管理
│       ├── errors.rs          # 错误处理
│       ├── response.rs        # API 响应
│       ├── middleware/        # 中间件
│       ├── models/            # 数据模型
│       └── utils/             # 工具函数
│
├── gateway/                   # API 网关 (端口 8080)
│   └── src/
│       ├── main.rs            # 入口
│       ├── routes.rs          # 路由
│       ├── handlers.rs        # 处理器
│       └── proxy.rs           # 请求代理
│
├── connection-service/        # 连接管理服务 (端口 8081)
│   └── src/
│       ├── main.rs            # 入口
│       ├── service.rs         # 业务逻辑
│       └── pool_manager.rs    # 连接池管理
│
├── query-service/             # SQL 查询服务 (端口 8082)
│   └── src/
│       ├── main.rs            # 入口
│       └── service.rs         # 查询执行
│
└── ai-service/                # AI 智能查询服务 (端口 8083)
    └── src/
        ├── main.rs            # 入口
        ├── models.rs          # AI 数据模型
        ├── service.rs         # AI 业务逻辑
        ├── handlers.rs        # HTTP 处理器
        └── state.rs           # 应用状态
```

## 服务架构

```
┌─────────────────────────────────────────────────────────────┐
│                      客户端 (前端/API)                        │
└─────────────────────────────┬───────────────────────────────┘
                              │ HTTP/JSON
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Gateway (8080)                           │
│              API 网关 · 路由转发 · 聚合健康检查                │
└────────┬──────────────────┬──────────────────┬──────────────┘
         │                  │                  │
         ▼                  ▼                  ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ Connection      │  │ Query           │  │ AI              │
│ Service (8081)  │  │ Service (8082)  │  │ Service (8083)  │
│                 │  │                 │  │                 │
│ · 连接管理       │  │ · SQL 执行      │  │ · Text2SQL      │
│ · 连接池管理     │  │ · 结果解析      │  │ · 语义理解       │
│ · 连接测试       │  │ · SQL 校验      │  │ · RAG 增强      │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │                  │                  │
         └──────────────────┴──────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                        外部数据库                             │
│     MySQL · PostgreSQL · SQLite · Redis · MongoDB ...        │
└─────────────────────────────────────────────────────────────┘
```

## 快速开始

### 使用启动脚本（推荐）

```bash
# 查看帮助
./start.sh help

# 单容器模式（开发推荐，内存 ~200MB）
./start.sh single

# 多容器模式（生产推荐）
./start.sh multi

# 本地直接运行（不需要 Docker）
./start.sh local

# 停止服务
./start.sh stop

# 查看日志
./start.sh logs
```

### 部署模式对比

| 模式 | 容器数 | 内存 | 命令 |
|------|--------|------|------|
| 单容器 | 1 | ~20MB | `./start.sh single` |
| 多容器 | 4 | ~40MB | `./start.sh multi` |
| 本地 | 0 | ~15MB | `./start.sh local` |

### 手动 Docker 部署

```bash
# 单容器模式
docker compose -f docker-compose.single.yml up -d

# 多容器模式
docker compose up -d
```

### 本地编译运行

```bash
# 检查编译
cargo check --workspace

# 构建所有服务
cargo build --workspace --release

# 运行单个服务
cargo run -p ai-service
```

## API 端点

### Gateway (8080)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/health` | 网关健康检查 |
| GET | `/api/health/all` | 聚合健康检查 |

### Connection Service (8081)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/connections` | 列出所有连接 |
| POST | `/api/connections` | 创建连接 |
| GET | `/api/connections/:id` | 获取连接详情 |
| DELETE | `/api/connections/:id` | 删除连接 |
| POST | `/api/connections/:id/test` | 测试连接 |

### Query Service (8082)

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/query` | 执行 SQL 查询 |
| GET | `/api/health` | 健康检查 |

### AI Service (8083)

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/ai/query` | 自然语言转 SQL |
| POST | `/api/ai/clarify` | 澄清回复 |
| POST | `/api/ai/validate` | SQL 校验 |
| GET | `/api/health` | 健康检查 |

## 环境变量

### 通用配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_HOST` | `0.0.0.0` | 服务监听地址 |
| `SERVER_PORT` | 服务特定 | 服务端口 |
| `RUST_LOG` | `info` | 日志级别 |

### AI Service 配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LLM_BASE_URL` | `https://api.openai.com/v1` | LLM API 地址 |
| `LLM_API_KEY` | - | LLM API 密钥 |
| `LLM_DEFAULT_MODEL` | `gpt-4o-mini` | 快速模型 |
| `LLM_HIGH_PRECISION_MODEL` | `gpt-4o` | 高精度模型 |

## 技术栈

| 组件 | 技术 | 说明 |
|------|------|------|
| Web 框架 | Axum 0.8 | 高性能异步 Web 框架 |
| 异步运行时 | Tokio | 全功能异步运行时 |
| 数据库 | SQLx | 多数据库支持 |
| 日志 | tracing | 结构化日志 |
| API 文档 | Utoipa | OpenAPI 自动生成 |
| 容器化 | Docker | 多阶段构建 |

## 相关文档

### 技术文档 (docs/)

- [文档索引](./docs/README.md) - 完整文档导航

**架构设计**
- [系统架构概览](./docs/architecture/overview.md)
- [微服务设计](./docs/architecture/microservices.md)
- [AI 功能架构](./docs/architecture/ai-architecture.md)

**服务文档**
- [Gateway](./docs/services/gateway.md)
- [Connection Service](./docs/services/connection-service.md)
- [Query Service](./docs/services/query-service.md)
- [AI Service](./docs/services/ai-service.md)

**开发指南**
- [快速开始](./docs/development/getting-started.md)
- [编码规范](./docs/development/coding-standards.md)
- [部署指南](./docs/development/deployment.md)
- [API 参考](./docs/api/api-reference.md)

### 设计文档

- [AI 架构设计](./AI_架构设计.md) - AI 功能详细设计与实现进度
