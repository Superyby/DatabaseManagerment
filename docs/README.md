# 技术文档

DatabaseManager 项目技术文档索引。

## 快速启动

```bash
# 单容器模式（开发推荐，内存 ~200MB）
./start.sh single

# 多容器模式（生产推荐）
./start.sh multi

# 本地运行（无 Docker）
./start.sh local

# 查看帮助
./start.sh help
```

## 文档结构

```
docs/
├── README.md                          # 文档索引（本文件）
│
├── architecture/                      # 架构设计
│   ├── overview.md                    # 系统架构概览
│   ├── microservices.md               # 微服务设计
│   └── ai-architecture.md             # AI 功能架构
│
├── services/                          # 服务设计
│   ├── gateway.md                     # API 网关
│   ├── connection-service.md          # 连接管理服务
│   ├── query-service.md               # 查询执行服务
│   └── ai-service.md                  # AI 智能查询服务
│
├── api/                               # API 文档
│   └── api-reference.md               # API 接口参考
│
└── development/                       # 开发指南
    ├── getting-started.md             # 快速开始
    ├── coding-standards.md            # 编码规范
    └── deployment.md                  # 部署指南
```

## 快速导航

### 架构设计
- [系统架构概览](./architecture/overview.md) - 整体架构、技术栈、设计原则
- [微服务设计](./architecture/microservices.md) - 服务划分、通信机制、部署模式
- [AI 功能架构](./architecture/ai-architecture.md) - Text2SQL、RAG、语义层设计

### 服务文档
- [Gateway 网关](./services/gateway.md) - 路由转发、聚合健康检查
- [Connection Service](./services/connection-service.md) - 连接管理、连接池
- [Query Service](./services/query-service.md) - SQL 执行、结果解析
- [AI Service](./services/ai-service.md) - 智能查询、自然语言处理

### API 文档
- [API 接口参考](./api/api-reference.md) - 完整的 API 端点文档

### 开发指南
- [快速开始](./development/getting-started.md) - 环境搭建、本地运行
- [编码规范](./development/coding-standards.md) - 代码风格、命名约定
- [部署指南](./development/deployment.md) - Docker 优化、部署模式、生产配置

## 部署模式对比

| 模式 | 容器 | 内存 | 适用场景 |
|------|------|------|----------|
| 单容器 | 1 | ~200MB | 开发测试 |
| 多容器 | 4 | ~400MB | 生产环境 |
| 本地 | 0 | ~150MB | 本地调试 |

## 版本信息

| 项目 | 版本 |
|------|------|
| DatabaseManager | 0.1.0 |
| Rust Edition | 2021 |
| Axum | 0.8 |
| 文档更新日期 | 2024-01 |
