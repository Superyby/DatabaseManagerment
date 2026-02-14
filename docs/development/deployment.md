# 部署指南

本文档描述项目的部署方案和生产配置。

## 1. 快速启动

### 1.1 使用启动脚本（推荐）

```bash
# 查看帮助
./start.sh help

# 单容器模式（开发推荐，内存占用低）
./start.sh single

# 多容器模式（生产推荐）
./start.sh multi

# 本地直接运行（不需要 Docker）
./start.sh local

# 停止服务
./start.sh stop

# 查看日志
./start.sh logs

# 查看状态
./start.sh status
```

### 1.2 部署模式对比

| 部署模式 | 容器数 | 内存占用 | 适用场景 | 启动命令 |
|----------|--------|----------|----------|----------|
| 单容器 | 1 | ~200MB | 开发测试 | `./start.sh single` |
| 多容器 | 4 | ~400MB | 生产环境 | `./start.sh multi` |
| 本地运行 | 0 | ~150MB | 本地调试 | `./start.sh local` |

---

## 2. Docker 优化说明

### 2.1 构建优化

项目采用多阶段构建，大幅优化构建速度和镜像体积：

```
┌─────────────────────────────────────────────────────────┐
│ 阶段 1: deps (依赖缓存层)                                 │
│   - 仅复制 Cargo.toml/Cargo.lock                         │
│   - 预编译所有依赖                                        │
│   - 此层会被 Docker 缓存，修改代码后无需重新编译依赖        │
├─────────────────────────────────────────────────────────┤
│ 阶段 2: builder (应用构建)                                │
│   - 复制源代码                                           │
│   - 只编译业务代码（依赖已缓存）                           │
│   - UPX 压缩二进制文件                                    │
├─────────────────────────────────────────────────────────┤
│ 阶段 3: 最终镜像                                          │
│   - 使用 scratch 空镜像                                   │
│   - 仅包含二进制 + CA 证书                                │
│   - 最终镜像约 10-20MB                                   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 构建时间对比

| 场景 | 优化前 | 优化后 |
|------|--------|--------|
| 首次构建 | ~10 分钟 | ~10 分钟 |
| 修改代码后重建 | ~10 分钟 | ~1-2 分钟 |
| 只改一个服务 | ~10 分钟 | ~30 秒 |

### 2.3 镜像体积对比

| 镜像 | 优化前 (Alpine) | 优化后 (scratch + UPX) |
|------|-----------------|------------------------|
| gateway | ~50MB | ~5MB |
| connection-service | ~60MB | ~8MB |
| query-service | ~60MB | ~8MB |
| ai-service | ~55MB | ~7MB |
| **全合一** | - | ~30MB |

### 2.4 内存限制配置

```yaml
# docker-compose.yml 中的资源限制
services:
  gateway:
    deploy:
      resources:
        limits:
          memory: 128M    # 最大内存
        reservations:
          memory: 32M     # 保留内存

  connection-service:
    deploy:
      resources:
        limits:
          memory: 256M
        reservations:
          memory: 64M
```

---

## 3. 单容器模式详解

单容器模式使用 Supervisor 在一个容器中运行所有服务：

```
┌────────────────────────────────────────┐
│          all-in-one 容器                │
│                                        │
│  ┌──────────────────────────────────┐  │
│  │         Supervisor               │  │
│  └──────────────────────────────────┘  │
│           │    │    │    │             │
│           ▼    ▼    ▼    ▼             │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │
│  │Gate │ │Conn │ │Query│ │ AI  │      │
│  │:8080│ │:8081│ │:8082│ │:8083│      │
│  └─────┘ └─────┘ └─────┘ └─────┘      │
│                                        │
│  服务间通过 127.0.0.1 通信，无网络开销   │
└────────────────────────────────────────┘
```

**使用方式**：

```bash
# 启动
docker compose -f docker-compose.single.yml up -d

# 或使用脚本
./start.sh single
```

**优点**：
- 内存占用低（~200MB vs ~400MB）
- 部署简单，只需管理一个容器
- 服务间通信无网络开销

**缺点**：
- 无法单独扩展某个服务
- 一个服务崩溃影响所有服务

---

## 4. 多容器模式详解

多容器模式每个服务独立运行：

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Network                        │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ Gateway  │  │Connection│  │  Query   │  │   AI    │ │
│  │  :8080   │  │  :8081   │  │  :8082   │  │  :8083  │ │
│  │  128MB   │  │  256MB   │  │  256MB   │  │  256MB  │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────┘
```

**使用方式**：

```bash
# 启动
docker compose up -d

# 或使用脚本
./start.sh multi
```

**优点**：
- 服务独立，故障隔离
- 可单独扩展某个服务
- 适合生产环境

---

## 5. 本地运行模式

不使用 Docker，直接运行编译后的二进制文件：

```bash
./start.sh local
```

这会：
1. 编译所有服务（release 模式）
2. 依次启动 connection-service → query-service → ai-service → gateway
3. 所有服务在后台运行

**停止服务**：

```bash
pkill -f 'target/release'
```

---

## 6. 环境配置

### 6.1 环境变量文件

创建 `.env` 文件（不会被提交到 Git）：

```bash
# .env
# 日志级别
RUST_LOG=info

# AI 服务配置（必填）
LLM_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxx
LLM_BASE_URL=https://api.openai.com/v1
LLM_DEFAULT_MODEL=gpt-4o-mini
LLM_HIGH_PRECISION_MODEL=gpt-4o

# 连接服务配置
MAX_CONNECTIONS=20
CONNECT_TIMEOUT=60
```

### 6.2 配置对比

| 配置项 | 开发环境 | 生产环境 |
|--------|----------|----------|
| RUST_LOG | debug | info |
| MAX_CONNECTIONS | 10 | 50 |
| CONNECT_TIMEOUT | 30 | 60 |
| 内存限制 | 无 | 有 |

---

## 7. 健康检查

### 7.1 检查端点

| 服务 | 端点 | 说明 |
|------|------|------|
| Gateway | http://localhost:8080/api/health | 网关状态 |
| 聚合检查 | http://localhost:8080/api/health/all | 所有服务状态 |
| Connection | http://localhost:8081/api/health | 连接服务状态 |
| Query | http://localhost:8082/api/health | 查询服务状态 |
| AI | http://localhost:8083/api/health | AI 服务状态 |

### 7.2 快速检查命令

```bash
# 单个服务
curl -s http://localhost:8080/api/health | jq

# 所有服务
curl -s http://localhost:8080/api/health/all | jq

# 简单检查
curl -s http://localhost:8080/api/health/all | jq '.status'
```

---

## 8. 生产部署建议

### 8.1 推荐架构

```
                     ┌─────────────────┐
                     │  Load Balancer  │
                     └────────┬────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
              ▼               ▼               ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │Gateway-1 │   │Gateway-2 │   │Gateway-3 │
        └──────────┘   └──────────┘   └──────────┘
              │               │               │
              └───────────────┼───────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
              ▼               ▼               ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │  连接池   │   │  查询池   │   │  AI池    │
        │  服务群   │   │  服务群   │   │  服务群   │
        └──────────┘   └──────────┘   └──────────┘
```

### 8.2 生产配置清单

- [ ] 设置内存限制
- [ ] 配置健康检查
- [ ] 设置重启策略 (`restart: unless-stopped`)
- [ ] 配置日志收集
- [ ] 设置监控告警
- [ ] 使用密钥管理服务存储 API Key
- [ ] 配置网络隔离

### 8.3 资源规划

| 服务 | CPU | 内存 | 实例数 |
|------|-----|------|--------|
| Gateway | 0.5-1 | 128-256MB | 2-3 |
| Connection | 0.5-1 | 256-512MB | 2-3 |
| Query | 1-2 | 256-512MB | 2-3 |
| AI | 1-2 | 256-512MB | 2-3 |

---

## 9. 故障排查

### 9.1 常见问题

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 服务无法启动 | 端口占用 | `lsof -i :8080` |
| 构建失败 | 依赖问题 | `docker compose build --no-cache` |
| 服务间通信失败 | 网络问题 | 检查 Docker 网络 |
| 内存不足 | 限制过低 | 调整 memory limits |

### 9.2 调试命令

```bash
# 查看日志
./start.sh logs

# 查看容器状态
docker compose ps

# 进入容器调试（多容器模式）
docker compose exec ai-service sh

# 查看资源使用
docker stats

# 重新构建（清除缓存）
docker compose build --no-cache
```

### 9.3 清理命令

```bash
# 停止并删除容器
docker compose down

# 删除所有镜像
docker compose down --rmi all

# 清理构建缓存
docker builder prune
```
