# AI Service

AI 智能查询微服务 - 提供 Text2SQL、语义理解、RAG 增强等 AI 能力。

## 功能概述
### 测试

| 功能 | 说明 | 状态 |
|------|------|------|
| Text2SQL | 自然语言转 SQL 查询 | 🚧 骨架 |
| 语义理解 | 指标、维度、口径的统一建模 | 📋 规划 |
| RAG 增强 | 历史 SQL、FAQ、指标定义的检索增强 | 📋 规划 |
| SQL 校验 | 安全性和执行风险评估 | ✅ 基础 |
| 多轮对话 | 澄清与上下文管理 | 🚧 骨架 |

## 目录结构

```
ai-service/
├── Cargo.toml              # 依赖配置
└── src/
    ├── main.rs             # 服务入口，OpenAPI 文档
    ├── models.rs           # 数据模型定义
    ├── state.rs            # 应用状态，AI 配置
    ├── routes.rs           # 路由定义
    ├── handlers.rs         # HTTP 处理器
    └── service.rs          # 业务逻辑
```

## API 接口

### POST /api/ai/query

自然语言转 SQL 查询。

**请求体**：
```json
{
  "request_id": "req_001",
  "question": "统计最近 30 天每个地区的订单总额",
  "connection_id": "conn_mysql_001",
  "context": {
    "session_id": "sess_001",
    "history": [
      {"role": "user", "content": "查询订单数据"},
      {"role": "assistant", "content": "请问您想查询哪个时间范围的订单？"}
    ]
  },
  "user_permissions": ["orders:read", "users:read"]
}
```

**响应体**：
```json
{
  "request_id": "req_001",
  "trace_id": "trace_xyz789",
  "status": "ready",
  "sql": "SELECT region, SUM(amount) AS total FROM orders WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY region",
  "explanation": "按地区分组统计最近 30 天的订单总额",
  "confidence": 0.92,
  "references": [
    {"type": "metric", "id": "gmv", "description": "销售额指标"}
  ],
  "lineage_summary": {
    "source_tables": ["orders"],
    "key_columns": ["region", "amount", "created_at"],
    "applied_rules": ["rule:gmv_calculation"]
  }
}
```

**状态说明**：

| status | 说明 |
|--------|------|
| `ready` | SQL 已生成，可以执行 |
| `need_clarification` | 需要用户澄清 |
| `failed` | 生成失败 |

### POST /api/ai/clarify

处理用户对澄清问题的回复。

**请求体**：
```json
{
  "request_id": "req_002",
  "original_request_id": "req_001",
  "question_id": "q_time_range",
  "answer": "最近 7 天",
  "connection_id": "conn_mysql_001"
}
```

### POST /api/ai/validate

校验 SQL 语句的安全性和执行风险。

**请求体**：
```json
{
  "sql": "SELECT * FROM orders WHERE status = 'completed'",
  "connection_id": "conn_mysql_001",
  "run_explain": true
}
```

**响应体**：
```json
{
  "valid": true,
  "errors": [],
  "warnings": ["建议添加 LIMIT 限制返回行数"],
  "risk_level": "medium",
  "explain_summary": {
    "estimated_rows": 10000,
    "full_table_scan": false,
    "indexes_used": ["idx_status"]
  }
}
```

### GET /api/health

健康检查端点。

**响应体**：
```json
{
  "status": "healthy",
  "service": "ai-service",
  "version": "0.1.0",
  "timestamp": "2024-01-15T10:30:00Z",
  "llm_configured": true
}
```

## 环境变量

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `SERVER_HOST` | 否 | `0.0.0.0` | 服务监听地址 |
| `SERVER_PORT` | 否 | `8083` | 服务端口 |
| `LLM_BASE_URL` | 否 | `https://api.openai.com/v1` | LLM API 基础 URL |
| `LLM_API_KEY` | **是** | - | LLM API 密钥 |
| `LLM_DEFAULT_MODEL` | 否 | `gpt-4o-mini` | 快速模型（简单查询） |
| `LLM_HIGH_PRECISION_MODEL` | 否 | `gpt-4o` | 高精度模型（复杂查询） |
| `LLM_MAX_TOKENS` | 否 | `4000` | 最大 Token 数 |
| `LLM_CONFIDENCE_THRESHOLD` | 否 | `0.7` | 置信度阈值 |
| `CONNECTION_SERVICE_URL` | 否 | `http://localhost:8081` | 连接服务地址 |
| `QUERY_SERVICE_URL` | 否 | `http://localhost:8082` | 查询服务地址 |

## 本地开发

```bash
# 设置环境变量
export LLM_API_KEY=your-api-key

# 运行服务
cargo run -p ai-service

# 测试健康检查
curl http://localhost:8083/api/health

# 测试自然语言查询
curl -X POST http://localhost:8083/api/ai/query \
  -H "Content-Type: application/json" \
  -d '{
    "request_id": "test_001",
    "question": "查询所有用户",
    "connection_id": "conn_001"
  }'
```

## 开发路线图

### Phase 1 - 基础功能 (当前)
- [x] 服务骨架搭建
- [x] API 接口定义
- [x] 基础 SQL 校验
- [ ] LLM 调用实现
- [ ] Schema 获取集成

### Phase 2 - 核心能力
- [ ] 完整 Text2SQL 流程
- [ ] 多轮对话管理
- [ ] 置信度评估
- [ ] 审计日志

### Phase 3 - 增强功能
- [ ] RAG 检索模块
- [ ] 语义层集成
- [ ] EXPLAIN 预检
- [ ] 数据脱敏

### Phase 4 - 生产就绪
- [ ] 模型分级与路由
- [ ] 成本控制与配额
- [ ] 熔断与降级
- [ ] 监控与告警

## 相关文档

- [AI 架构设计](../AI_架构设计.md) - 详细的 AI 功能设计文档
- [项目 README](../README.md) - 项目整体说明
