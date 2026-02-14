# AI Service

AI 智能查询服务，提供 Text2SQL、语义理解、RAG 增强等 AI 能力。

## 1. 服务信息

| 项目 | 值 |
|------|-----|
| 服务名 | ai-service |
| 端口 | 8083 |
| 入口 | `ai-service/src/main.rs` |

## 2. 职责

- 自然语言转 SQL（Text2SQL）
- 多轮对话与澄清
- SQL 安全校验
- 语义理解与口径解释

## 3. 目录结构

```
ai-service/
├── Cargo.toml
└── src/
    ├── main.rs         # 服务入口，OpenAPI 文档
    ├── models.rs       # 数据模型定义
    ├── state.rs        # 应用状态，AI 配置
    ├── routes.rs       # 路由定义
    ├── handlers.rs     # HTTP 处理器
    └── service.rs      # 业务逻辑
```

## 4. API 端点

### 4.1 自然语言查询

```http
POST /api/ai/query
Content-Type: application/json

{
  "request_id": "req_001",
  "question": "统计最近 30 天每个地区的订单总额",
  "connection_id": "conn_mysql_001",
  "context": {
    "session_id": "sess_001",
    "history": []
  },
  "user_permissions": ["orders:read"]
}

Response:
{
  "code": 0,
  "data": {
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
      "applied_rules": []
    }
  }
}
```

### 4.2 澄清回复

```http
POST /api/ai/clarify
Content-Type: application/json

{
  "request_id": "req_002",
  "original_request_id": "req_001",
  "question_id": "q_time_range",
  "answer": "最近 7 天",
  "connection_id": "conn_mysql_001"
}
```

### 4.3 SQL 校验

```http
POST /api/ai/validate
Content-Type: application/json

{
  "sql": "SELECT * FROM orders WHERE status = 'completed'",
  "connection_id": "conn_mysql_001",
  "run_explain": true
}

Response:
{
  "code": 0,
  "data": {
    "valid": true,
    "errors": [],
    "warnings": ["建议添加 LIMIT 限制返回行数"],
    "risk_level": "medium",
    "explain_summary": null
  }
}
```

### 4.4 健康检查

```http
GET /api/health

Response:
{
  "status": "healthy",
  "service": "ai-service",
  "version": "0.1.0",
  "timestamp": "2024-01-15T10:30:00Z",
  "llm_configured": true
}
```

## 5. 数据模型

### 5.1 查询状态

```rust
pub enum QueryStatus {
    Ready,              // SQL 已生成，可执行
    NeedClarification,  // 需要用户澄清
    Failed,             // 生成失败
}
```

### 5.2 澄清问题

```rust
pub struct ClarificationQuestion {
    pub question_id: String,
    pub question: String,
    pub dimension: String,  // time_range / metric / dimension / filter
    pub options: Vec<ClarificationOption>,
    pub default_value: Option<String>,
}
```

### 5.3 血缘摘要

```rust
pub struct LineageSummary {
    pub source_tables: Vec<String>,
    pub key_columns: Vec<String>,
    pub applied_rules: Vec<String>,
}
```

## 6. 配置

### 6.1 AI 配置结构

```rust
pub struct AiConfig {
    pub llm_base_url: String,        // LLM API 地址
    pub llm_api_key: String,         // API 密钥
    pub default_model: String,       // 快速模型
    pub high_precision_model: String,// 高精度模型
    pub max_tokens: u32,             // 最大 Token
    pub confidence_threshold: f64,   // 置信度阈值
}
```

### 6.2 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `8083` | 监听端口 |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | LLM API 地址 |
| `LLM_API_KEY` | - | LLM API 密钥（必填） |
| `LLM_DEFAULT_MODEL` | `gpt-4o-mini` | 快速模型 |
| `LLM_HIGH_PRECISION_MODEL` | `gpt-4o` | 高精度模型 |
| `LLM_MAX_TOKENS` | `4000` | 最大 Token 数 |
| `LLM_CONFIDENCE_THRESHOLD` | `0.7` | 置信度阈值 |
| `CONNECTION_SERVICE_URL` | `http://localhost:8081` | 连接服务地址 |
| `QUERY_SERVICE_URL` | `http://localhost:8082` | 查询服务地址 |

## 7. 核心流程

```
用户问题
    │
    ▼
┌─────────────────┐
│ 获取 Schema     │ ← 从 connection-service
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ RAG 检索        │ ← 历史 SQL、FAQ、指标（规划中）
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ LLM 生成 SQL    │ ← 调用 LLM API
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ SQL 校验        │ ← 安全检查
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 置信度评估      │ ← 是否需要澄清
└────────┬────────┘
         │
         ▼
返回结果
```

## 8. 实现进度

| 功能 | 状态 | 说明 |
|------|------|------|
| 服务骨架 | ✅ 完成 | 入口、路由、处理器 |
| 数据模型 | ✅ 完成 | 请求/响应结构 |
| SQL 校验 | ✅ 基础 | 只读检查、关键词过滤 |
| 健康检查 | ✅ 完成 | 包含 LLM 配置状态 |
| LLM 调用 | 🚧 进行中 | 接口已定义，逻辑待实现 |
| Schema 获取 | 🚧 进行中 | 框架已搭建 |
| RAG 检索 | 📋 规划 | 需选型向量数据库 |
| 多轮对话 | 🚧 骨架 | 数据模型已定义 |
| 语义层 | 📋 规划 | 需设计数据结构 |

## 9. 安全考虑

- SQL 只读检查
- 危险关键词过滤
- 权限校验（规划中）
- 数据脱敏（规划中）
- 审计日志（规划中）

## 10. 下一步开发

1. 实现 LLM 调用逻辑
2. 集成 Schema 获取
3. 完善置信度评估
4. 实现多轮对话上下文管理
