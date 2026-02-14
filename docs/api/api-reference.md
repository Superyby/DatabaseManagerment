# API 接口参考

完整的 API 端点文档。

## 1. 通用规范

### 1.1 请求格式

- Content-Type: `application/json`
- 字符编码: `UTF-8`

### 1.2 响应格式

```json
{
  "code": 0,
  "message": "success",
  "data": { ... },
  "timestamp": "2024-01-15T10:30:00Z",
  "service": "ai-service"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| code | number | 状态码，0 表示成功 |
| message | string | 状态消息 |
| data | object | 响应数据 |
| timestamp | string | 响应时间 |
| service | string | 处理服务名 |

### 1.3 错误码

| 代码 | 说明 |
|------|------|
| 0 | 成功 |
| 400 | 请求参数无效 |
| 401 | 未授权 |
| 404 | 资源未找到 |
| 500 | 服务器内部错误 |
| 502 | 上游服务不可用 |

### 1.4 请求头

| Header | 必填 | 说明 |
|--------|------|------|
| Content-Type | 是 | application/json |
| X-Request-Id | 否 | 请求追踪 ID，不传则自动生成 |

---

## 2. Gateway (8080)

### 2.1 网关健康检查

```http
GET /api/health
```

**响应**：
```json
{
  "status": "healthy",
  "service": "gateway",
  "version": "0.1.0",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### 2.2 聚合健康检查

```http
GET /api/health/all
```

**响应**：
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "services": [
    {
      "name": "connection-service",
      "url": "http://connection-service:8081",
      "healthy": true
    },
    {
      "name": "query-service",
      "url": "http://query-service:8082",
      "healthy": true
    },
    {
      "name": "ai-service",
      "url": "http://ai-service:8083",
      "healthy": true
    }
  ]
}
```

---

## 3. Connection Service (8081)

### 3.1 列出所有连接

```http
GET /api/connections
```

**响应**：
```json
{
  "code": 0,
  "data": [
    {
      "id": "conn_001",
      "name": "生产数据库",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "database": "production",
      "created_at": "2024-01-15T10:00:00Z"
    }
  ]
}
```

### 3.2 创建连接

```http
POST /api/connections
```

**请求体**：
```json
{
  "name": "生产数据库",
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "username": "root",
  "password": "secret",
  "database": "production"
}
```

**字段说明**：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | string | 是 | 连接名称 |
| type | string | 是 | 数据库类型 |
| host | string | 是* | 主机地址 |
| port | number | 否 | 端口号 |
| username | string | 是* | 用户名 |
| password | string | 是* | 密码 |
| database | string | 否 | 数据库名 |
| file_path | string | 是* | SQLite 文件路径 |

*: 根据数据库类型有不同要求

**响应**：
```json
{
  "code": 0,
  "data": {
    "id": "conn_001",
    "name": "生产数据库",
    ...
  }
}
```

### 3.3 获取连接详情

```http
GET /api/connections/:id
```

**路径参数**：

| 参数 | 说明 |
|------|------|
| id | 连接 ID |

### 3.4 删除连接

```http
DELETE /api/connections/:id
```

### 3.5 测试连接

```http
POST /api/connections/:id/test
```

**响应**：
```json
{
  "code": 0,
  "data": {
    "success": true,
    "latency_ms": 15,
    "server_version": "8.0.33"
  }
}
```

---

## 4. Query Service (8082)

### 4.1 执行查询

```http
POST /api/query
```

**请求体**：
```json
{
  "connection_id": "conn_001",
  "sql": "SELECT * FROM users LIMIT 10",
  "timeout_ms": 30000
}
```

**字段说明**：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| connection_id | string | 是 | 连接 ID |
| sql | string | 是 | SQL 语句 |
| timeout_ms | number | 否 | 超时时间（毫秒），默认 30000 |

**响应**：
```json
{
  "code": 0,
  "data": {
    "columns": [
      {"name": "id", "type": "INT"},
      {"name": "name", "type": "VARCHAR"}
    ],
    "rows": [
      [1, "Alice"],
      [2, "Bob"]
    ],
    "row_count": 2,
    "execution_time_ms": 15
  }
}
```

### 4.2 健康检查

```http
GET /api/health
```

---

## 5. AI Service (8083)

### 5.1 自然语言查询

```http
POST /api/ai/query
```

**请求体**：
```json
{
  "request_id": "req_001",
  "question": "统计最近 30 天每个地区的订单总额",
  "connection_id": "conn_001",
  "context": {
    "session_id": "sess_001",
    "history": [
      {"role": "user", "content": "查询订单"},
      {"role": "assistant", "content": "请问查询哪个时间范围？"}
    ]
  },
  "user_permissions": ["orders:read", "users:read"]
}
```

**字段说明**：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| request_id | string | 是 | 请求唯一标识 |
| question | string | 是 | 自然语言问题 |
| connection_id | string | 是 | 目标数据库连接 ID |
| context | object | 否 | 对话上下文 |
| context.session_id | string | 否 | 会话 ID |
| context.history | array | 否 | 历史对话记录 |
| user_permissions | array | 否 | 用户权限列表 |

**响应**：
```json
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
      {
        "type": "metric",
        "id": "gmv",
        "description": "销售额指标"
      }
    ],
    "lineage_summary": {
      "source_tables": ["orders"],
      "key_columns": ["region", "amount", "created_at"],
      "applied_rules": []
    }
  }
}
```

**状态说明**：

| status | 说明 |
|--------|------|
| ready | SQL 已生成，可执行 |
| need_clarification | 需要用户澄清 |
| failed | 生成失败 |

### 5.2 澄清回复

```http
POST /api/ai/clarify
```

**请求体**：
```json
{
  "request_id": "req_002",
  "original_request_id": "req_001",
  "question_id": "q_time_range",
  "answer": "最近 7 天",
  "connection_id": "conn_001"
}
```

### 5.3 SQL 校验

```http
POST /api/ai/validate
```

**请求体**：
```json
{
  "sql": "SELECT * FROM orders WHERE status = 'completed'",
  "connection_id": "conn_001",
  "run_explain": true
}
```

**响应**：
```json
{
  "code": 0,
  "data": {
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
}
```

### 5.4 健康检查

```http
GET /api/health
```

**响应**：
```json
{
  "status": "healthy",
  "service": "ai-service",
  "version": "0.1.0",
  "timestamp": "2024-01-15T10:30:00Z",
  "llm_configured": true
}
```

---

## 6. OpenAPI 文档

各服务提供 OpenAPI 3.0 文档：

| 服务 | 文档地址 |
|------|----------|
| Gateway | http://localhost:8080/api-docs/openapi.json |
| Connection Service | http://localhost:8081/api-docs/openapi.json |
| Query Service | http://localhost:8082/api-docs/openapi.json |
| AI Service | http://localhost:8083/api-docs/openapi.json |

可导入 Swagger Editor 或 Postman 查看。
