# Connection Service

数据库连接管理服务，负责连接配置的 CRUD、连接池管理、连接测试。

## 1. 服务信息

| 项目 | 值 |
|------|-----|
| 服务名 | connection-service |
| 端口 | 8081 |
| 入口 | `connection-service/src/main.rs` |

## 2. 职责

- 数据库连接配置管理（CRUD）
- 动态连接池管理
- 连接可用性测试
- 支持多种数据库类型

## 3. 目录结构

```
connection-service/
├── Cargo.toml
└── src/
    ├── main.rs           # 服务入口
    ├── routes.rs         # 路由定义
    ├── handlers.rs       # HTTP 处理器
    ├── service.rs        # 业务逻辑（Trait + 实现）
    ├── pool_manager.rs   # 连接池管理
    └── state.rs          # 应用状态
```

## 4. 支持的数据库类型

```rust
pub enum DbType {
    MySQL,
    MariaDB,
    Postgres,
    SQLite,
    Redis,
    MongoDB,
    Oracle,
    SqlServer,
    ClickHouse,
    Cassandra,
    Elasticsearch,
    Neo4j,
    DynamoDB,
    CockroachDB,
    TiDB,
    OceanBase,
    PolarDB,
    Hive,
}
```

## 5. API 端点

### 5.1 列出所有连接

```http
GET /api/connections

Response:
{
  "code": 0,
  "message": "success",
  "data": [
    {
      "id": "conn_001",
      "name": "生产数据库",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "database": "production"
    }
  ]
}
```

### 5.2 创建连接

```http
POST /api/connections
Content-Type: application/json

{
  "name": "生产数据库",
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "username": "root",
  "password": "secret",
  "database": "production"
}

Response:
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "conn_001",
    "name": "生产数据库",
    ...
  }
}
```

### 5.3 获取连接详情

```http
GET /api/connections/:id

Response:
{
  "code": 0,
  "data": { ... }
}
```

### 5.4 删除连接

```http
DELETE /api/connections/:id

Response:
{
  "code": 0,
  "message": "连接已删除"
}
```

### 5.5 测试连接

```http
POST /api/connections/:id/test

Response:
{
  "code": 0,
  "data": {
    "success": true,
    "latency_ms": 15,
    "server_version": "8.0.33"
  }
}
```

## 6. 连接池管理

### 6.1 架构设计

```rust
pub struct PoolManager {
    config: AppConfig,
    mysql_pools: RwLock<HashMap<String, MySqlPool>>,
    pg_pools: RwLock<HashMap<String, PgPool>>,
    sqlite_pools: RwLock<HashMap<String, SqlitePool>>,
    connections: RwLock<HashMap<String, ConnectionConfig>>,
}
```

### 6.2 连接池配置

```rust
let pool = MySqlPoolOptions::new()
    .max_connections(config.max_connections)    // 最大连接数
    .acquire_timeout(Duration::from_secs(30))   // 获取超时
    .idle_timeout(Duration::from_secs(600))     // 空闲超时
    .connect(&connection_string)
    .await?;
```

### 6.3 连接池生命周期

```
创建连接配置 → 初始化连接池 → 使用连接 → 空闲回收 → 删除连接 → 关闭连接池
```

## 7. 服务层设计

使用 Trait 模式便于测试：

```rust
#[async_trait]
pub trait ConnectionServiceTrait: Send + Sync {
    async fn create(&self, config: ConnectionConfig) -> AppResult<ConnectionConfig>;
    async fn get(&self, id: &str) -> AppResult<ConnectionConfig>;
    async fn list(&self) -> AppResult<Vec<ConnectionConfig>>;
    async fn delete(&self, id: &str) -> AppResult<()>;
    async fn test(&self, id: &str) -> AppResult<TestResult>;
}

pub struct ConnectionService {
    pool_manager: Arc<PoolManager>,
}

impl ConnectionServiceTrait for ConnectionService {
    // 实现...
}
```

## 8. 内部接口

供其他服务调用的内部接口：

```http
GET /internal/pools/:id

Response:
{
  "connection_id": "conn_001",
  "pool_type": "mysql",
  "active_connections": 5,
  "idle_connections": 3
}
```

## 9. 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `8081` | 监听端口 |
| `MAX_CONNECTIONS` | `10` | 每个连接池最大连接数 |
| `CONNECT_TIMEOUT` | `30` | 连接超时（秒） |
| `DATA_DIR` | `./data` | 配置持久化目录 |
| `RUST_LOG` | `info` | 日志级别 |

## 10. 安全考虑

- 密码不记录到日志
- 响应中不返回密码字段
- 连接字符串加密存储（规划中）
