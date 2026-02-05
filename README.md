## 1. 项目结构
```text
src/
  main.rs             ← 应用入口（路由注册）
  routes/             ← 相当于 controller
    mod.rs
    health.rs         ← 健康检查路由
    database.rs       ← 数据库管理路由
  handlers/           ← （可选）处理具体逻辑（类似 service）
  models/             ← 数据结构（相当于 entity/dto）
    mod.rs
    database.rs       ← DatabaseItem, ConnectionConfig 等
  services/           ← 业务逻辑（如连接数据库、执行 SQL）
  db/                 ← 数据库连接池、SQL 执行
  errors/             ← 自定义错误类型（thiserror）
```