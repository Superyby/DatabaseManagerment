//! 查询执行服务模块

use common::errors::{AppError, AppResult};
use common::models::query::{QueryRequest, QueryResult};
use common::utils::SqlValidator;

/// SQL 查询执行服务
pub struct QueryService {
    connection_service_url: String,
    http_client: reqwest::Client,
}

impl QueryService {
    /// 创建新的查询服务实例
    pub fn new(connection_service_url: String, http_client: reqwest::Client) -> Self {
        Self {
            connection_service_url,
            http_client,
        }
    }

    /// 执行 SQL 查询
    pub async fn execute(&self, req: QueryRequest) -> AppResult<QueryResult> {
        // 校验 SQL
        SqlValidator::validate(&req.sql)?;

        // 从连接服务获取连接信息
        let _pool_info = self.get_pool_info(&req.connection_id).await?;

        // TODO: 实现实际的查询执行逻辑
        // 目前返回占位结果
        let start = std::time::Instant::now();
        
        // 占位实现 - 实际实现需要：
        // 1. 从连接服务获取数据库连接
        // 2. 执行 SQL 查询
        // 3. 解析并返回结果
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            affected_rows: None,
            execution_time_ms,
        })
    }

    /// 从连接服务获取连接池信息
    async fn get_pool_info(&self, connection_id: &str) -> AppResult<serde_json::Value> {
        let url = format!("{}/internal/pools/{}", self.connection_service_url, connection_id);
        
        let response = self.http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| AppError::ExternalService(format!("无法连接到连接服务: {}", e)))?;

        if !response.status().is_success() {
            return Err(AppError::ConnectionNotFound(connection_id.to_string()));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| AppError::ExternalService(format!("连接服务返回无效响应: {}", e)))?;

        Ok(json)
    }
}

