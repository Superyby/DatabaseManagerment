//! 连接管理服务模块

use std::sync::Arc;
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use common::errors::{AppError, AppResult};
use common::models::connection::{ConnectionItem, CreateConnectionRequest};
use crate::pool_manager::PoolManager;

/// 连接服务 Trait
#[async_trait]
pub trait ConnectionServiceTrait: Send + Sync {
    /// 列出所有连接
    async fn list(&self) -> Vec<ConnectionItem>;
    
    /// 创建新连接
    async fn create(&self, req: CreateConnectionRequest) -> AppResult<ConnectionItem>;
    
    /// 根据 ID 获取连接
    async fn get(&self, id: &str) -> AppResult<ConnectionItem>;
    
    /// 根据 ID 删除连接
    async fn delete(&self, id: &str) -> AppResult<()>;
    
    /// 测试连接
    async fn test(&self, id: &str) -> AppResult<u64>;
}

/// 数据库连接管理服务
pub struct ConnectionService {
    pool_manager: Arc<PoolManager>,
}

impl ConnectionService {
    /// 创建新的连接服务实例
    pub fn new(pool_manager: Arc<PoolManager>) -> Self {
        Self { pool_manager }
    }
}

#[async_trait]
impl ConnectionServiceTrait for ConnectionService {
    async fn list(&self) -> Vec<ConnectionItem> {
        self.pool_manager
            .list_connections()
            .await
            .into_iter()
            .map(ConnectionItem::from)
            .collect()
    }

    async fn create(&self, req: CreateConnectionRequest) -> AppResult<ConnectionItem> {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().to_rfc3339();
        let config = req.into_config(id.clone(), created_at);

        // 添加到连接池管理器（会进行验证并建立连接）
        self.pool_manager.add_connection(config.clone()).await?;

        tracing::info!(id = %id, name = %config.name, "连接已创建");
        Ok(ConnectionItem::from(config))
    }

    async fn get(&self, id: &str) -> AppResult<ConnectionItem> {
        self.pool_manager
            .get_connection(id)
            .await
            .map(ConnectionItem::from)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))
    }

    async fn delete(&self, id: &str) -> AppResult<()> {
        self.pool_manager.remove_connection(id).await?;
        tracing::info!(id = %id, "连接已删除");
        Ok(())
    }

    async fn test(&self, id: &str) -> AppResult<u64> {
        let latency = self.pool_manager.test_connection(id).await?;
        Ok(latency.as_millis() as u64)
    }
}

