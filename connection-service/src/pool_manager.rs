//! Database connection pool manager.
//!
//! Manages connection pools for different database types (MySQL, PostgreSQL, SQLite, Redis).

use std::collections::HashMap;
use std::time::Duration;

use common::config::AppConfig;
use common::errors::{AppError, AppResult};
use common::models::connection::{ConnectionConfig, DbType};
use redis::aio::ConnectionManager as RedisConnectionManager;
use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, sqlite::SqlitePoolOptions};
use sqlx::{MySqlPool, PgPool, SqlitePool};
use tokio::sync::RwLock;

/// Connection pool wrapper for different database types.
#[derive(Clone)]
pub enum DatabasePool {
    /// MySQL connection pool.
    MySQL(MySqlPool),
    /// PostgreSQL connection pool.
    Postgres(PgPool),
    /// SQLite connection pool.
    SQLite(SqlitePool),
    /// Redis connection manager.
    Redis(RedisConnectionManager),
}

/// Manages database connection pools.
///
/// Maintains a collection of connection pools, one for each active database connection.
/// Supports MySQL, PostgreSQL, SQLite, and Redis.
pub struct PoolManager {
    config: AppConfig,
    /// Connection pools indexed by connection ID.
    pools: RwLock<HashMap<String, DatabasePool>>,
    /// Connection configurations indexed by connection ID.
    configs: RwLock<HashMap<String, ConnectionConfig>>,
}

impl PoolManager {
    /// Creates a new pool manager.
    pub fn new(config: AppConfig) -> Self {
        Self {
            config,
            pools: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new database connection.
    pub async fn add_connection(&self, config: ConnectionConfig) -> AppResult<()> {
        let id = config.id.clone();
        let timeout = Duration::from_secs(self.config.connect_timeout_secs);
        let max_connections = self.config.max_connections;

        let pool = match &config.db_type {
            DbType::MySQL => {
                let url = self.build_mysql_url(&config)?;
                let pool = MySqlPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(timeout)
                    .connect(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                DatabasePool::MySQL(pool)
            }
            DbType::Postgres => {
                let url = self.build_postgres_url(&config)?;
                let pool = PgPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(timeout)
                    .connect(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                DatabasePool::Postgres(pool)
            }
            DbType::SQLite => {
                let path = config
                    .file_path
                    .as_deref()
                    .ok_or_else(|| AppError::Validation("SQLite requires file_path".into()))?;
                let url = format!("sqlite:{}?mode=rwc", path);
                let pool = SqlitePoolOptions::new()
                    .max_connections(1) // SQLite is single-writer
                    .connect(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                DatabasePool::SQLite(pool)
            }
            DbType::Redis => {
                let url = self.build_redis_url(&config)?;
                let client = redis::Client::open(url)
                    .map_err(|e| AppError::RedisConnection(e.to_string()))?;
                let manager = RedisConnectionManager::new(client)
                    .await
                    .map_err(|e| AppError::RedisConnection(e.to_string()))?;
                DatabasePool::Redis(manager)
            }
        };

        self.pools.write().await.insert(id.clone(), pool);
        self.configs.write().await.insert(id, config);
        Ok(())
    }

    /// Tests a database connection.
    pub async fn test_connection(&self, id: &str) -> AppResult<Duration> {
        let pools = self.pools.read().await;
        let pool = pools
            .get(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        let start = std::time::Instant::now();

        match pool {
            DatabasePool::MySQL(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;
            }
            DatabasePool::Postgres(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;
            }
            DatabasePool::SQLite(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;
            }
            DatabasePool::Redis(manager) => {
                let mut conn = manager.clone();
                redis::cmd("PING")
                    .query_async::<String>(&mut conn)
                    .await
                    .map_err(|e| AppError::RedisOperation(e.to_string()))?;
            }
        }

        Ok(start.elapsed())
    }

    /// Removes a database connection.
    pub async fn remove_connection(&self, id: &str) -> AppResult<()> {
        self.pools.write().await.remove(id);
        self.configs
            .write()
            .await
            .remove(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;
        Ok(())
    }

    /// Gets all connection configurations.
    pub async fn list_connections(&self) -> Vec<ConnectionConfig> {
        self.configs.read().await.values().cloned().collect()
    }

    /// Gets a connection configuration by ID.
    pub async fn get_connection(&self, id: &str) -> Option<ConnectionConfig> {
        self.configs.read().await.get(id).cloned()
    }

    /// Gets a connection pool by ID.
    pub async fn get_pool(&self, id: &str) -> Option<DatabasePool> {
        self.pools.read().await.get(id).cloned()
    }

    /// Checks if a connection exists.
    pub async fn connection_exists(&self, id: &str) -> bool {
        self.configs.read().await.contains_key(id)
    }

    /// Gets the number of active connections.
    pub async fn connection_count(&self) -> usize {
        self.configs.read().await.len()
    }

    // ============== URL Builders ==============

    fn build_mysql_url(&self, config: &ConnectionConfig) -> AppResult<String> {
        let host = config
            .host
            .as_deref()
            .ok_or_else(|| AppError::Validation("MySQL requires host".into()))?;
        let port = config.port.unwrap_or(3306);
        let username = config.username.as_deref().unwrap_or("root");
        let password = config.password.as_deref().unwrap_or("");
        let database = config.database.as_deref().unwrap_or("");

        Ok(format!(
            "mysql://{}:{}@{}:{}/{}",
            username, password, host, port, database
        ))
    }

    fn build_postgres_url(&self, config: &ConnectionConfig) -> AppResult<String> {
        let host = config
            .host
            .as_deref()
            .ok_or_else(|| AppError::Validation("PostgreSQL requires host".into()))?;
        let port = config.port.unwrap_or(5432);
        let username = config.username.as_deref().unwrap_or("postgres");
        let password = config.password.as_deref().unwrap_or("");
        let database = config.database.as_deref().unwrap_or("postgres");

        Ok(format!(
            "postgres://{}:{}@{}:{}/{}",
            username, password, host, port, database
        ))
    }

    fn build_redis_url(&self, config: &ConnectionConfig) -> AppResult<String> {
        let host = config
            .host
            .as_deref()
            .ok_or_else(|| AppError::Validation("Redis requires host".into()))?;
        let port = config.port.unwrap_or(6379);

        if let Some(password) = &config.password {
            Ok(format!("redis://:{}@{}:{}", password, host, port))
        } else {
            Ok(format!("redis://{}:{}", host, port))
        }
    }
}
