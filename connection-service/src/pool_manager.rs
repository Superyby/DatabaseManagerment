//! Database connection pool manager.
//!
//! Manages connection pools for different database types (MySQL, PostgreSQL, SQLite, Redis).

use std::collections::HashMap;
use std::time::Duration;

use common::config::AppConfig;
use common::errors::{AppError, AppResult};
use common::models::connection::{ConnectionConfig, DbType};
use common::models::monitor::{
    ConnectionPoolStats, DatabaseInfo, DatabaseStats, MonitorOverview, ProcessInfo,
};
use redis::aio::ConnectionManager as RedisConnectionManager;
use sqlx::{mysql::MySqlPoolOptions, postgres::PgPoolOptions, sqlite::SqlitePoolOptions, Row};
use sqlx::{MySqlPool, PgPool, SqlitePool};
use tokio::sync::RwLock;

/// Row from the `connections` MySQL table.
#[derive(sqlx::FromRow)]
struct ConnectionRow {
    id: String,
    name: String,
    db_type: String,
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
    database_name: Option<String>,
    file_path: Option<String>,
    created_at: String,
}

impl ConnectionRow {
    fn into_config(self) -> ConnectionConfig {
        ConnectionConfig {
            id: self.id,
            name: self.name,
            db_type: parse_db_type(&self.db_type),
            host: self.host,
            port: self.port,
            username: self.username,
            password: self.password,
            database: self.database_name,
            file_path: self.file_path,
            created_at: self.created_at,
        }
    }
}

fn parse_db_type(s: &str) -> DbType {
    match s.to_lowercase().as_str() {
        "mysql" => DbType::MySQL,
        "postgres" => DbType::Postgres,
        "sqlite" => DbType::SQLite,
        "redis" => DbType::Redis,
        "mongodb" => DbType::MongoDB,
        "clickhouse" => DbType::ClickHouse,
        "elasticsearch" => DbType::Elasticsearch,
        "oracle" => DbType::Oracle,
        "sqlserver" => DbType::SqlServer,
        "mariadb" => DbType::MariaDB,
        "cassandra" => DbType::Cassandra,
        "influxdb" => DbType::InfluxDB,
        "db2" => DbType::DB2,
        "couchdb" => DbType::CouchDB,
        "neo4j" => DbType::Neo4j,
        "memcached" => DbType::Memcached,
        "hbase" => DbType::HBase,
        "milvus" => DbType::Milvus,
        _ => DbType::MySQL, // fallback
    }
}

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
    /// Unsupported database type.
    Unsupported,
}

/// Manages database connection pools.
///
/// Maintains a collection of connection pools, one for each active database connection.
/// Connection configs are persisted in a MySQL metadata database.
pub struct PoolManager {
    config: AppConfig,
    /// The MySQL pool for metadata persistence (connections table).
    meta_pool: MySqlPool,
    /// Runtime connection pools indexed by connection ID (cache only).
    pools: RwLock<HashMap<String, DatabasePool>>,
}

impl PoolManager {
    /// Creates a new pool manager with MySQL metadata persistence.
    /// Automatically creates the `connections` table and loads existing connections.
    pub async fn new(config: AppConfig, meta_pool: MySqlPool) -> AppResult<Self> {
        let mgr = Self {
            config,
            meta_pool,
            pools: RwLock::new(HashMap::new()),
        };

        // Ensure the connections table exists
        mgr.ensure_table().await?;

        // Load existing connections from DB and try to create pools
        mgr.load_connections_from_db().await;

        Ok(mgr)
    }

    /// Creates the connections table if it does not exist.
    async fn ensure_table(&self) -> AppResult<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS `connections` (
                `id`            VARCHAR(64)   NOT NULL,
                `name`          VARCHAR(100)  NOT NULL,
                `db_type`       VARCHAR(32)   NOT NULL,
                `host`          VARCHAR(255)  DEFAULT NULL,
                `port`          SMALLINT UNSIGNED DEFAULT NULL,
                `username`      VARCHAR(128)  DEFAULT NULL,
                `password`      VARCHAR(512)  DEFAULT NULL,
                `database_name` VARCHAR(128)  DEFAULT NULL,
                `file_path`     VARCHAR(512)  DEFAULT NULL,
                `created_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                KEY `idx_db_type` (`db_type`),
                KEY `idx_created_at` (`created_at`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        .execute(&self.meta_pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(format!("Failed to create connections table: {}", e)))?;

        tracing::info!("Metadata table `connections` ensured");
        Ok(())
    }

    /// Loads all connection configs from MySQL and tries to create pools for each.
    async fn load_connections_from_db(&self) {
        match self.list_connections().await {
            configs if !configs.is_empty() => {
                tracing::info!(count = configs.len(), "Loading saved connections from DB");
                for config in configs {
                    let id = config.id.clone();
                    match self.try_create_pool(&config).await {
                        Ok(pool) => {
                            self.pools.write().await.insert(id.clone(), pool);
                            tracing::info!(id = %id, name = %config.name, "Pool restored");
                        }
                        Err(e) => {
                            tracing::warn!(id = %id, error = %e, "Saved connection pool creation failed (will retry on test)");
                        }
                    }
                }
            }
            _ => {
                tracing::info!("No saved connections found in DB");
            }
        }
    }

    /// Adds a new database connection.
    /// Saves the config to MySQL first, then attempts to create a connection pool.
    pub async fn add_connection(&self, config: ConnectionConfig) -> AppResult<()> {
        let id = config.id.clone();

        // Persist to MySQL (created_at uses DEFAULT CURRENT_TIMESTAMP)
        sqlx::query(
            "INSERT INTO `connections` (`id`, `name`, `db_type`, `host`, `port`, `username`, `password`, `database_name`, `file_path`)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&config.id)
        .bind(&config.name)
        .bind(config.db_type.to_string())
        .bind(&config.host)
        .bind(config.port)
        .bind(&config.username)
        .bind(&config.password)
        .bind(&config.database)
        .bind(&config.file_path)
        .execute(&self.meta_pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(format!("Failed to save connection: {}", e)))?;

        // Then attempt to connect (non-fatal if it fails)
        match self.try_create_pool(&config).await {
            Ok(pool) => {
                self.pools.write().await.insert(id, pool);
            }
            Err(e) => {
                tracing::warn!(id = %id, error = %e, "Connection saved but pool creation failed (will retry on test)");
            }
        }
        Ok(())
    }

    /// Attempts to create a database connection pool.
    async fn try_create_pool(&self, config: &ConnectionConfig) -> AppResult<DatabasePool> {
        let timeout = Duration::from_secs(self.config.connect_timeout_secs);
        let max_connections = self.config.max_connections;

        match &config.db_type {
            DbType::MySQL => {
                let url = self.build_mysql_url(config)?;
                let pool = MySqlPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(timeout)
                    .connect(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                Ok(DatabasePool::MySQL(pool))
            }
            DbType::Postgres => {
                let url = self.build_postgres_url(config)?;
                let pool = PgPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(timeout)
                    .connect(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                Ok(DatabasePool::Postgres(pool))
            }
            DbType::SQLite => {
                let path = config
                    .file_path
                    .as_deref()
                    .ok_or_else(|| AppError::Validation("SQLite requires file_path".into()))?;
                let url = format!("sqlite:{}?mode=rwc", path);
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                Ok(DatabasePool::SQLite(pool))
            }
            DbType::Redis => {
                let url = self.build_redis_url(config)?;
                let client = redis::Client::open(url)
                    .map_err(|e| AppError::RedisConnection(e.to_string()))?;
                let manager = RedisConnectionManager::new(client)
                    .await
                    .map_err(|e| AppError::RedisConnection(e.to_string()))?;
                Ok(DatabasePool::Redis(manager))
            }
            _ => Ok(DatabasePool::Unsupported)
        }
    }

    /// Tests a database connection.
    /// If no pool exists (e.g., initial connection failed), attempts to create one first.
    pub async fn test_connection(&self, id: &str) -> AppResult<Duration> {
        // If no pool exists, try to create one from saved config in DB
        {
            let pools = self.pools.read().await;
            if !pools.contains_key(id) {
                drop(pools);
                if let Some(config) = self.get_connection(id).await {
                    let pool = self.try_create_pool(&config).await?;
                    self.pools.write().await.insert(id.to_string(), pool);
                } else {
                    return Err(AppError::ConnectionNotFound(id.to_string()));
                }
            }
        }

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
            DatabasePool::Unsupported => {
                return Err(AppError::UnsupportedDatabaseType("Connection type not supported yet".into()));
            }
        }

        Ok(start.elapsed())
    }

    /// Removes a database connection from DB and pool cache.
    pub async fn remove_connection(&self, id: &str) -> AppResult<()> {
        self.pools.write().await.remove(id);

        let result = sqlx::query("DELETE FROM `connections` WHERE `id` = ?")
            .bind(id)
            .execute(&self.meta_pool)
            .await
            .map_err(|e| AppError::DatabaseQuery(format!("Failed to delete connection: {}", e)))?;

        if result.rows_affected() == 0 {
            return Err(AppError::ConnectionNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Gets all connection configurations from MySQL.
    pub async fn list_connections(&self) -> Vec<ConnectionConfig> {
        let rows = sqlx::query_as::<_, ConnectionRow>(
            "SELECT `id`, `name`, `db_type`, `host`, `port`, `username`, `password`, `database_name`, `file_path`, CAST(`created_at` AS CHAR) as created_at FROM `connections` ORDER BY `created_at` DESC"
        )
        .fetch_all(&self.meta_pool)
        .await
        .unwrap_or_default();

        rows.into_iter().map(|r| r.into_config()).collect()
    }

    /// Gets a connection configuration by ID from MySQL.
    pub async fn get_connection(&self, id: &str) -> Option<ConnectionConfig> {
        sqlx::query_as::<_, ConnectionRow>(
            "SELECT `id`, `name`, `db_type`, `host`, `port`, `username`, `password`, `database_name`, `file_path`, CAST(`created_at` AS CHAR) as created_at FROM `connections` WHERE `id` = ?"
        )
        .bind(id)
        .fetch_optional(&self.meta_pool)
        .await
        .ok()
        .flatten()
        .map(|r| r.into_config())
    }

    /// Gets a connection pool by ID (from cache).
    pub async fn get_pool(&self, id: &str) -> Option<DatabasePool> {
        self.pools.read().await.get(id).cloned()
    }

    /// Checks if a connection exists in DB.
    pub async fn connection_exists(&self, id: &str) -> bool {
        self.get_connection(id).await.is_some()
    }

    /// Gets the number of saved connections from DB.
    pub async fn connection_count(&self) -> usize {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM `connections`")
            .fetch_one(&self.meta_pool)
            .await
            .unwrap_or((0,));
        row.0 as usize
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

    // ============== Monitoring Methods ==============

    /// Gets the connection pool stats for a given connection.
    pub async fn get_pool_stats(&self, id: &str) -> AppResult<ConnectionPoolStats> {
        let pools = self.pools.read().await;
        match pools.get(id) {
            Some(pool) => match pool {
                DatabasePool::MySQL(p) => Ok(ConnectionPoolStats {
                    active: p.size() as u32 - p.num_idle() as u32,
                    idle: p.num_idle() as u32,
                    max_size: self.config.max_connections,
                    is_connected: true,
                }),
                DatabasePool::Postgres(p) => Ok(ConnectionPoolStats {
                    active: p.size() as u32 - p.num_idle() as u32,
                    idle: p.num_idle() as u32,
                    max_size: self.config.max_connections,
                    is_connected: true,
                }),
                DatabasePool::SQLite(p) => Ok(ConnectionPoolStats {
                    active: p.size() as u32 - p.num_idle() as u32,
                    idle: p.num_idle() as u32,
                    max_size: 1,
                    is_connected: true,
                }),
                DatabasePool::Redis(_) => Ok(ConnectionPoolStats {
                    active: 1,
                    idle: 0,
                    max_size: 1,
                    is_connected: true,
                }),
                DatabasePool::Unsupported => Ok(ConnectionPoolStats {
                    active: 0,
                    idle: 0,
                    max_size: 0,
                    is_connected: false,
                }),
            },
            None => Ok(ConnectionPoolStats {
                active: 0,
                idle: 0,
                max_size: self.config.max_connections,
                is_connected: false,
            }),
        }
    }

    /// Gets database server statistics for a connection.
    pub async fn get_database_stats(&self, id: &str) -> AppResult<DatabaseStats> {
        let pools = self.pools.read().await;
        let pool = pools
            .get(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        match pool {
            DatabasePool::MySQL(p) => self.get_mysql_stats(p).await,
            DatabasePool::Postgres(p) => self.get_postgres_stats(p).await,
            DatabasePool::SQLite(_) => Ok(DatabaseStats {
                server_version: Some("SQLite (embedded)".to_string()),
                ..Default::default()
            }),
            DatabasePool::Redis(manager) => self.get_redis_stats(manager).await,
            DatabasePool::Unsupported => Err(AppError::UnsupportedDatabaseType(
                "Monitoring not supported".into(),
            )),
        }
    }

    /// Gets active processes for a connection.
    pub async fn get_processes(&self, id: &str) -> AppResult<Vec<ProcessInfo>> {
        let pools = self.pools.read().await;
        let pool = pools
            .get(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        match pool {
            DatabasePool::MySQL(p) => self.get_mysql_processes(p).await,
            DatabasePool::Postgres(p) => self.get_postgres_processes(p).await,
            _ => Ok(vec![]),
        }
    }

    /// Lists databases on the server for a connection.
    pub async fn get_databases(&self, id: &str) -> AppResult<Vec<DatabaseInfo>> {
        let pools = self.pools.read().await;
        let pool = pools
            .get(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        match pool {
            DatabasePool::MySQL(p) => self.get_mysql_databases(p).await,
            DatabasePool::Postgres(p) => self.get_postgres_databases(p).await,
            _ => Ok(vec![]),
        }
    }

    /// Gets full monitoring overview.
    pub async fn get_monitor_overview(&self, id: &str) -> AppResult<MonitorOverview> {
        let config = self
            .get_connection(id)
            .await
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        let stats = self.get_database_stats(id).await.unwrap_or_default();
        let pool = self.get_pool_stats(id).await?;

        Ok(MonitorOverview {
            connection_id: id.to_string(),
            connection_name: config.name.clone(),
            db_type: config.db_type.to_string(),
            stats,
            pool,
            timestamp: chrono::Utc::now().to_rfc3339(),
        })
    }

    // ---- MySQL monitoring helpers ----

    async fn get_mysql_stats(&self, pool: &MySqlPool) -> AppResult<DatabaseStats> {
        let mut stats = DatabaseStats::default();

        // SHOW GLOBAL STATUS
        let rows = sqlx::query("SHOW GLOBAL STATUS")
            .fetch_all(pool)
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        for row in &rows {
            let name: String = row.try_get("Variable_name").unwrap_or_default();
            let value: String = row.try_get("Value").unwrap_or_default();
            match name.as_str() {
                "Uptime" => stats.uptime_seconds = value.parse().unwrap_or(0),
                "Questions" | "Queries" => {
                    let v = value.parse().unwrap_or(0u64);
                    if v > stats.total_queries {
                        stats.total_queries = v;
                    }
                }
                "Threads_connected" => {
                    stats.active_connections = value.parse().unwrap_or(0)
                }
                "Slow_queries" => stats.slow_queries = value.parse().unwrap_or(0),
                "Bytes_received" => stats.bytes_received = value.parse().unwrap_or(0),
                "Bytes_sent" => stats.bytes_sent = value.parse().unwrap_or(0),
                "Innodb_buffer_pool_pages_total" => {
                    let pages: u64 = value.parse().unwrap_or(0);
                    stats.buffer_pool_size = Some(pages * 16384); // 16KB per page
                }
                _ => {}
            }
        }

        // SHOW GLOBAL VARIABLES for max_connections and version
        let vars = sqlx::query("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('max_connections', 'version')")
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        for row in &vars {
            let name: String = row.try_get("Variable_name").unwrap_or_default();
            let value: String = row.try_get("Value").unwrap_or_default();
            match name.as_str() {
                "max_connections" => stats.max_connections = value.parse().unwrap_or(0),
                "version" => stats.server_version = Some(format!("MySQL {}", value)),
                _ => {}
            }
        }

        if stats.uptime_seconds > 0 {
            stats.queries_per_second =
                stats.total_queries as f64 / stats.uptime_seconds as f64;
        }

        Ok(stats)
    }

    async fn get_mysql_processes(&self, pool: &MySqlPool) -> AppResult<Vec<ProcessInfo>> {
        let rows = sqlx::query("SHOW PROCESSLIST")
            .fetch_all(pool)
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut processes = Vec::new();
        for row in &rows {
            processes.push(ProcessInfo {
                id: row.try_get::<u64, _>("Id").unwrap_or(0),
                user: row.try_get::<String, _>("User").unwrap_or_default(),
                host: row.try_get::<String, _>("Host").unwrap_or_default(),
                db: row.try_get::<Option<String>, _>("db").unwrap_or(None),
                command: row.try_get::<String, _>("Command").unwrap_or_default(),
                time: row.try_get::<u32, _>("Time").unwrap_or(0) as u64,
                state: row.try_get::<Option<String>, _>("State").unwrap_or(None),
                info: row.try_get::<Option<String>, _>("Info").unwrap_or(None),
            });
        }
        Ok(processes)
    }

    async fn get_mysql_databases(&self, pool: &MySqlPool) -> AppResult<Vec<DatabaseInfo>> {
        let rows = sqlx::query(
            "SELECT 
                s.SCHEMA_NAME as name,
                COUNT(t.TABLE_NAME) as tables_count,
                COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH) / 1024 / 1024, 0) as size_mb
             FROM information_schema.SCHEMATA s
             LEFT JOIN information_schema.TABLES t ON s.SCHEMA_NAME = t.TABLE_SCHEMA
             GROUP BY s.SCHEMA_NAME
             ORDER BY size_mb DESC"
        )
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut databases = Vec::new();
        for row in &rows {
            databases.push(DatabaseInfo {
                name: row.try_get::<String, _>("name").unwrap_or_default(),
                tables_count: row.try_get::<i64, _>("tables_count").unwrap_or(0) as u32,
                size_mb: row.try_get::<f64, _>("size_mb").unwrap_or(0.0),
            });
        }
        Ok(databases)
    }

    // ---- PostgreSQL monitoring helpers ----

    async fn get_postgres_stats(&self, pool: &PgPool) -> AppResult<DatabaseStats> {
        let mut stats = DatabaseStats::default();

        // Server version
        if let Ok(row) = sqlx::query("SHOW server_version").fetch_one(pool).await {
            let ver: String = row.try_get("server_version").unwrap_or_default();
            stats.server_version = Some(format!("PostgreSQL {}", ver));
        }

        // Active connections
        if let Ok(row) = sqlx::query("SELECT count(*) as cnt FROM pg_stat_activity")
            .fetch_one(pool)
            .await
        {
            stats.active_connections = row.try_get::<i64, _>("cnt").unwrap_or(0) as u32;
        }

        // Max connections
        if let Ok(row) = sqlx::query("SHOW max_connections").fetch_one(pool).await {
            let val: String = row.try_get("max_connections").unwrap_or_default();
            stats.max_connections = val.parse().unwrap_or(0);
        }

        // Aggregated stats from pg_stat_database
        if let Ok(row) = sqlx::query(
            "SELECT COALESCE(SUM(xact_commit + xact_rollback), 0) as total_queries,
                    COALESCE(SUM(blks_read), 0) as blks_read,
                    COALESCE(SUM(blks_hit), 0) as blks_hit
             FROM pg_stat_database"
        )
        .fetch_one(pool)
        .await
        {
            stats.total_queries = row.try_get::<i64, _>("total_queries").unwrap_or(0) as u64;
        }

        // Uptime
        if let Ok(row) = sqlx::query(
            "SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint as uptime"
        )
        .fetch_one(pool)
        .await
        {
            stats.uptime_seconds = row.try_get::<i64, _>("uptime").unwrap_or(0) as u64;
        }

        if stats.uptime_seconds > 0 {
            stats.queries_per_second =
                stats.total_queries as f64 / stats.uptime_seconds as f64;
        }

        Ok(stats)
    }

    async fn get_postgres_processes(&self, pool: &PgPool) -> AppResult<Vec<ProcessInfo>> {
        let rows = sqlx::query(
            "SELECT pid, usename, client_addr, datname, state, query, 
                    EXTRACT(EPOCH FROM (now() - query_start))::bigint as duration
             FROM pg_stat_activity
             WHERE state IS NOT NULL
             ORDER BY duration DESC NULLS LAST
             LIMIT 50"
        )
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut processes = Vec::new();
        for row in &rows {
            processes.push(ProcessInfo {
                id: row.try_get::<i32, _>("pid").unwrap_or(0) as u64,
                user: row.try_get::<String, _>("usename").unwrap_or_default(),
                host: row
                    .try_get::<Option<String>, _>("client_addr")
                    .unwrap_or(None)
                    .unwrap_or_else(|| "local".to_string()),
                db: row.try_get::<Option<String>, _>("datname").unwrap_or(None),
                command: row
                    .try_get::<Option<String>, _>("state")
                    .unwrap_or(None)
                    .unwrap_or_else(|| "unknown".to_string()),
                time: row.try_get::<i64, _>("duration").unwrap_or(0) as u64,
                state: row.try_get::<Option<String>, _>("state").unwrap_or(None),
                info: row.try_get::<Option<String>, _>("query").unwrap_or(None),
            });
        }
        Ok(processes)
    }

    async fn get_postgres_databases(&self, pool: &PgPool) -> AppResult<Vec<DatabaseInfo>> {
        let rows = sqlx::query(
            "SELECT d.datname as name,
                    (SELECT count(*) FROM information_schema.tables WHERE table_catalog = d.datname) as tables_count,
                    pg_database_size(d.datname) / 1024.0 / 1024.0 as size_mb
             FROM pg_database d
             WHERE d.datistemplate = false
             ORDER BY size_mb DESC"
        )
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut databases = Vec::new();
        for row in &rows {
            databases.push(DatabaseInfo {
                name: row.try_get::<String, _>("name").unwrap_or_default(),
                tables_count: row.try_get::<i64, _>("tables_count").unwrap_or(0) as u32,
                size_mb: row.try_get::<f64, _>("size_mb").unwrap_or(0.0),
            });
        }
        Ok(databases)
    }

    // ---- Redis monitoring helpers ----

    async fn get_redis_stats(
        &self,
        manager: &RedisConnectionManager,
    ) -> AppResult<DatabaseStats> {
        let mut conn = manager.clone();
        let info: String = redis::cmd("INFO")
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::RedisOperation(e.to_string()))?;

        let mut stats = DatabaseStats::default();
        for line in info.lines() {
            if let Some((key, val)) = line.split_once(':') {
                match key {
                    "uptime_in_seconds" => {
                        stats.uptime_seconds = val.trim().parse().unwrap_or(0)
                    }
                    "connected_clients" => {
                        stats.active_connections = val.trim().parse().unwrap_or(0)
                    }
                    "maxclients" => {
                        stats.max_connections = val.trim().parse().unwrap_or(0)
                    }
                    "total_commands_processed" => {
                        stats.total_queries = val.trim().parse().unwrap_or(0)
                    }
                    "used_memory" => {
                        stats.buffer_pool_size =
                            Some(val.trim().parse().unwrap_or(0));
                    }
                    "redis_version" => {
                        stats.server_version =
                            Some(format!("Redis {}", val.trim()));
                    }
                    _ => {}
                }
            }
        }

        if stats.uptime_seconds > 0 {
            stats.queries_per_second =
                stats.total_queries as f64 / stats.uptime_seconds as f64;
        }

        Ok(stats)
    }
}
