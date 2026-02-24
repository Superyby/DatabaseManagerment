//! Database connection pool manager.
//!
//! Manages connection pools for different database types (MySQL, PostgreSQL, SQLite, Redis).

use std::collections::HashMap;
use std::time::Duration;

use common::config::AppConfig;
use common::errors::{AppError, AppResult};
use common::models::connection::{ConnectionConfig, DbType};
use common::models::database::{ColumnDetail, TableInfo, TableSchema};
use common::models::monitor::{
    ConnectionPoolStats, DatabaseInfo, DatabaseStats, MonitorOverview, ProcessInfo,
};
use common::models::query::{ColumnInfo, QueryResult};
use mongodb::bson::doc;
use redis::aio::ConnectionManager as RedisConnectionManager;
use sqlx::{mysql::MySqlPoolOptions, mysql::MySqlRow, postgres::PgPoolOptions, postgres::PgRow, sqlite::SqlitePoolOptions, Row, Column, TypeInfo};
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
    /// MongoDB client.
    MongoDB(mongodb::Client),
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
            DbType::MongoDB => {
                let url = self.build_mongodb_url(config)?;
                let options = mongodb::options::ClientOptions::parse(&url)
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                let client = mongodb::Client::with_options(options)
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                // Verify connection by pinging
                client
                    .database("admin")
                    .run_command(doc! { "ping": 1 })
                    .await
                    .map_err(|e| AppError::DatabaseConnection(e.to_string()))?;
                Ok(DatabasePool::MongoDB(client))
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
            DatabasePool::MongoDB(client) => {
                client
                    .database("admin")
                    .run_command(doc! { "ping": 1 })
                    .await
                    .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;
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
            "mysql://{}:{}@{}:{}/{}?charset=utf8mb4",
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

    fn build_mongodb_url(&self, config: &ConnectionConfig) -> AppResult<String> {
        let host = config
            .host
            .as_deref()
            .ok_or_else(|| AppError::Validation("MongoDB requires host".into()))?;
        let port = config.port.unwrap_or(27017);

        let auth = match (&config.username, &config.password) {
            (Some(user), Some(pass)) if !user.is_empty() => format!("{}:{}@", user, pass),
            _ => String::new(),
        };
        let db = config.database.as_deref().unwrap_or("");
        Ok(format!("mongodb://{}{}:{}/{}", auth, host, port, db))
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
                DatabasePool::MongoDB(_) => Ok(ConnectionPoolStats {
                    active: 1,
                    idle: 0,
                    max_size: self.config.max_connections,
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
            DatabasePool::MongoDB(client) => self.get_mongodb_stats(client).await,
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
            DatabasePool::MongoDB(client) => self.get_mongodb_databases(client).await,
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

    /// Robustly extract a String from a MySQL row.
    /// Falls back to reading raw bytes if the String decode fails (e.g. binary collation).
    fn mysql_get_string(row: &MySqlRow, col: &str) -> String {
        row.try_get::<String, _>(col)
            .unwrap_or_else(|_| {
                row.try_get::<Vec<u8>, _>(col)
                    .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                    .unwrap_or_default()
            })
    }

    /// Robustly extract an optional String from a MySQL row.
    fn mysql_get_opt_string(row: &MySqlRow, col: &str) -> Option<String> {
        row.try_get::<Option<String>, _>(col)
            .unwrap_or_else(|_| {
                row.try_get::<Option<Vec<u8>>, _>(col)
                    .ok()
                    .flatten()
                    .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
            })
    }

    async fn get_mysql_stats(&self, pool: &MySqlPool) -> AppResult<DatabaseStats> {
        let mut stats = DatabaseStats::default();

        // SHOW GLOBAL STATUS
        let rows = sqlx::query("SHOW GLOBAL STATUS")
            .fetch_all(pool)
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        for row in &rows {
            let name: String = Self::mysql_get_string(row, "Variable_name");
            let value: String = Self::mysql_get_string(row, "Value");
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
            let name: String = Self::mysql_get_string(row, "Variable_name");
            let value: String = Self::mysql_get_string(row, "Value");
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
        let rows = sqlx::query(
            "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO
             FROM information_schema.PROCESSLIST
             ORDER BY TIME DESC"
        )
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut processes = Vec::new();
        for row in &rows {
            processes.push(ProcessInfo {
                id: row.try_get::<u64, _>("ID").unwrap_or(0),
                user: Self::mysql_get_string(row, "USER"),
                host: Self::mysql_get_string(row, "HOST"),
                db: Self::mysql_get_opt_string(row, "DB"),
                command: Self::mysql_get_string(row, "COMMAND"),
                time: row.try_get::<i32, _>("TIME").unwrap_or(0) as u64,
                state: Self::mysql_get_opt_string(row, "STATE"),
                info: Self::mysql_get_opt_string(row, "INFO"),
            });
        }
        Ok(processes)
    }

    async fn get_mysql_databases(&self, pool: &MySqlPool) -> AppResult<Vec<DatabaseInfo>> {
        let rows = sqlx::query(
            "SELECT 
                s.SCHEMA_NAME,
                COUNT(t.TABLE_NAME) as tables_count,
                CAST(COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH) / 1024 / 1024, 0) AS DOUBLE) as size_mb
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
                name: Self::mysql_get_string(row, "SCHEMA_NAME"),
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

    // ============== Query Execution ==============

    /// Executes a SQL query against a connection and returns results.
    pub async fn execute_query(&self, id: &str, sql: &str, limit: u32) -> AppResult<QueryResult> {
        let start = std::time::Instant::now();

        let pools = self.pools.read().await;
        let pool = pools
            .get(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        match pool {
            DatabasePool::MySQL(p) => self.execute_mysql_query(p, sql, limit, start).await,
            DatabasePool::Postgres(p) => self.execute_postgres_query(p, sql, limit, start).await,
            _ => Err(AppError::UnsupportedDatabaseType(
                "SQL query execution is only supported for MySQL and PostgreSQL".to_string(),
            )),
        }
    }

    async fn execute_mysql_query(
        &self,
        pool: &MySqlPool,
        sql: &str,
        limit: u32,
        start: std::time::Instant,
    ) -> AppResult<QueryResult> {
        // Safety: add LIMIT if not present
        let sql = Self::ensure_limit(sql, limit);

        let rows: Vec<MySqlRow> = sqlx::query(&sql)
            .fetch_all(pool)
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Extract column info
        let columns: Vec<ColumnInfo> = if let Some(first) = rows.first() {
            first
                .columns()
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name().to_string(),
                    data_type: c.type_info().to_string(),
                    nullable: None,
                })
                .collect()
        } else {
            vec![]
        };

        // Extract row data
        let mut result_rows = Vec::new();
        for row in &rows {
            let mut values = Vec::new();
            for idx in 0..row.columns().len() {
                values.push(Self::mysql_value_to_json(row, idx));
            }
            result_rows.push(values);
        }

        let row_count = result_rows.len();
        Ok(QueryResult {
            columns,
            rows: result_rows,
            row_count,
            affected_rows: None,
            execution_time_ms,
        })
    }

    async fn execute_postgres_query(
        &self,
        pool: &PgPool,
        sql: &str,
        limit: u32,
        start: std::time::Instant,
    ) -> AppResult<QueryResult> {
        let sql = Self::ensure_limit(sql, limit);

        let rows: Vec<PgRow> = sqlx::query(&sql)
            .fetch_all(pool)
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        let columns: Vec<ColumnInfo> = if let Some(first) = rows.first() {
            first
                .columns()
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name().to_string(),
                    data_type: c.type_info().to_string(),
                    nullable: None,
                })
                .collect()
        } else {
            vec![]
        };

        let mut result_rows = Vec::new();
        for row in &rows {
            let mut values = Vec::new();
            for idx in 0..row.columns().len() {
                values.push(Self::pg_value_to_json(row, idx));
            }
            result_rows.push(values);
        }

        let row_count = result_rows.len();
        Ok(QueryResult {
            columns,
            rows: result_rows,
            row_count,
            affected_rows: None,
            execution_time_ms,
        })
    }

    /// Convert a MySQL row value at index to JSON
    fn mysql_value_to_json(row: &MySqlRow, idx: usize) -> serde_json::Value {
        // Try i64
        if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
            return match v {
                Some(n) => serde_json::Value::Number(n.into()),
                None => serde_json::Value::Null,
            };
        }
        // Try f64
        if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
            return match v {
                Some(n) => serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::String(n.to_string())),
                None => serde_json::Value::Null,
            };
        }
        // Try String
        if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
            return match v {
                Some(s) => serde_json::Value::String(s),
                None => serde_json::Value::Null,
            };
        }
        // Try bytes as hex
        if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
            return match v {
                Some(b) => serde_json::Value::String(format!("0x{}", hex_encode(&b))),
                None => serde_json::Value::Null,
            };
        }
        serde_json::Value::Null
    }

    /// Convert a Postgres row value at index to JSON
    fn pg_value_to_json(row: &PgRow, idx: usize) -> serde_json::Value {
        if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
            return match v {
                Some(n) => serde_json::Value::Number(n.into()),
                None => serde_json::Value::Null,
            };
        }
        if let Ok(v) = row.try_get::<Option<i32>, _>(idx) {
            return match v {
                Some(n) => serde_json::Value::Number(n.into()),
                None => serde_json::Value::Null,
            };
        }
        if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
            return match v {
                Some(n) => serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::String(n.to_string())),
                None => serde_json::Value::Null,
            };
        }
        if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
            return match v {
                Some(b) => serde_json::Value::Bool(b),
                None => serde_json::Value::Null,
            };
        }
        if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
            return match v {
                Some(s) => serde_json::Value::String(s),
                None => serde_json::Value::Null,
            };
        }
        serde_json::Value::Null
    }

    /// Ensure SQL has a LIMIT clause
    fn ensure_limit(sql: &str, limit: u32) -> String {
        let upper = sql.to_uppercase();
        if upper.contains("LIMIT") {
            return sql.to_string();
        }
        
        // 移除末尾空白和分号，确保添加 LIMIT 时有空格分隔
        let trimmed = sql.trim_end().trim_end_matches(';');
        if trimmed.is_empty() {
            return sql.to_string();
        }
        
        format!("{} LIMIT {}", trimmed, limit)
    }

    // ============== Schema Methods ==============

    /// Gets table schema for a connection (for AI context).
    pub async fn get_table_schema(&self, id: &str) -> AppResult<TableSchema> {
        let config = self
            .get_connection(id)
            .await
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        let pools = self.pools.read().await;
        let pool = pools
            .get(id)
            .ok_or_else(|| AppError::ConnectionNotFound(id.to_string()))?;

        let database_name = config.database.clone().unwrap_or_default();

        let tables = match pool {
            DatabasePool::MySQL(p) => self.get_mysql_table_schema(p, &database_name).await?,
            DatabasePool::Postgres(p) => self.get_postgres_table_schema(p).await?,
            _ => vec![],
        };

        Ok(TableSchema {
            database: database_name,
            db_type: config.db_type.to_string(),
            tables,
        })
    }

    async fn get_mysql_table_schema(
        &self,
        pool: &MySqlPool,
        database: &str,
    ) -> AppResult<Vec<TableInfo>> {
        let rows = sqlx::query(
            "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY
             FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = ?
             ORDER BY TABLE_NAME, ORDINAL_POSITION
             LIMIT 500",
        )
        .bind(database)
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut tables: Vec<TableInfo> = Vec::new();
        let mut current_table: Option<String> = None;

        for row in &rows {
            let table_name: String = Self::mysql_get_string(row, "TABLE_NAME");
            let col = ColumnDetail {
                name: Self::mysql_get_string(row, "COLUMN_NAME"),
                data_type: Self::mysql_get_string(row, "COLUMN_TYPE"),
                nullable: Self::mysql_get_string(row, "IS_NULLABLE") == "YES",
                key: {
                    let k = Self::mysql_get_string(row, "COLUMN_KEY");
                    if k.is_empty() { None } else { Some(k) }
                },
            };

            if current_table.as_deref() != Some(&table_name) {
                current_table = Some(table_name.clone());
                tables.push(TableInfo {
                    name: table_name,
                    columns: vec![col],
                });
            } else if let Some(t) = tables.last_mut() {
                t.columns.push(col);
            }
        }

        Ok(tables)
    }

    async fn get_postgres_table_schema(
        &self,
        pool: &PgPool,
    ) -> AppResult<Vec<TableInfo>> {
        let rows = sqlx::query(
            "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable,
                    CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PRI'
                         WHEN tc.constraint_type = 'UNIQUE' THEN 'UNI'
                         ELSE NULL END AS column_key
             FROM information_schema.columns c
             LEFT JOIN information_schema.key_column_usage kcu
                ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name
             LEFT JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
             WHERE c.table_schema = 'public'
             ORDER BY c.table_name, c.ordinal_position
             LIMIT 500",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut tables: Vec<TableInfo> = Vec::new();
        let mut current_table: Option<String> = None;

        for row in &rows {
            let table_name: String = row.try_get("table_name").unwrap_or_default();
            let col = ColumnDetail {
                name: row.try_get("column_name").unwrap_or_default(),
                data_type: row.try_get("data_type").unwrap_or_default(),
                nullable: row.try_get::<String, _>("is_nullable").unwrap_or_default() == "YES",
                key: row.try_get::<Option<String>, _>("column_key").unwrap_or(None),
            };

            if current_table.as_deref() != Some(&table_name) {
                current_table = Some(table_name.clone());
                tables.push(TableInfo {
                    name: table_name,
                    columns: vec![col],
                });
            } else if let Some(t) = tables.last_mut() {
                t.columns.push(col);
            }
        }

        Ok(tables)
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

    // ============== MongoDB Monitoring ==============

    async fn get_mongodb_stats(
        &self,
        client: &mongodb::Client,
    ) -> AppResult<DatabaseStats> {
        let db = client.database("admin");
        let result = db
            .run_command(doc! { "serverStatus": 1 })
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut stats = DatabaseStats::default();

        // Server version
        if let Some(v) = result.get_str("version").ok() {
            stats.server_version = Some(format!("MongoDB {}", v));
        }

        // Uptime
        if let Some(up) = result.get_f64("uptime").ok() {
            stats.uptime_seconds = up as u64;
        }

        // Connections
        if let Some(conns) = result.get_document("connections").ok() {
            stats.active_connections = conns.get_i32("current").unwrap_or(0) as u32;
            stats.max_connections = conns.get_i32("available").unwrap_or(0) as u32
                + stats.active_connections;
        }

        // Operations (opcounters)
        if let Some(ops) = result.get_document("opcounters").ok() {
            let insert = ops.get_i64("insert").or(ops.get_i32("insert").map(|v| v as i64)).unwrap_or(0);
            let query = ops.get_i64("query").or(ops.get_i32("query").map(|v| v as i64)).unwrap_or(0);
            let update = ops.get_i64("update").or(ops.get_i32("update").map(|v| v as i64)).unwrap_or(0);
            let delete = ops.get_i64("delete").or(ops.get_i32("delete").map(|v| v as i64)).unwrap_or(0);
            stats.total_queries = (insert + query + update + delete) as u64;
        }

        // Memory
        if let Some(mem) = result.get_document("mem").ok() {
            let resident_mb = mem.get_i32("resident").unwrap_or(0) as u64;
            stats.buffer_pool_size = Some(resident_mb * 1024 * 1024); // MB -> bytes
        }

        if stats.uptime_seconds > 0 {
            stats.queries_per_second =
                stats.total_queries as f64 / stats.uptime_seconds as f64;
        }

        Ok(stats)
    }

    async fn get_mongodb_databases(
        &self,
        client: &mongodb::Client,
    ) -> AppResult<Vec<DatabaseInfo>> {
        let db_names = client
            .list_database_names()
            .await
            .map_err(|e| AppError::DatabaseQuery(e.to_string()))?;

        let mut databases = Vec::new();
        for name in db_names {
            let db = client.database(&name);
            let stats_result = db.run_command(doc! { "dbStats": 1 }).await;
            let size_mb = match stats_result {
                Ok(doc) => {
                    let data_size = doc.get_f64("dataSize")
                        .or(doc.get_i64("dataSize").map(|v| v as f64))
                        .or(doc.get_i32("dataSize").map(|v| v as f64))
                        .unwrap_or(0.0);
                    data_size / 1024.0 / 1024.0
                }
                Err(_) => 0.0,
            };
            databases.push(DatabaseInfo {
                name: name.clone(),
                size_mb,
                tables_count: 0,
            });
        }

        Ok(databases)
    }
}

/// Simple hex encode for binary data display
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
