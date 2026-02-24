//! API response wrapper types.
//!
//! Provides a unified response format for all API endpoints.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 响应状态码常量
/// 
/// 状态码分类：
/// - 2xx: 成功
/// - 4xx: 客户端错误
/// - 5xx: 服务器错误
/// - 7xx: 业务异常
/// - 8xx: 数据库相关错误
/// - 9xx: 外部服务错误
pub mod code {
    // ==================== 成功 (2xx) ====================
    /// 操作成功
    pub const SUCCESS: i32 = 200;
    /// 创建成功
    pub const CREATED: i32 = 201;
    /// 已接受，异步处理中
    pub const ACCEPTED: i32 = 202;
    /// 无内容（删除成功）
    pub const NO_CONTENT: i32 = 204;

    // ==================== 客户端错误 (4xx) ====================
    /// 请求参数错误
    pub const BAD_REQUEST: i32 = 400;
    /// 未授权（未登录）
    pub const UNAUTHORIZED: i32 = 401;
    /// 禁止访问（无权限）
    pub const FORBIDDEN: i32 = 403;
    /// 资源未找到
    pub const NOT_FOUND: i32 = 404;
    /// 请求方法不允许
    pub const METHOD_NOT_ALLOWED: i32 = 405;
    /// 资源冲突（如重复创建）
    pub const CONFLICT: i32 = 409;
    /// 参数校验失败
    pub const VALIDATION_ERROR: i32 = 422;
    /// 请求过于频繁
    pub const TOO_MANY_REQUESTS: i32 = 429;

    // ==================== 服务器错误 (5xx) ====================
    /// 服务器内部错误
    pub const INTERNAL_ERROR: i32 = 500;
    /// 功能未实现
    pub const NOT_IMPLEMENTED: i32 = 501;
    /// 网关错误
    pub const BAD_GATEWAY: i32 = 502;
    /// 服务不可用
    pub const SERVICE_UNAVAILABLE: i32 = 503;
    /// 网关超时
    pub const GATEWAY_TIMEOUT: i32 = 504;

    // ==================== 业务异常 (7xx) ====================
    /// 通用业务异常
    pub const BUSINESS_ERROR: i32 = 700;
    /// 数据不存在
    pub const DATA_NOT_FOUND: i32 = 701;
    /// 数据已存在（重复）
    pub const DATA_ALREADY_EXISTS: i32 = 702;
    /// 数据状态异常
    pub const DATA_STATE_ERROR: i32 = 703;
    /// 操作不允许
    pub const OPERATION_NOT_ALLOWED: i32 = 704;
    /// 配置错误
    pub const CONFIG_ERROR: i32 = 705;

    // ==================== 数据库相关 (8xx) ====================
    /// 数据库连接失败
    pub const DB_CONNECTION_ERROR: i32 = 800;
    /// 数据库连接不存在
    pub const DB_CONNECTION_NOT_FOUND: i32 = 801;
    /// 数据库连接已存在
    pub const DB_CONNECTION_EXISTS: i32 = 802;
    /// 数据库连接测试失败
    pub const DB_CONNECTION_TEST_FAILED: i32 = 803;
    /// 不支持的数据库类型
    pub const DB_UNSUPPORTED_TYPE: i32 = 804;
    /// SQL 执行错误
    pub const DB_QUERY_ERROR: i32 = 810;
    /// SQL 语法错误
    pub const DB_SQL_SYNTAX_ERROR: i32 = 811;
    /// SQL 不安全（危险操作）
    pub const DB_UNSAFE_SQL: i32 = 812;
    /// SQL 执行超时
    pub const DB_QUERY_TIMEOUT: i32 = 813;
    /// 数据库连接池耗尽
    pub const DB_POOL_EXHAUSTED: i32 = 814;
    /// Redis 连接失败
    pub const REDIS_CONNECTION_ERROR: i32 = 820;
    /// Redis 操作失败
    pub const REDIS_OPERATION_ERROR: i32 = 821;

    // ==================== 外部服务错误 (9xx) ====================
    /// 外部服务调用失败
    pub const EXTERNAL_SERVICE_ERROR: i32 = 900;
    /// 外部服务超时
    pub const EXTERNAL_SERVICE_TIMEOUT: i32 = 901;
    /// 外部服务不可用
    pub const EXTERNAL_SERVICE_UNAVAILABLE: i32 = 902;
    /// 微服务间通信失败
    pub const SERVICE_COMMUNICATION_ERROR: i32 = 910;
}

/// Standard API response wrapper.
///
/// All API endpoints return responses in this format for consistency.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiResponse<T: Serialize> {
    /// 响应状态码（200=成功，400=参数错误，404=未找到，500=服务器错误，700=业务异常）
    pub code: i32,

    /// 响应消息
    pub message: String,

    /// Whether the request was successful.
    pub success: bool,

    /// Response data (present on success).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,

    /// Error details (present on failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,

    /// Response metadata.
    pub meta: ResponseMeta,
}

/// API error details.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    /// Error code for client handling (e.g., "VALIDATION_ERROR", "NOT_FOUND").
    pub code: String,

    /// Human-readable error message.
    pub message: String,

    /// Additional error details (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Response metadata.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ResponseMeta {
    /// Request ID for tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Response timestamp.
    pub timestamp: DateTime<Utc>,

    /// Request processing time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// Service name that handled the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
}

impl Default for ResponseMeta {
    fn default() -> Self {
        Self {
            request_id: None,
            timestamp: Utc::now(),
            duration_ms: None,
            service: None,
        }
    }
}

impl ResponseMeta {
    /// Creates a new ResponseMeta with service name.
    pub fn with_service(service: impl Into<String>) -> Self {
        Self {
            service: Some(service.into()),
            ..Default::default()
        }
    }
}

/// Pagination information for list responses.
#[derive(Debug, Serialize, ToSchema)]
pub struct Pagination {
    /// Current page number (1-based).
    pub page: u32,

    /// Number of items per page.
    pub page_size: u32,

    /// Total number of items.
    pub total: u64,

    /// Total number of pages.
    pub total_pages: u32,

    /// Whether there is a next page.
    pub has_next: bool,

    /// Whether there is a previous page.
    pub has_prev: bool,
}

impl Pagination {
    /// Creates pagination info from total count and page parameters.
    pub fn new(page: u32, page_size: u32, total: u64) -> Self {
        let total_pages = ((total as f64) / (page_size as f64)).ceil() as u32;
        Self {
            page,
            page_size,
            total,
            total_pages,
            has_next: page < total_pages,
            has_prev: page > 1,
        }
    }
}

/// Paginated list response.
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedData<T: Serialize> {
    /// List of items.
    pub items: Vec<T>,

    /// Pagination information.
    pub pagination: Pagination,
}

impl<T: Serialize> PaginatedData<T> {
    /// Creates a new paginated data response.
    pub fn new(items: Vec<T>, page: u32, page_size: u32, total: u64) -> Self {
        Self {
            items,
            pagination: Pagination::new(page, page_size, total),
        }
    }
}

impl<T: Serialize> ApiResponse<T> {
    /// Creates a successful response with data.
    pub fn ok(data: T) -> Self {
        Self {
            code: code::SUCCESS,
            message: "操作成功".to_string(),
            success: true,
            data: Some(data),
            error: None,
            meta: ResponseMeta::default(),
        }
    }

    /// Creates a successful response with custom message.
    pub fn ok_with_msg(data: T, msg: impl Into<String>) -> Self {
        Self {
            code: code::SUCCESS,
            message: msg.into(),
            success: true,
            data: Some(data),
            error: None,
            meta: ResponseMeta::default(),
        }
    }

    /// Creates a successful response with data and request ID.
    pub fn ok_with_request_id(data: T, request_id: impl Into<String>) -> Self {
        Self {
            code: code::SUCCESS,
            message: "操作成功".to_string(),
            success: true,
            data: Some(data),
            error: None,
            meta: ResponseMeta {
                request_id: Some(request_id.into()),
                ..Default::default()
            },
        }
    }

    /// Creates a successful response with data and duration.
    pub fn ok_with_duration(data: T, duration_ms: u64) -> Self {
        Self {
            code: code::SUCCESS,
            message: "操作成功".to_string(),
            success: true,
            data: Some(data),
            error: None,
            meta: ResponseMeta {
                duration_ms: Some(duration_ms),
                ..Default::default()
            },
        }
    }

    /// Creates a successful response with service name.
    pub fn ok_with_service(data: T, service: impl Into<String>) -> Self {
        Self {
            code: code::SUCCESS,
            message: "操作成功".to_string(),
            success: true,
            data: Some(data),
            error: None,
            meta: ResponseMeta::with_service(service),
        }
    }

    /// Sets the request ID on the response.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.meta.request_id = Some(request_id.into());
        self
    }

    /// Sets the duration on the response.
    pub fn with_duration(mut self, duration_ms: u64) -> Self {
        self.meta.duration_ms = Some(duration_ms);
        self
    }

    /// Sets the service name on the response.
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.meta.service = Some(service.into());
        self
    }
}

impl ApiResponse<()> {
    /// Creates an error response with code 700 (business error).
    pub fn err(error_code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            code: code::BUSINESS_ERROR,
            message: msg.into(),
            success: false,
            data: None,
            error: Some(ApiError {
                code: error_code.into(),
                message: "业务异常".to_string(),
                details: None,
            }),
            meta: ResponseMeta::default(),
        }
    }

    /// Creates an error response with custom status code.
    pub fn err_with_code(status_code: i32, error_code: impl Into<String>, msg: impl Into<String>) -> Self {
        let message = msg.into();
        Self {
            code: status_code,
            message: message.clone(),
            success: false,
            data: None,
            error: Some(ApiError {
                code: error_code.into(),
                message,
                details: None,
            }),
            meta: ResponseMeta::default(),
        }
    }

    /// Creates an error response with details.
    pub fn err_with_details(
        error_code: impl Into<String>,
        msg: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            code: code::BUSINESS_ERROR,
            message: msg.into(),
            success: false,
            data: None,
            error: Some(ApiError {
                code: error_code.into(),
                message: "业务异常".to_string(),
                details: Some(details),
            }),
            meta: ResponseMeta::default(),
        }
    }

    /// Creates a success response without data.
    pub fn success() -> Self {
        Self {
            code: code::SUCCESS,
            message: "操作成功".to_string(),
            success: true,
            data: None,
            error: None,
            meta: ResponseMeta::default(),
        }
    }
}

/// Empty response for delete operations.
#[derive(Debug, Serialize, ToSchema)]
pub struct EmptyData;
