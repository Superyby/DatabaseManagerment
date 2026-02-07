//! API response wrapper types.
//!
//! Provides a unified response format for all API endpoints.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

/// Standard API response wrapper.
///
/// All API endpoints return responses in this format for consistency.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiResponse<T: Serialize> {
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
#[derive(Debug, Serialize, ToSchema)]
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
#[derive(Debug, Serialize, ToSchema)]
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
            success: true,
            data: Some(data),
            error: None,
            meta: ResponseMeta::default(),
        }
    }

    /// Creates a successful response with data and request ID.
    pub fn ok_with_request_id(data: T, request_id: impl Into<String>) -> Self {
        Self {
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
    /// Creates an error response.
    pub fn err(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
                details: None,
            }),
            meta: ResponseMeta::default(),
        }
    }

    /// Creates an error response with details.
    pub fn err_with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
                details: Some(details),
            }),
            meta: ResponseMeta::default(),
        }
    }

    /// Creates a success response without data.
    pub fn success() -> Self {
        Self {
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
