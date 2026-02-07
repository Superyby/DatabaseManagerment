//! Request ID middleware.
//!
//! Generates and attaches unique request IDs for request tracing and logging.

use axum::{
    body::Body,
    http::{header::HeaderName, HeaderValue, Request},
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

/// Header name for request ID.
pub static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Request ID middleware handler.
///
/// Generates a unique ID for each request and attaches it to both
/// the request extensions and response headers.
///
/// If the request already has an X-Request-ID header, it will be used instead.
///
/// # Arguments
/// * `req` - The incoming HTTP request
/// * `next` - The next middleware or handler in the chain
///
/// # Returns
/// The response with X-Request-ID header attached.
pub async fn request_id_middleware(mut req: Request<Body>, next: Next) -> Response {
    // Check for existing request ID header
    let request_id = req
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Store in request extensions for handlers to access
    req.extensions_mut().insert(RequestId(request_id.clone()));

    // Create a tracing span with request ID
    let span = tracing::info_span!(
        "request",
        request_id = %request_id,
        method = %req.method(),
        uri = %req.uri(),
    );
    let _guard = span.enter();

    // Process request
    let mut response = next.run(req).await;

    // Add request ID to response headers
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER.clone(), value);
    }

    response
}

/// Request ID wrapper for storing in request extensions.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl RequestId {
    /// Returns the request ID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Creates a new request ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
