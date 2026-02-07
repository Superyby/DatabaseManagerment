//! Authentication middleware.
//!
//! Provides request authentication and authorization.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

/// Authentication middleware handler.
///
/// Validates authentication tokens and authorizes requests.
/// Currently a placeholder that allows all requests through.
///
/// # Arguments
/// * `req` - The incoming HTTP request
/// * `next` - The next middleware or handler in the chain
///
/// # Returns
/// The response from downstream handlers, or an error status code.
pub async fn auth_middleware(
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // TODO: Implement actual authentication
    // - Extract token from Authorization header
    // - Validate token
    // - Attach user info to request extensions
    Ok(next.run(req).await)
}

/// Extract bearer token from Authorization header.
pub fn extract_bearer_token(req: &Request<Body>) -> Option<&str> {
    req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}
