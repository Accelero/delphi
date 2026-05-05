//! Dev-mode auth injection (gated by the `dev-auth` cargo feature).
//!
//! The dev `AuthContext` is built once at startup by
//! [`bootstrap::seed_dev_world`](super::bootstrap::seed_dev_world); this
//! middleware just clones it into every request's extensions. No per-request
//! DB hits.

use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use axum::Extension;

use crate::auth::context::AuthContext;

pub async fn dev_inject_middleware(
    Extension(ctx): Extension<Arc<AuthContext>>,
    mut req: Request,
    next: Next,
) -> Response {
    req.extensions_mut().insert((*ctx).clone());
    next.run(req).await
}
