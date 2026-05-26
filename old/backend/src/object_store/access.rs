//! Object-access minting — the swappable seam for direct-to-storage
//! reads and writes.
//!
//! Distinct from [`super::ObjectStore`], which stays the backend's *own*
//! server-side R/W surface (validation read-back, text extraction,
//! commit, cleaner). `AccessMinter` is the **client-facing** seam: the
//! backend makes the authorization decision, then mints a short-lived,
//! scoped handle (URL + method + expiry) the browser uses to talk to the
//! object store **directly** — no proxy, no Traefik in the byte path.
//!
//! Every mode reduces to "hand the client a URL + method + expiry," which
//! is exactly why this seam can later accommodate CDN signed
//! cookies/tokens, STS temp creds, or a streaming proxy without changing
//! callers or the frontend. We implement only [`S3PresignAccess`] now;
//! the deferred drop-ins are documented in
//! `docs/architecture/object-access.md`.

use std::time::Duration;

use async_trait::async_trait;
use axum::http::Method;
use chrono::{DateTime, Utc};

use crate::error::Result;

/// The operation a client wants to perform against an object.
#[derive(Debug, Clone)]
pub enum AccessOp {
    /// Read the whole object (ranged GETs from PDF.js hit the store
    /// directly; the minted URL covers them).
    Download,
    /// Write one part of an in-flight multipart upload.
    UploadPart { upload_id: String, part_number: u16 },
}

/// A client-usable handle: where to go, how, what to send, and until
/// when. The browser fetches/PUTs `url` directly.
#[derive(Debug, Clone)]
pub struct AccessGrant {
    /// The client fetches / PUTs here directly.
    pub url: String,
    pub method: Method,
    /// Headers the client must echo. Usually empty for presigned URLs
    /// (everything travels in the query string).
    pub headers: Vec<(String, String)>,
    pub expires_at: DateTime<Utc>,
}

/// Mints client-usable access handles. The **authorization decision is
/// the caller's** — by the time `mint` runs, the tenant/doc check has
/// already passed; this seam only governs byte *transport*, never the
/// access decision.
///
/// Wired into `AppState` as `Arc<dyn AccessMinter>`. Swapping the impl
/// (CDN / STS / proxy) is a deployment-config change with no caller or
/// frontend churn.
#[async_trait]
pub trait AccessMinter: Send + Sync {
    /// Mint a client-usable handle for `op` on `key`, valid for `ttl`.
    async fn mint(&self, key: &str, op: AccessOp, ttl: Duration) -> Result<AccessGrant>;
}
