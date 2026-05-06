//! Shared test harness for backend integration tests.
//!
//! Each integration-test file (`auth_pipeline.rs`, `chat_streaming.rs`, …)
//! runs as its own binary, so we keep this module small and explicit
//! rather than relying on globals. Build the world you need with
//! [`TestApp::build()`] and drive it with `tower::ServiceExt::oneshot`.

#![allow(dead_code)] // each test binary uses a different subset of helpers

pub mod fake_llm;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use surrealdb::RecordId;
use tower::ServiceExt;

use delphi::api;
use delphi::auth::{
    self, AuthMode, ClaimsExtractor, HeaderClaimsExtractor, HeaderConfig, IdentityDeps,
};
use delphi::state::AppState;
use delphi::storage::SurrealStorage;

use crate::common::fake_llm::FakeLlmClient;

/// Everything a test needs to drive the backend in-process: the fully-built
/// axum [`Router`], plus a handle to the in-memory SurrealDB for direct
/// inspection / seeding outside the HTTP path.
pub struct TestApp {
    pub router: Router,
    pub storage: Arc<SurrealStorage>,
    pub default_tenant_id: RecordId,
    pub default_tenant_slug: String,
}

impl TestApp {
    /// Build a fresh app: in-memory SurrealDB, schema applied, default tenant
    /// created, header-mode auth, fake LLM. Each call is independent — no
    /// shared state between tests.
    pub async fn build() -> Self {
        // Unique namespace per test process so parallel `cargo test` runs
        // can't see each other (the in-memory engine is process-local
        // anyway, but this keeps things explicit).
        let storage = Arc::new(
            SurrealStorage::in_memory("delphi_test", "main")
                .await
                .expect("connect in-memory surreal"),
        );

        // Same code path the real backend takes on startup: applies the
        // canonical schema. `IF NOT EXISTS` everywhere — safe to re-run.
        use delphi::storage::Storage;
        storage
            .init_schema()
            .await
            .expect("init schema in test db");

        let default_tenant_slug = "test".to_string();
        let default_tenant_id = auth::resolve_default_tenant(storage.db(), &default_tenant_slug)
            .await
            .expect("resolve default tenant");

        let identity_deps = IdentityDeps {
            db: storage.db().clone(),
            default_tenant_slug: default_tenant_slug.clone(),
            default_tenant_id: default_tenant_id.clone(),
        };

        let mode = AuthMode::Header(HeaderConfig {
            default_tenant_slug: default_tenant_slug.clone(),
        });

        let extractor: Arc<dyn ClaimsExtractor> = Arc::new(HeaderClaimsExtractor::new());

        let state = AppState {
            storage: storage.clone(),
            llm: Arc::new(FakeLlmClient::default()),
        };

        let router = api::build_router(state, None, &mode, identity_deps, extractor);

        TestApp {
            router,
            storage,
            default_tenant_id,
            default_tenant_slug,
        }
    }

    /// Issue a request through the full middleware stack. Consumes the body
    /// and returns the parsed JSON / raw bytes / status.
    pub async fn send(&self, req: Request<Body>) -> TestResponse {
        let res = self
            .router
            .clone()
            .oneshot(req)
            .await
            .expect("router oneshot");
        let status = res.status();
        let bytes = res
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        TestResponse { status, bytes }
    }
}

pub struct TestResponse {
    pub status: StatusCode,
    pub bytes: bytes::Bytes,
}

impl TestResponse {
    pub fn json<T: DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.bytes).expect("decode response JSON")
    }

    pub fn text(&self) -> String {
        String::from_utf8(self.bytes.to_vec()).expect("response is valid UTF-8")
    }
}

/// Build a `Request` with the canonical `X-Auth-*` headers a real proxy
/// would set. Defaults to a dev-shaped identity in the default tenant —
/// mutate with `.tenant()` / `.roles()` before calling `.build()`.
pub struct AuthRequestBuilder {
    sub: String,
    iss: String,
    email: String,
    name: Option<String>,
    tenant_slug: Option<String>,
    roles: Option<String>,
}

impl Default for AuthRequestBuilder {
    fn default() -> Self {
        Self {
            sub: "test-user".into(),
            iss: "https://idp.test/".into(),
            email: "test@delphi.test".into(),
            name: Some("Test User".into()),
            tenant_slug: None,
            roles: None,
        }
    }
}

impl AuthRequestBuilder {
    pub fn tenant(mut self, slug: impl Into<String>) -> Self {
        self.tenant_slug = Some(slug.into());
        self
    }
    pub fn roles(mut self, csv: impl Into<String>) -> Self {
        self.roles = Some(csv.into());
        self
    }
    pub fn sub(mut self, s: impl Into<String>) -> Self {
        self.sub = s.into();
        self
    }
    pub fn iss(mut self, s: impl Into<String>) -> Self {
        self.iss = s.into();
        self
    }
    pub fn email(mut self, s: impl Into<String>) -> Self {
        self.email = s.into();
        self
    }

    /// Apply the configured headers to a `Request::builder()`. Returns a
    /// builder you finalise with `.body(Body::empty())` / similar.
    pub fn apply<B>(self, mut req: Request<B>) -> Request<B> {
        let h = req.headers_mut();
        h.insert(
            HeaderName::from_static("x-auth-user-id"),
            HeaderValue::from_str(&self.sub).unwrap(),
        );
        h.insert(
            HeaderName::from_static("x-auth-issuer"),
            HeaderValue::from_str(&self.iss).unwrap(),
        );
        h.insert(
            HeaderName::from_static("x-auth-email"),
            HeaderValue::from_str(&self.email).unwrap(),
        );
        if let Some(n) = self.name {
            h.insert(
                HeaderName::from_static("x-auth-name"),
                HeaderValue::from_str(&n).unwrap(),
            );
        }
        if let Some(t) = self.tenant_slug {
            h.insert(
                HeaderName::from_static("x-auth-tenant-id"),
                HeaderValue::from_str(&t).unwrap(),
            );
        }
        if let Some(r) = self.roles {
            h.insert(
                HeaderName::from_static("x-auth-roles"),
                HeaderValue::from_str(&r).unwrap(),
            );
        }
        req
    }
}
