//! Shared test harness for backend integration tests.
//!
//! Each integration-test file (`auth_pipeline.rs`, `chat_streaming.rs`, …)
//! runs as its own binary, so we keep this module small and explicit
//! rather than relying on globals. Build the world you need with
//! [`TestApp::build()`] and drive it with `tower::ServiceExt::oneshot`.

#![allow(dead_code)] // each test binary uses a different subset of helpers

pub mod fake_llm;
pub mod fake_sink;
pub mod fake_source;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use surrealdb::RecordId;
use tower::ServiceExt;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::json;

use delphi::api;
use delphi::auth::{
    self, AuthMode, ClaimsExtractor, HeaderConfig, IdentityDeps, JwtClaimsExtractor,
};
use delphi::ingestion::Pipeline;
use delphi::object_store::{MemObjectStore, ObjectStore};
use delphi::state::AppState;
use delphi::storage::{RequestDbPool, Storage, SystemDb};

use crate::common::fake_llm::FakeLlmClient;

/// Everything a test needs to drive the backend in-process: the fully-built
/// axum [`Router`], plus a handle to the in-memory SurrealDB for direct
/// inspection / seeding outside the HTTP path.
pub struct TestApp {
    pub router: Router,
    pub system: Arc<SystemDb>,
    pub db: Arc<RequestDbPool>,
    pub object_store: Arc<dyn ObjectStore>,
    pub default_tenant_id: RecordId,
    pub default_tenant_slug: String,
    /// Shared with the Discovery SSE endpoint and `NotifyingSink`. Tests
    /// can `subscribe()` to verify ingest fan-out without parsing the
    /// SSE stream.
    pub events: tokio::sync::broadcast::Sender<delphi::ingestion::NewDocumentEvent>,
}

impl TestApp {
    /// Build a fresh app: in-memory SurrealDB, schema applied, default tenant
    /// created, header-mode auth, fake LLM. Each call is independent — no
    /// shared state between tests.
    pub async fn build() -> Self {
        let system = Arc::new(
            SystemDb::in_memory("delphi_test", "main")
                .await
                .expect("connect in-memory surreal"),
        );

        system
            .init_schema()
            .await
            .expect("init schema in test db");

        let default_tenant_slug = "test".to_string();
        let default_tenant_id = auth::resolve_default_tenant(&system, &default_tenant_slug)
            .await
            .expect("resolve default tenant");

        let identity_deps = IdentityDeps {
            system: system.clone(),
            default_tenant_slug: default_tenant_slug.clone(),
            default_tenant_id: default_tenant_id.clone(),
        };

        let mode = AuthMode::Header(HeaderConfig {
            default_tenant_slug: default_tenant_slug.clone(),
        });

        let extractor: Arc<dyn ClaimsExtractor> = Arc::new(JwtClaimsExtractor::new());

        let request_pool = Arc::new(RequestDbPool::from_system(&system));
        let object_store: Arc<dyn ObjectStore> = Arc::new(MemObjectStore::new());
        let (events_tx, _) = tokio::sync::broadcast::channel(64);
        let pipeline_storage: Arc<dyn Storage> = request_pool.clone();
        let pipeline: Arc<dyn delphi::ingestion::IngestSink> =
            Arc::new(Pipeline::new(pipeline_storage));
        let sink: Arc<dyn delphi::ingestion::IngestSink> = Arc::new(
            delphi::ingestion::NotifyingSink::new(pipeline, events_tx.clone()),
        );
        let state = AppState {
            db: request_pool.clone(),
            llm: Arc::new(FakeLlmClient::default()),
            sink,
            object_store: object_store.clone(),
            events: events_tx.clone(),
        };

        let router = api::build_router(state, None, &mode, identity_deps, extractor);

        TestApp {
            router,
            system,
            db: request_pool,
            object_store,
            default_tenant_id,
            default_tenant_slug,
            events: events_tx,
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

/// Build a `Request` with an `Authorization: Bearer <jwt>` header
/// shaped like what the BFF produces. Defaults to a dev-shaped
/// identity in the default tenant — mutate with `.tenant()` /
/// `.roles()` before calling `.apply()`.
///
/// Tokens are unsigned (`alg: none`, empty signature). The backend's
/// `JwtClaimsExtractor` doesn't validate signatures — that's the
/// BFF's job in production — so unsigned is sufficient for tests.
pub struct AuthRequestBuilder {
    sub: String,
    iss: String,
    email: String,
    name: Option<String>,
    tenant_slug: Option<String>,
    roles: Vec<String>,
}

impl Default for AuthRequestBuilder {
    fn default() -> Self {
        Self {
            sub: "test-user".into(),
            iss: "https://idp.test/".into(),
            email: "test@delphi.test".into(),
            name: Some("Test User".into()),
            tenant_slug: None,
            roles: Vec::new(),
        }
    }
}

impl AuthRequestBuilder {
    pub fn tenant(mut self, slug: impl Into<String>) -> Self {
        self.tenant_slug = Some(slug.into());
        self
    }
    /// Comma-separated list of role names, matching the legacy
    /// X-Auth-Roles wire format. Convenient for one-line role
    /// declarations in test bodies.
    pub fn roles(mut self, csv: impl Into<String>) -> Self {
        self.roles = csv
            .into()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
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

    /// Mint a JWT carrying the configured claims and attach it to the
    /// request as `Authorization: Bearer …`.
    pub fn apply<B>(self, mut req: Request<B>) -> Request<B> {
        let mut payload = json!({
            "sub": self.sub,
            "iss": self.iss,
            "email": self.email,
        });
        if let Some(n) = &self.name {
            payload["preferred_username"] = json!(n);
        }
        if let Some(t) = &self.tenant_slug {
            payload["tenant_id"] = json!(t);
        }
        if !self.roles.is_empty() {
            payload["roles"] = json!(self.roles);
        }
        let jwt = unsigned_jwt(&payload);
        req.headers_mut().insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );
        req
    }
}

/// `header.payload.signature` with `alg: none` and an empty signature
/// — sufficient for the inbound extractor (which doesn't validate).
fn unsigned_jwt(payload: &serde_json::Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
    let body = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(b"");
    format!("{header}.{body}.{sig}")
}
