//! Shared test harness for backend integration tests.
//!
//! Each integration-test file (`auth_pipeline.rs`, `chat_streaming.rs`, …)
//! runs as its own binary, so we keep this module small and explicit
//! rather than relying on globals. Build the world you need with
//! [`TestApp::build()`] and drive it with `tower::ServiceExt::oneshot`.

#![allow(dead_code)] // each test binary uses a different subset of helpers

pub mod fake_embedder;
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

use serde_json::json;

use jsonwebtoken::{encode, EncodingKey, Header};

use delphi::api;
use delphi::auth::{
    self, AuthMode, ClaimsExtractor, HeaderConfig, Hs512Validator, IdentityDeps,
    JwtClaimsExtractor, JwtValidator,
};
use delphi::chat::SessionRegistry;
use delphi::embedder::Embedder;
use delphi::object_store::{MemObjectStore, ObjectStore};
use delphi::state::AppState;
use delphi::storage::{JwtAccessConfig, JwtAccessKind, RequestDbPool, SystemDb};
use delphi::text_extractor::TextExtractor;

/// HS512 secret shared between the test JWT signer and SurrealDB's
/// `app_session` access method. Test-only — not used in production builds.
pub const TEST_JWT_SECRET: &str = "delphi-integration-test-hs512-secret-do-not-use-elsewhere";

use crate::common::fake_llm::FakeLlmClient;

/// Everything a test needs to drive the backend in-process: the fully-built
/// axum [`Router`], plus a handle to the in-memory SurrealDB for direct
/// inspection / seeding outside the HTTP path.
pub struct TestApp {
    pub router: Router,
    pub system: Arc<SystemDb>,
    pub object_store: Arc<dyn ObjectStore>,
    pub default_tenant_id: RecordId,
    pub default_tenant_slug: String,
    /// Shared with the Discovery SSE endpoint and `NotifyingSink`. Tests
    /// can `subscribe()` to verify ingest fan-out without parsing the
    /// SSE stream.
    pub events: tokio::sync::broadcast::Sender<delphi::ingestion::FeedItemEvent>,
    /// Same `SessionRegistry` the router holds in its `AppState`. Tests
    /// that need to drive the chat-streaming handshake directly (e.g.,
    /// `chat_handshake.rs`) reach in via this handle instead of going
    /// through the HTTP path.
    pub session_registry: Arc<SessionRegistry>,
}

impl TestApp {
    /// Build a fresh app: in-memory SurrealDB, schema applied, default tenant
    /// created, JWT-mode auth, fake LLM, HS512 access method registered with
    /// the test secret so `AuthRequestBuilder`'s signed JWTs authenticate
    /// against the engine. Each call is independent — no shared state
    /// between tests.
    /// Build with optional RAG components injected. `None` for any
    /// keeps the default test behaviour (no chunking, no paper
    /// embedding, no retrieval). The integration tests opt in by
    /// passing fakes via [`TestApp::build_with_rag`].
    pub async fn build_with_rag(
        text_extractor: Option<Arc<dyn TextExtractor>>,
        chunk_embedder: Option<Arc<dyn Embedder>>,
        document_embedder: Option<Arc<dyn Embedder>>,
    ) -> Self {
        Self::build_inner(text_extractor, chunk_embedder, document_embedder).await
    }

    pub async fn build() -> Self {
        Self::build_inner(None, None, None).await
    }

    async fn build_inner(
        text_extractor: Option<Arc<dyn TextExtractor>>,
        chunk_embedder: Option<Arc<dyn Embedder>>,
        document_embedder: Option<Arc<dyn Embedder>>,
    ) -> Self {
        let system = Arc::new(
            SystemDb::in_memory("delphi_test", "main")
                .await
                .expect("connect in-memory surreal"),
        );

        system.init_schema().await.expect("init schema in test db");

        // Register the `app_session` RECORD access method against the same
        // HS512 secret `AuthRequestBuilder` signs with. SurrealDB validates
        // every per-request `db.authenticate(jwt)` against this.
        system
            .define_jwt_access(&JwtAccessConfig {
                kind: JwtAccessKind::Hs512 {
                    secret: TEST_JWT_SECRET.into(),
                },
                expected_issuer: None,
                expected_audience: None,
                session_duration_secs: Some(60),
            })
            .await
            .expect("define jwt access in test db");

        let default_tenant_slug = "test".to_string();
        let default_tenant_id = auth::resolve_default_tenant(&system, &default_tenant_slug)
            .await
            .expect("resolve default tenant");

        // RequestDbPool clones the system handle — every slot is a
        // clone of the same in-memory engine, so they share ONE
        // session. With size > 1, two requests can hold "different"
        // slots concurrently; one request's [`AuthedDb::Drop`] then
        // races the next request's `db.authenticate(bearer)` and
        // clears the freshly-set RECORD session out from under it
        // (visible as `$auth = NONE` in handler queries). Single-slot
        // forces serialization through the channel: the next acquire
        // waits for Drop's invalidate+send to complete, so authenticate
        // always runs on a known-clean session. Production has
        // physically independent WebSocket connections per slot and is
        // not subject to this.
        let request_pool = RequestDbPool::in_memory(system.raw(), 1)
            .await
            .expect("init test request pool");

        let identity_deps = IdentityDeps {
            system: system.clone(),
            pool: request_pool.clone(),
            default_tenant_slug: default_tenant_slug.clone(),
            default_tenant_id: default_tenant_id.clone(),
        };

        let mode = AuthMode::Header(HeaderConfig {
            default_tenant_slug: default_tenant_slug.clone(),
        });

        // Same secret SurrealDB's `app_session` access method validates
        // against above — backend and engine agree on the key material.
        let validator: Arc<dyn JwtValidator> =
            Arc::new(Hs512Validator::new(TEST_JWT_SECRET, None, None));
        let extractor: Arc<dyn ClaimsExtractor> = Arc::new(JwtClaimsExtractor::new(validator));

        let object_store: Arc<dyn ObjectStore> = Arc::new(MemObjectStore::new());
        let (events_tx, _) = tokio::sync::broadcast::channel(64);
        let session_registry = Arc::new(SessionRegistry::new());
        let state = AppState {
            llm: Arc::new(FakeLlmClient::default()),
            session_registry: session_registry.clone(),
            request_db_pool: request_pool.clone(),
            object_store: object_store.clone(),
            events: events_tx.clone(),
            // RAG: integration tests that don't set up an embedder leave
            // these `None`; the ingest path then runs metadata-only.
            // The `rag_ingest` integration test builds its own wired-up
            // router instead of calling `TestApp::build`.
            text_extractor,
            chunk_embedder,
            document_embedder,
        };

        let router = api::build_router(state, None, &mode, identity_deps, extractor);

        TestApp {
            router,
            system,
            object_store,
            default_tenant_id,
            default_tenant_slug,
            events: events_tx,
            session_registry,
        }
    }

    /// Bind a real TCP listener and serve the router on it. Returns the
    /// `http://127.0.0.1:<port>` base URL plus the JoinHandle so the
    /// test can abort the server on teardown.
    ///
    /// Used by tests that exercise the scheduler's loopback HTTP path —
    /// `IngestApiClient` speaks `reqwest`, not `tower::ServiceExt`, so
    /// the router has to live behind a real socket.
    pub async fn serve_local(&self) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let addr = listener.local_addr().expect("local_addr");
        let app = self.router.clone();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
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
/// Tokens are HS512-signed with [`TEST_JWT_SECRET`]. The backend's
/// `JwtClaimsExtractor` re-validates the signature in-process (audit
/// finding N3); SurrealDB's `app_session` AUTHENTICATE clause then
/// re-validates again on the per-request `db.authenticate(jwt)`.
/// Both layers consult the same secret, set up in [`TestApp::build`].
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
    /// request as `Authorization: Bearer …`. Signed HS512 with
    /// [`TEST_JWT_SECRET`] so SurrealDB's `app_session` AUTHENTICATE
    /// clause (defined in [`TestApp::build`]) accepts it for the
    /// per-request `db.authenticate(jwt)` step.
    pub fn apply<B>(self, mut req: Request<B>) -> Request<B> {
        let mut payload = json!({
            "sub": self.sub,
            "iss": self.iss,
            "email": self.email,
            // `ac` tells SurrealDB which DEFINE ACCESS method to validate
            // against. Production IdPs won't carry this; SurrealDB falls
            // back to "try all defined access methods" when absent. Tests
            // set it explicitly so the engine doesn't probe.
            "ac": "app_session",
            "ns": "delphi_test",
            "db": "main",
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().timestamp() + 60,
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
        let jwt = encode(
            &Header::new(jsonwebtoken::Algorithm::HS512),
            &payload,
            &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
        )
        .expect("sign test JWT");
        req.headers_mut().insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );
        req
    }
}
