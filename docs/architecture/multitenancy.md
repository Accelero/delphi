# Multi-Tenancy Implementation Plan

Scope: close finding **C1** (and the linked C2 / H1 / M1 / M2 / M3 / M4 / S1
from `docs/AUDIT.md`) by introducing engine-enforced tenant isolation in
SurrealDB, while restructuring the backend so a small, contained module is
the only thing that holds elevated DB privileges.

This is the single source of truth for the tenancy work. Once it ships,
fold the relevant content into `ARCH.md` and delete this file.

---

## Goals

1. **Engine-enforced isolation.** Every domain table refuses cross-tenant
   reads and writes via SurrealDB `PERMISSIONS` clauses. A forgotten
   `WHERE tenant = …` in handler code cannot leak data — the engine adds
   it.
2. **Minimal trust surface in the backend.** Per-request handlers run
   with engine-enforced RBAC. Only a small, contained module
   (boot / admin / scheduler) holds the service-user credential that
   bypasses RBAC.
3. **Defence in depth at the trust boundary.** The backend doesn't trust
   X-Auth-* headers for security decisions. The IdP-signed JWT is
   forwarded all the way to SurrealDB, which independently validates it
   against the IdP's JWKS URL. A bypassed BFF cannot grant DB access.
4. **One tenant per user (v1).** API enforces single-membership. Schema
   permits many memberships per user (we keep the join table) so v2
   multi-membership is a code change, not a migration.
5. **Same shape for single-user and SaaS deployments.** Single-user is
   "one tenant, one user"; SaaS is "many tenants, many users." Same
   code, different IdP config.

## Non-goals

- Multi-tenant per session (tenant switching mid-session). v1 picks one
  tenant per user at provisioning time; switching = re-auth.
- Self-serve tenant creation. Tenants are created out-of-band by the
  operator (via admin CLI or directly against the IdP).
- Per-tenant scheduler adapters (one arXiv config per tenant). v1
  scheduler uses one configured tenant for all adapters.
- Migrating existing data. The codebase is pre-customer; we apply the
  new schema fresh and re-seed dev.

---

## Target architecture

```
                   IdP (Keycloak / Auth0 / Dex-for-dev)
                   │  emits JWT with claims:
                   │    iss, sub, email, tenant_id, roles
                   ▼
   Browser ──cookie──► Traefik ──forward-auth──► oauth2-proxy
                          │                          │ holds JWT
                          ▼                          │ in Redis session
                    forwards JWT to backend ◄────────┘
                    via Authorization: Bearer <jwt>
                          │
                          ▼
                       Backend
                          │
                          ├─ identity middleware
                          │    parses JWT (defence in depth: validates
                          │      signature against IdP JWKS)
                          │    pre-flight ensure_user via SystemDb
                          │    (idempotent SELECT-or-CREATE on app_user)
                          │
                          ├─ acquire connection from RequestDbPool
                          │    db.authenticate(jwt)
                          │    SurrealDB validates signature against
                          │      IdP JWKS URL, runs AUTHENTICATE clause,
                          │      sets $auth = app_user record,
                          │      $token = JWT claims
                          │
                          ├─ handler runs queries
                          │    engine rewrites to add
                          │      WHERE tenant = $token.tenant_id
                          │
                          └─ release connection back to pool
```

Two distinct DB connection lifecycles:

| | RequestDbPool | SystemDb |
|---|---|---|
| Principal | per-request, IdP JWT, RECORD access | service user, DB `EDITOR` role |
| PERMISSIONS apply? | **yes** — engine-enforced | no — bypassed |
| Used by | request handlers | schema apply, ensure_user pre-flight, scheduler ingest, admin CLI |
| Reachable from | `AppState` (and only `AppState`) | composition root, scheduler, admin module |
| Concurrency | pool of N (8–16) WS connections, each authenticated per-checkout | single shared multiplexed connection |

The two are **different types** in Rust. Handlers physically cannot run
a `SystemDb` query because the type isn't in `AppState`.

---

## Phase 1: structural reshape (no semantic change)

Goal: get the codebase into the right shape without flipping engine
enforcement on. After this PR the backend behaves exactly as today; only
the structure has changed. Tests still pass under the existing X-Auth
header pattern.

### Schema additions (`backend/schema.surql`)

Add to every domain table:

```surql
DEFINE FIELD IF NOT EXISTS tenant ON document TYPE record<tenant>
    ASSERT $value != NONE;
```

Apply to: `document`, `document_content`, `chunk`, `document_version`,
`source_state`, `feed_read`. Add an index on `tenant` for each (queries
will filter by it heavily).

`canonical_id` index changes from globally `UNIQUE` to `UNIQUE` per
tenant: `DEFINE INDEX document_canonical_id ON document FIELDS tenant,
canonical_id UNIQUE;` — closes finding **H1**.

Note: schema is `IF NOT EXISTS` everywhere. For pre-customer dev, just
re-apply against an empty DB. No migration required.

### Storage layer reshape (`backend/src/storage/`)

Split the single `Surreal<Any>` into two types:

```rust
// storage/system.rs — privileged singleton
pub struct SystemDb(Surreal<Any>);
impl SystemDb {
    pub async fn connect(...) -> Result<Self> { ... }
    pub fn raw(&self) -> &Surreal<Any> { &self.0 }   // for bootstrap.rs only
}

// storage/request.rs — per-request authenticated pool
pub struct RequestDbPool { ... }
impl RequestDbPool {
    pub async fn with_jwt<F, R>(&self, jwt: &str, f: F) -> Result<R>
        where F: FnOnce(AuthenticatedDb) -> R { ... }
}

// AuthenticatedDb is the only handle handlers receive. It wraps a
// connection that has already been db.authenticate()'d.
pub struct AuthenticatedDb<'a> { /* opaque */ }
```

`Storage` trait gains `tenant_id: &RecordId` on every domain method:

```rust
async fn upsert_document(&self, tenant: &RecordId, doc: &Document) -> Result<DocId>;
async fn list_feed(&self, tenant: &RecordId, user: &RecordId,
                   cursor: Option<FeedCursor>, limit: usize) -> Result<Vec<FeedItem>>;
// ... etc.
```

Implementations write the tenant column on insert and `WHERE tenant = $t`
on read. Phase 1 still runs queries via the service-user connection — so
`PERMISSIONS` are not in effect yet — but the *shape* is correct.

### `AppState` change

```rust
pub struct AppState {
    pub db: Arc<RequestDbPool>,    // changed: was Arc<dyn Storage>
    pub llm: Arc<dyn LlmClient>,
    pub sink: Arc<dyn IngestSink>,
    pub object_store: Arc<dyn ObjectStore>,
    pub events: broadcast::Sender<NewDocumentEvent>,
}
```

`SystemDb` is **not** in `AppState`. Handlers cannot reach it.

### Composition root (`api/serve`)

Today's `surreal_from_env()` becomes two factories:

```rust
let system_db = SystemDb::from_env().await?;        // service-user creds
system_db.init_schema().await?;
let default_tenant = resolve_default_tenant(&system_db, ...).await?;

let request_pool = RequestDbPool::from_env(/* JWKS, pool size */).await?;

let scheduler = if sources_enabled {
    sources::run_scheduler(sink, filter, system_db.clone(), registry)
};

let app = build_router(AppState { db: request_pool, ... }, ...);
```

`SystemDb` is owned by `serve`, passed only to `bootstrap`, `scheduler`,
`admin`. Never reaches `AppState`.

### `auth/bootstrap.rs` changes

`ensure_user` and `resolve_default_tenant` take `&SystemDb` instead of
`&Surreal<Any>`. Closes finding **M3** (auth no longer imports
`surrealdb::Surreal` directly).

### Service-user credentials & production guard

New env vars:

- `SURREAL_SERVICE_USER` / `SURREAL_SERVICE_PASS` — DB-level user with
  `EDITOR` role. Replaces today's `SURREAL_USER` / `SURREAL_PASS`.

`enforce_production_guard` extended: when `RUST_ENV=production`, refuse
to start if any of:

- `SURREAL_SERVICE_USER` unset or equals `root`
- `SURREAL_SERVICE_PASS` unset or equals `root`

(JWT-related guards land in Phase 2.)

Closes findings **C4** and the credential half of **C3**.

### Tests

- All existing integration tests rewritten to use the new
  `Storage` signature (`tenant_id` arg added; tests pass the default
  tenant).
- New test: cross-tenant write via `Storage` trait → verify the row
  carries the correct tenant column. (Phase 1 doesn't enforce
  isolation; this just proves the column is populated.)
- Schema test: assert `tenant` column is NOT NULL on every domain table.

### Files touched in Phase 1

- `backend/schema.surql` — tenant column + indexes
- `backend/src/storage/mod.rs` — split into `system.rs` + `request.rs`
- `backend/src/storage/system.rs` *(new)*
- `backend/src/storage/request.rs` *(new)*
- `backend/src/storage/surreal.rs` — implements `Storage` against
  `RequestDbPool`'s `AuthenticatedDb` (still uses service-user
  connection in Phase 1 — same effect as today)
- `backend/src/storage/models.rs` — `Document` / `FeedItem` etc. gain
  `tenant: RecordId`. Closes finding **M4** by replacing
  `surrealdb::Datetime` with `chrono::DateTime<Utc>` at the same time.
- `backend/src/state.rs` — `AppState.db` type change
- `backend/src/api/mod.rs` — composition root rewiring
- `backend/src/auth/middleware.rs` — takes `SystemDb` for ensure_user
- `backend/src/auth/bootstrap.rs` — takes `SystemDb`
- `backend/src/admin.rs` — takes `SystemDb`
- `backend/src/sources/scheduler.rs` — takes `SystemDb`
- `backend/src/ingestion/pipeline.rs` — `IngestRequest` gains
  `tenant_id`; `Pipeline::ingest` writes it
- `backend/src/api/{chat,discovery,ingestion}.rs` — handlers pass
  `auth.tenant_id` to storage calls
- `backend/Cargo.toml` — drop `tower-http` cors feature (finding **H5**)
- `backend/tests/common/mod.rs` — TestApp builder updated
- `docker-compose.yml`, `docker-compose.full.yml`, `.env.example` —
  rename creds, add production guard env vars

---

## Phase 2: enable engine enforcement + JWT-to-DB

Goal: turn on the engine-enforcement layer. After this PR a forgotten
`WHERE tenant` in any handler query is impossible — engine refuses.

### Schema: PERMISSIONS clauses

Every domain table:

```surql
DEFINE TABLE document SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant = $token.tenant_id
        FOR create   WHERE tenant = $token.tenant_id;

DEFINE FIELD tenant ON document TYPE record<tenant>
    ASSERT $value != NONE AND $value = $token.tenant_id;
```

The field-level `ASSERT` blocks "spoofed tenant on write" — engine
refuses if the row's tenant doesn't match the caller's token claim.
Cheap and loud.

### Schema: DEFINE ACCESS

```surql
DEFINE ACCESS app_session ON DATABASE TYPE RECORD
    WITH JWT URL "$IDP_JWKS_URL"
    AUTHENTICATE {
        IF $token.iss != $idp_issuer       { THROW "bad iss" };
        IF $expected_aud NOT IN $token.aud { THROW "bad aud" };
        RETURN (SELECT id FROM app_user
                WHERE iss = $token.iss AND sub = $token.sub LIMIT 1)[0];
    }
    DURATION FOR SESSION 30m;
```

Schema bootstrap reads `IDP_JWKS_URL`, `IDP_ISSUER`, `IDP_AUDIENCE`
from env and templates them into the schema before applying. (Or:
parameterize via SurrealDB `DEFINE PARAM` if cleaner.)

### Identity middleware: JWT path replaces header path

Today: `HeaderClaimsExtractor` → `Claims` → `ensure_user` →
`AuthContext`.

New: `JwtClaimsExtractor` reads `Authorization: Bearer <jwt>`,
validates it locally (defence in depth — same JWKS URL the DB uses),
parses claims into `AuthContext`. Then:

1. **Pre-flight `ensure_user` via `SystemDb`.** Idempotent. Required
   because the DB's AUTHENTICATE clause needs `app_user` to exist.
2. **Per request, acquire `AuthenticatedDb` from `RequestDbPool` with
   the JWT.** That handle is what handlers get.

X-Auth-* headers become **advisory only** — useful for the dev banner
in `/api/auth/me`, ignored for security.

### `ClaimsExtractor` trait stays — second impl added

Per ARCH.md's existing aspiration:

- `HeaderClaimsExtractor` — kept for tier-1 dev (header injection
  pattern is fine when nothing crosses the network).
- `JwtClaimsExtractor` — production. Validates JWT signature in-process
  before even hitting the DB.

Selected at startup based on `AUTH_MODE`:
- `header` — current behaviour
- `jwt` — new path; required for any deployment exposed beyond
  localhost

### BFF config (Tier 2)

`ops/oauth2-proxy/oauth2-proxy.cfg`:
- Set `pass_authorization_header = true` (currently `false`)
- Optionally drop the X-Auth-* injection (`alpha.yaml`)

`ops/traefik/dynamic/routes.yml`:
- Forward-auth still gates auth, but the response header forwarded to
  backend is `Authorization: Bearer …` (constructed from the
  IdP-issued token oauth2-proxy holds).

### Tests: cross-tenant isolation suite

New integration test file:
`backend/tests/cross_tenant_isolation.rs`. Cases:

- Tenant A user cannot SELECT tenant B's documents (engine refuses).
- Tenant A user cannot mark tenant B's document as read (engine refuses).
- Tenant A user cannot CREATE a document with `tenant = tenant:b`
  (field ASSERT refuses).
- Tenant A user with a hand-crafted query missing the WHERE clause
  still gets only their own rows (PERMISSIONS rewrite).
- Service-user `SystemDb` *can* read across tenants (admin path).

### Files touched in Phase 2

- `backend/schema.surql` — PERMISSIONS clauses, DEFINE ACCESS
- `backend/src/auth/jwt.rs` *(new)* — `JwtClaimsExtractor`
- `backend/src/auth/mod.rs` — re-export
- `backend/src/auth/middleware.rs` — JWT path, pass JWT to
  `RequestDbPool.with_jwt`
- `backend/src/auth/config.rs` — `AuthMode::Jwt(JwtConfig)` variant
  (JWKS URL, issuer, audience env vars)
- `backend/src/storage/request.rs` — actual `db.authenticate(jwt)`
  per checkout
- `backend/src/api/mod.rs` — pick extractor by mode
- `backend/src/auth/guard.rs` — production guard refuses
  `AUTH_MODE=header` when `RUST_ENV=production`
- `ops/oauth2-proxy/oauth2-proxy.cfg` — pass authorization header
- `ops/traefik/dynamic/routes.yml` — forward `Authorization` header
- `docker-compose.full.yml` — env vars for JWKS URL, issuer, audience
- `backend/tests/cross_tenant_isolation.rs` *(new)*
- `docs/ARCH.md` — fold the relevant parts in; delete this file

### Configuration: production hardening

Production guard adds:

- `IDP_JWKS_URL` must be set when `AUTH_MODE=jwt`
- `IDP_ISSUER` must be set when `AUTH_MODE=jwt`
- `IDP_AUDIENCE` must be set when `AUTH_MODE=jwt`
- `AUTH_MODE` cannot be `header` when `RUST_ENV=production`

Closes finding **C3** in full.

---

## Decisions

1. **Tenant claim name:** `tenant_id` everywhere — JWT claim, schema
   field, code identifier. If we adopt Auth0 (which emits `org_id`),
   map at the IdP-config layer (one Auth0 Action remapping `org_id` →
   `tenant_id`). Keeps the rest of the codebase IdP-portable.

2. **Connection pool sizing:** `DB_POOL_SIZE` env var, default `8`.
   Operators tune for their hardware.

3. **JWT-expiry mid-request behaviour:** backend translates
   `db.authenticate` rejection into **401 Unauthorized**. The SPA's
   existing 401 handler hard-navigates to `/oauth2/sign_in`, which
   oauth2-proxy resolves transparently (silent refresh) if its
   session is still valid. **Configure cookie TTL ≤ access-token
   TTL** so the case is essentially impossible under correct config:
   either the cookie is valid (and oauth2-proxy refreshes the JWT
   before forwarding) or it isn't (and oauth2-proxy 401s upstream of
   the backend). The 401 path is the recovery for a misconfigured
   BFF, not a hot path.

4. **SSE auth lifecycle:** auth runs once at connection setup; the
   stream then floats on that single check. **Backend force-closes
   each SSE after 1 hour** (with ±5min jitter to avoid herd
   reconnect after a server restart). Browser's `EventSource`
   auto-reconnects, runs fresh auth on the new connection, picks up
   any tenant/role/revocation changes. Brief invisible blip every
   ~1h is acceptable.

   Implementation shape:

   ```rust
   fn broadcast_to_sse(
       rx: broadcast::Receiver<NewDocumentEvent>,
       tenant: RecordId,
   ) -> impl Stream<Item = Result<Event, Infallible>> {
       let deadline = tokio::time::Instant::now()
           + Duration::from_secs(3600)
           + jitter(±5min);
       futures::stream::unfold((rx, deadline), |(mut rx, deadline)| async move {
           loop {
               tokio::select! {
                   biased;
                   _ = tokio::time::sleep_until(deadline) => return None,
                   ev = rx.recv() => match ev {
                       Ok(e) if e.tenant == tenant => return Some((Ok(e.into()), (rx, deadline))),
                       Ok(_)  => continue,                       // other tenant — skip
                       Err(Lagged(n)) => { warn!(missed=n); continue; }
                       Err(Closed) => return None,
                   }
               }
           }
       })
   }
   ```

   `NewDocumentEvent` gains a `tenant: RecordId` field; `NotifyingSink`
   sets it from the `IngestRequest`. The per-connection tenant filter
   inside the unfold is what closes finding **C2** end-to-end (the
   server-side filter, not the periodic reconnect, is the
   load-bearing piece for isolation; the reconnect is a lifecycle
   hardening).

5. **Scheduler tenant assignment (single-tenant v1):** new env var
   `SOURCES_DEFAULT_TENANT_SLUG`. Scheduler resolves the slug to a
   `RecordId` once at startup and threads it through every
   `IngestRequest`. v2 multi-tenant scheduler ingest is a separate
   change.

6. **Cookie/token TTL relationship (operator guidance):**
   document in `ops/oauth2-proxy/oauth2-proxy.cfg` that
   `cookie_lifetime` ≤ access-token TTL, and `cookie_refresh` ≈
   80% of access-token TTL. Keeps the JWT-expiry edge case
   structurally impossible.

---

## Findings closed

After Phase 2 ships, these `docs/AUDIT.md` items become `[x]`:

- **C1** — engine-enforced tenant isolation
- **C2** — SSE channel partitioned by tenant (handlers only see their
  tenant's events; closed by Phase 1's `tenant_id` plumbing through
  `NewDocumentEvent`)
- **C3** — defence in depth at the trust boundary; X-Auth headers no
  longer security-load-bearing
- **C4** — production guard refuses default credentials
- **H1** — per-tenant `canonical_id` UNIQUE
- **M1** — `Storage::wipe` and `Counts` accept tenant scope (Phase 1
  trait reshape includes this)
- **M2** — `Surreal<Any>` no longer leaks; `SystemDb` is the contained
  escape hatch
- **M3** — `auth` depends on `SystemDb` trait surface, not a
  SurrealDB type
- **M4** — `chrono::DateTime<Utc>` replaces `surrealdb::Datetime` in
  the storage interface
- **S1** — auth → storage dependency direction corrected

Findings unaffected by this work (close separately): **H2, H3, H4, H5,
H6, H7, M5–M11, L1–L11, S2.**

---

## Rollout

- Phase 1 = one PR. Reviewable in one sitting (~2 days work). Behaviour
  unchanged; tests stay green.
- Phase 2 = one PR. Adds the engine-enforcement layer and the JWT path.
  Tests grow (cross-tenant suite). Tier-2 dev stack reconfigured.
- Between phases: dev runs Tier-1 happily on Phase 1's shape. No
  external customer impact.

When Phase 2 ships, fold this doc's Section "Target architecture" into
`docs/ARCH.md` (replacing the current "Multi-tenancy & access control"
section, which is currently aspirational), and delete this file.
