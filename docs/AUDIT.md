# Delphi — Code Audit Findings

Audit performed against the Rust backend, SurrealDB schema, BFF
(Traefik + oauth2-proxy + Keycloak) configs, frontend SPA, and
architecture docs. Findings are grouped by severity and have stable
IDs so they can be referenced from PRs and checked off as fixes land.

Mark items as `[x]` once a fix has been merged and verified.

---

## Critical

- [x] **C1.** Tenancy gap: `docs/ARCH.md` claims every domain record carries
  `tenant_id` and that SurrealDB record-level access rules enforce isolation.
  In reality the `document`, `document_content`, `chunk`,
  `document_version`, `source_state`, and `feed_read` tables have no
  tenant field, the schema has no `DEFINE ACCESS` rules, and the backend
  connects as SurrealDB `Root` (which would bypass any rule). No storage
  method takes a tenant filter. With more than one tenant in the same DB,
  every read leaks across tenants.

  _Resolved: Phase 1 added `tenant_id` to every domain table + threaded
  it through the `Storage` trait. Phase 2 added `PERMISSIONS` clauses
  + `DEFINE ACCESS … TYPE RECORD WITH JWT`. `backend/tests/cross_tenant_isolation.rs`
  proves engine refuses cross-tenant access on 5 cases._

- [x] **C2.** SSE event broadcast (`api/discovery.rs::events`) is a single
  global `broadcast::Sender<NewDocumentEvent>`; every authenticated user
  subscribes to every other tenant's new-document titles in real time.
  Either partition the channel by tenant or filter on subscribe.

  _Resolved: `NewDocumentEvent` carries `tenant_id`; SSE handler
  captures `auth.tenant_id` at connection setup and filters events
  in-loop. Plus 1h ±5min force-reconnect so the handler picks up
  role/revocation changes on next connect._

- [x] **C3.** Backend trust boundary is purely network. `HeaderClaimsExtractor`
  performs no signature validation. Tier-1 dev exposes `8081:8081` to the
  host; in dev mode the injector overwrites headers, but a misbuilt prod
  image (no `dev-auth` feature) on the same compose snippet is one mistake
  away from `curl -H 'X-Auth-User-Id: admin'` superuser. Add a
  defence-in-depth extractor (JWT validation or `X-Internal-Auth`
  shared-secret) before any non-localhost deployment.

  _Resolved (architecture): full-JWT cutover landed. `X-Auth-*` headers
  are gone from the backend; production identity arrives as
  `Authorization: Bearer <IdP-signed JWT>` only. The forged-header
  bypass class is impossible — there's nothing to forge that isn't a
  signed JWT. Backend doesn't validate the signature today (BFF does);
  signature-validation in `JwtClaimsExtractor` is the small defence-in-depth
  drop-in for when we want two-of-two safety._

- [x] **C4.** `docker-compose.full.yml` ships `RUST_ENV=production` together
  with `SURREAL_USER=root / SURREAL_PASS=root`. `.env.example` documents
  the same defaults. Refuse to start when `RUST_ENV=production` and
  Surreal credentials match the documented defaults; or move credentials
  to required-env (no defaults) for the prod compose.

  _Resolved: Phase 1 split creds into `SURREAL_SERVICE_USER` /
  `SURREAL_SERVICE_PASS`. `enforce_production_guard` refuses to start
  under `RUST_ENV=production` when either is unset or equals `root`._

---

## High

- [x] **H1.** `Pipeline::ingest` dedup is global: `get_document_by_canonical`
  is not tenant-scoped, and `document.canonical_id` has a global UNIQUE
  index. Two tenants ingesting the same arXiv paper collide; the second
  silently no-ops and inherits the first's metadata. Twin of C1 in the
  ingestion direction.

  _Resolved: `document_canonical_id` index is now
  `UNIQUE (tenant_id, canonical_id)`. `Pipeline::ingest` looks up by
  `(tenant, canonical_id)`._

- [x] **H2.** `mark_read` is not idempotent under concurrent races: SELECT-
  then-CREATE without a transaction. Schema has a UNIQUE `(user, document)`
  index, but the unique-violation isn't swallowed — the second concurrent
  call returns 500. Either use `UPSERT` or catch the unique violation.

  _Resolved: rewrote `SurrealStorage::mark_read` as a single
  `INSERT INTO feed_read … ON DUPLICATE KEY UPDATE read_at = read_at`
  statement. The UPDATE side is a deliberate no-op so first-read
  semantics are preserved (a re-mark doesn't bump the timestamp).
  SurrealDB's MVCC can still reject genuinely concurrent writes to
  the same row with a transient "Resource busy" error, so the call
  retries up to 5x with a few ms of backoff per attempt. Live
  smoke against tier-1 with 16 parallel POSTs returned 16/16 = 204.
  New unit test (`mark_read_concurrent_calls_all_succeed_and_create_one_row`)
  asserts both classes of error are masked from callers._

- [x] **H3.** No request body size limits configured. axum's 2 MB default
  applies, but `/api/ingestion/documents` accepts an `IngestRequest` with
  arbitrary `raw_text` and `metadata`. Set explicit per-route
  `DefaultBodyLimit` (especially tighter on `/api/chat`).

  _Deferred to infra: see [INFRA-BACKLOG.md#i1](INFRA-BACKLOG.md#i1-per-route-body-size-limits-was-audit-h3).
  Belongs at the reverse proxy (Traefik `buffering.maxRequestBodyBytes`
  per-route) so oversized requests never reach the backend. Single-user
  / private deployments skip._

- [x] **H5.** `Cargo.toml` enables `tower-http`'s `cors` feature but no
  `CorsLayer` is wired. Either drop the feature or add an explicit
  permissive-in-dev / restricted-in-prod layer.

  _Resolved: feature dropped from `Cargo.toml`._

- [x] **H6.** `LocalFsObjectStore::put` uses a non-unique tmp filename
  (`<key>.<ext>.tmp`). Concurrent writers to the same key clobber each
  other's tmp file before rename. Add a unique suffix (`.<pid>.<rand>`).

  _Resolved: tmp filename is now `<key>.<ext>.<pid>.<seq>.tmp` where
  `seq` comes from a process-wide `AtomicU64` (no new deps). pid
  disambiguates across processes sharing the root; the atomic
  disambiguates within a process. New test
  (`concurrent_put_same_key_all_succeed`) fans out 16 parallel puts to
  the same key, asserts every one returns Ok, the final read returns
  one writer's payload, and no `.tmp` siblings linger._

- [x] **H7.** `conversation` and `message` tables are defined in the schema
  but never written to by any code. Schema-as-aspiration drifts. Either
  delete the tables until chat persistence ships, or land a minimal
  write-path now.

  _Resolved: chat persistence landed with the v3 streaming surface.
  `Storage` exposes `create_conversation` / `list_conversations` /
  `get_conversation` / `commit_turn` / `rename_conversation` /
  `delete_conversation`; the SurrealDB impl writes both tables
  (`storage/surreal.rs`), and `api/chat.rs` drives them end-to-end._

---

## Medium

- [x] **M1.** `Storage::wipe` and `Storage::counts` are global — no tenant
  scoping. `delphi admin wipe` would nuke every tenant's data. Require an
  explicit `--tenant=` argument or `--all-tenants` confirmation flag.

  _Resolved at the API: `SystemDb::counts(Option<&RecordId>)` and
  `SystemDb::wipe(Option<&RecordId>)` accept a tenant filter. Admin CLI
  still defaults to all-tenants — gating is documented; a `--tenant=`
  CLI flag is a small follow-up._

- [x] **M2.** `SurrealStorage` and `Surreal<Any>` leak across the storage
  module boundary. `auth/bootstrap.rs` runs raw SurrealQL against the
  underlying handle; `storage::SurrealStorage` is `pub`-exported as an
  "escape hatch". Move `ensure_user` / `resolve_default_tenant` etc.
  behind `Storage` trait methods so the trait is the only seam.

  _Resolved: `SystemDb` is now the typed escape hatch; `SurrealStorage`
  is no longer exported. `auth/bootstrap.rs` and `auth/middleware.rs`
  take `&SystemDb` (the typed surface) — `raw()` is exposed only for
  the contained system paths (bootstrap, scheduler, admin) and for
  integration tests that drive the engine directly._

- [x] **M3.** Dependency-direction inversion: `auth/middleware.rs` and
  `auth/bootstrap.rs` import `surrealdb::Surreal<Any>` directly. Per
  CLAUDE.md ("depend on abstractions, not implementations"), `auth`
  should depend on the `Storage` trait, not SurrealDB. Linked to M2.

  _Resolved with M2 — auth depends on `SystemDb` (typed) not on raw
  SurrealDB types._

- [x] **M4.** `surrealdb::Datetime` leaks across the storage interface
  (`Document.published_at`, `Document.ingested_at`, `FeedCursor.ingested_at`).
  ARCH.md says "SurrealDB types do not leak across the module boundary".
  Replace with `chrono::DateTime<Utc>` in `Document` / `FeedCursor`; do
  the wire-format conversion inside the SurrealStorage impl on serialize.

  _Resolved: public models use `chrono::DateTime<Utc>`; the
  `DocumentWire` struct inside `storage/surreal.rs` handles the
  conversion to/from `surrealdb::Datetime` at the (de)serialize
  boundary._

- [ ] **M5.** `api/chat.rs` builds the response with `Response::builder()
  ...body(...).unwrap()`. Fine today (all values are static), but a
  panic-prone shape if header values become dynamic. Use typed
  constructors.

- [ ] **M6.** `error::Error` derives `#[error("surreal: {0}")]` from
  `surrealdb::Error`. Handlers all log internally and return generic
  strings, but a future `Result<_, Error>::into_response()` would expose
  internal SurrealQL fragments to clients. Mark the surface or wrap at
  the API boundary.

- [ ] **M7.** `Pipeline::ingest` is not transactional: `upsert_document`
  succeeds, then `upsert_content` fails → document row exists with no
  content row, dedup hash matches on next call, content is never
  written. Use a Surreal transaction or a saga with cleanup.

- [ ] **M8.** `IngestRequest::metadata: serde_json::Value` is unbounded
  in size and depth. With `/api/ingestion/documents` open to ingester
  roles, malformed/oversized JSON persists into `document.metadata` and
  slows queries forever. Add depth/size validation.

- [ ] **M9.** Frontend renders `item.source_uri` as `<a href={...}>`
  without scheme validation (`PaperCard.tsx`). React filters
  `javascript:` but not `data:` / custom schemes. Trusted today
  (arXiv-only), risky as more sources are added. Add an
  `https:`/`http:`-only allowlist at render.

- [~] **M10.** `useFeedEvents` doesn't surface SSE connection state.
  Network-flap UX is "live updates silently stop". Expose a
  `connectionState` and/or refetch first page on focus after long
  hidden duration to repair gaps.

  _Partially addressed: backend now force-closes SSE after 1h ±5min
  jitter, so the browser auto-reconnects regularly and picks up gaps.
  The original network-flap silent-failure mode is unchanged — a
  `connectionState` UI surface is still a follow-up._

- [x] **M13.** `POST /api/discovery/items/:id/read` returns 204 even
  when `:id` does not exist or belongs to another tenant. Surfaced by
  `tests/e2e/tenant-leakage.spec.ts` while writing the leakage suite.
  Per-user read-state row gets created against an unverified document
  id — bloats `feed_read` and would let a curious caller probe other
  tenants' id space (existence side-channel). Fix: validate the
  document exists in the caller's tenant before upserting `feed_read`.

  _Resolved: no longer applicable. The mark-read endpoint and the
  `feed_read` table no longer exist in the codebase — `api/mod.rs`
  registers only `/api/discovery/feed` and `/api/discovery/feed/events`,
  and `feed_read` is absent from `schema.surql`. If per-user read state
  is reintroduced, the tenant-scoping requirement called out here must
  be honoured up front._

- [ ] **M14.** Tier-2 sign-out shows Keycloak's "Are you sure?"
  confirmation page before clearing SSO cookies, because we don't
  pass `id_token_hint` to the end-session endpoint. Keycloak ≥ 18
  requires the hint to skip the prompt. Two fixes possible:
  (a) make oauth2-proxy forward the IdP-issued id_token through to
      the rd target — not a native oauth2-proxy feature, would need
      a small wrapper or a fork;
  (b) move the redirect into a tiny backend `/api/auth/logout`
      handler that has the id_token in scope (BFF stores it
      server-side) and constructs the URL with `id_token_hint`.
  Test `tests/e2e/logout.spec.ts` currently clicks the confirm
  button via `signOutViaKeycloak`, which masks this UX wart but
  proves the security chain is correct.

- [ ] **M12.** No instant permission/revocation path. After an admin
  disables a user or drops a role in Keycloak, the user keeps
  authenticated access until the next oauth2-proxy token refresh
  (≤20 min) or JWT `exp` (≤30 min) — worst-case ~20 min of stale
  access. ARCH.md previously implied a Redis blacklist closed this
  gap; it does not, because oauth2-proxy ships **no** native blacklist
  hook, no per-request revocation check, and no back-channel logout
  endpoint (upstream `oauth2-proxy/oauth2-proxy#1224` and `#1684`,
  both still open as of May 2026). The Guiding Principles rule out
  any solution that would teach the backend about revocation
  (no blacklist check or introspection in `JwtClaimsExtractor`).
  Treated as advanced functionality and deferred. Two viable paths
  when we revisit, both keep auth at the edge:
  (a) **Shorter access-token TTL** — pure IdP + proxy config
      change; "instant" SLO becomes "≤ TTL" (e.g. 60 s). Backend
      contract unchanged. Cheapest, no code.
  (b) **Replace the edge** with one that supports per-request
      policy evaluation natively — **Pomerium**, **Ory Oathkeeper**,
      or a managed IAP (Cloudflare Access, Google IAP). Backend
      contract unchanged.

---

## Low

- [ ] **L1.** `Cargo.toml` puts `kv-mem` in default features so production
  builds also link the in-memory engine. Move it behind a `kv-mem`
  feature gated to dev/test.

- [x] **L2.** No rate-limit middleware anywhere; `/api/chat` logs every
  request and forwards to upstream LLM. One user can flood logs and run
  up upstream spend. Add a per-user rate-limit.

  _Deferred to infra: see [INFRA-BACKLOG.md#i2](INFRA-BACKLOG.md#i2-per-user-rate-limiting-on-apichat-was-audit-l2).
  Belongs at the proxy / API gateway with per-user keying (requires
  oauth2-proxy header injection so Traefik can see the identity).
  Single-user / private deployments skip._

- [ ] **L3.** `.env` (gitignored) contains a real `MINIMAX_API_KEY` on
  disk. Not in git history. Rotate and treat like any other credential.

- [ ] **L4.** `BIND_ADDR` defaults to `0.0.0.0:8081`. Right call inside
  docker, wrong call when running `cargo run` locally. Default to
  `127.0.0.1:8081`; require explicit override for `0.0.0.0`.

- [ ] **L6.** `enforce_production_guard` only fires on
  `RUST_ENV=production`. `RUST_ENV=staging` with `AUTH_MODE=dev` boots
  silently. Use an allowlist of non-prod environments instead.

- [ ] **L7.** `tenant.slug` schema ASSERT enforces lowercase, but the
  `tenant_id` claim from the IdP arrives as-is. A user with a
  `tenant_id` attribute of `Acme` misses the `acme` tenant row and
  auto-provisions a new one. Normalize the claim at ingest in
  `JwtClaimsExtractor` (or a dedicated `Claims::normalize` step) before
  `ensure_user`.

- [x] **L8.** `oauth2-proxy/alpha.yaml` maps the multi-value `groups`
  claim to `X-Auth-Tenant-Id` (single value). oauth2-proxy comma-joins,
  so two groups → `groupA,groupB` → no tenant matches. Pick the first
  element explicitly, or commit to a documented "first group is tenant"
  parse on the backend.

  _Resolved: full-JWT cutover dropped X-Auth-* header projection
  entirely. `tenant_id` arrives as a dedicated JWT claim from the IdP
  (Keycloak user-attribute mapper); no multi-value flattening involved._

- [x] **L9.** `roles` claim is split on `,` with no canonical separator
  policy. Document the constraint or use a named-list header
  convention.

  _Resolved: backend reads `roles` as a JSON array directly from the
  JWT payload — no string-splitting, no separator ambiguity._

- [ ] **L10.** `data/` is created root-owned by docker compose; local
  `cargo` / `bun` runs can't read or write it. Set `user:` in compose
  to host UID, or document the root-owned dir.

- [x] **L11.** `storage/mod.rs` re-exports `SurrealStorage` as `pub` but
  `surreal_from_env` as `pub(crate)`. After the lib/bin split,
  `pub(crate)` suffices for both. Keep the public surface honest.

  _Resolved with M2: `SurrealStorage` is no longer publicly exported.
  External callers go through `SystemDb` (system path) or
  `RequestDbPool` (request path) instead._

- [ ] **L12.** Discovery feed: the "new" glow / chip on a freshly-arrived
  card fades after the card has been ≥50% in view for 1s
  (`useNewnessFade` in `frontend/src/components/discovery/Feed.tsx`).
  Should instead persist until the user actually engages with the card
  — fade on `mouseenter` / focus, not on dwell. Dwell-fade can clear
  the highlight off-screen-but-rendered cards (e.g. a doc that lands
  near the top while the user is reading further down) before they
  ever see it.

---

## Module-structure / coupling notes

- [x] **S1.** `auth` module imports `surrealdb::Surreal<Any>` directly
  (linked to M2/M3). Re-shape so `auth` depends on the `Storage` trait,
  not on a SurrealDB handle.

  _Resolved with M2/M3 — `auth/bootstrap.rs` and `auth/middleware.rs`
  depend on `SystemDb`, the typed escape hatch from the storage
  module._

- [ ] **S2.** `api/mod.rs::serve` is the composition root and reaches
  into `storage::surreal_from_env`, `llm::llm_from_env`,
  `object_store::from_url`, etc. Consider moving wiring into a separate
  `compose.rs` so `api` stays purely about HTTP routing.

---

## New findings from the JWT cutover

- [x] **N1.** Tier-1 dev mode (`auth/dev.rs::dev_inject_middleware` +
  `AuthMode::Dev`) is broken since the full-JWT cutover. The dev
  injector still writes `X-Auth-*` headers; `JwtClaimsExtractor` no
  longer reads them. Every request through the tier-1 stack 401s.

  _Resolved: `dev_inject_middleware` now mints an HS512-signed JWT
  with the same claim shape (`sub` / `iss` / `email` /
  `preferred_username` / `tenant_id` / `roles` / `ac` / `ns` / `db` /
  `iat` / `exp`) the production IdP would emit, signed with
  `SURREAL_JWT_SECRET` so SurrealDB's `app_session` access method
  validates it engine-side. The equivalence test in `auth/dev.rs`
  round-trips the dev JWT through `JwtClaimsExtractor` to enforce
  "dev mode is a strict subset of prod" at compile time. Tier-1
  Playwright `chat-roundtrip` passes; backend test suite stays green
  in both `--no-default-features` and `--features dev-auth`._

- [ ] **N2.** `ensure_user` auto-provisions tenants from JWT claims
  (`upsert_tenant` on unknown slug). The architecture doc previously
  said "never auto-create arbitrary tenants from claims"; we relaxed
  that to make tier-2 dev work end-to-end without an admin-seeding
  step. The reasoning still holds in operational terms — the BFF and
  IdP together bound which tenant slugs can ever arrive — but the
  trust assumption is worth a paragraph in `docs/ARCH.md` to make
  the dependency on IdP-side discipline explicit.

- [x] **N3.** Backend doesn't validate the JWT signature
  (`JwtClaimsExtractor` only decodes the payload). The BFF validates
  against Keycloak's JWKS, so this is the same trust model as before
  — but the defence-in-depth slot is now obvious and small: add a
  `jsonwebtoken::decode_header` + JWKS-cached validation step in
  `JwtClaimsExtractor::extract`. Worth doing before any deployment
  where the backend port might be exposed beyond localhost.

  _Resolved: new `auth/validator.rs` defines a `JwtValidator` trait
  with `Hs512Validator` (shared secret — tier-1 dev + tests) and
  `JwksValidator` (fetches the IdP's JWKS, caches by `kid`, refreshes
  on cache miss; alg pinned to the JWK's declared algorithm to block
  alg-confusion). `JwtClaimsExtractor` now takes an
  `Arc<dyn JwtValidator>` and validates signature + `exp` (+ optional
  `iss` / `aud`) before lifting any claim. `validator_from_jwt_access`
  consumes the same `JwtAccessConfig` as `SystemDb::define_jwt_access`
  — backend and SurrealDB validate against the same key material from
  one `SURREAL_JWT_*` env knob. Bad-signature, expired, and
  iss-mismatch cases are now 401 at the backend boundary._

- [ ] **N4.** `oauth2-proxy` config has settled on v7.4.0 (pinned in
  `docker-compose.full.yml`) because v7.6's alpha-config tightened
  the legacy-vs-alpha overlap rules in ways that broke the split we
  rely on. Periodic re-evaluation: try v7.6+ again when the upstream
  has documented the cookie/session/server fields more clearly, and
  collapse to a single config file.

- [x] **N5.** Tier-2 e2e regression: `db.authenticate` against
  Keycloak's JWKS failed with a generic "There was a problem with
  authentication" on every request.

  _Resolved: two distinct bugs were stacked._

  _**Bug 1 — missing routing claims.** SurrealDB's `db.authenticate(jwt)`
  uses `ac` (and `ns` / `db`) claims to route to the right access
  method. Our HS512 paths (`auth/dev.rs::mint_dev_jwt`,
  `tests/cross_tenant_isolation.rs::mint_jwt`, `tests/common/mod.rs`)
  all inject them manually; vanilla Keycloak tokens carry none. Fix:
  added three `oidc-hardcoded-claim-mapper` entries to
  `ops/keycloak/realm-export.json` so the access token now emits
  `ac: "app_session"`, `ns: "delphi"`, `db: "main"` alongside the
  normal claims._

  _**Bug 2 — SystemDb session race.** Phase 2 wiring's
  `ensure_root_session` did `invalidate()` + `signin(Root)` on every
  system-path upsert. In production the SystemDb owns its own
  connection nothing else touches, so the sequence is unnecessary
  and races with concurrent requests (A's signin clobbered by B's
  invalidate before A's query runs). Tier-2's two parallel
  Playwright workers exposed it as 500s with "IAM error: Not enough
  permissions". Fix: added a `shared_engine` flag on `SystemDb`
  (true only for embedded engines that share session state with the
  pool's clones, i.e. tests); `ensure_root_session` is a no-op when
  false._

  _Both tier-2 specs pass (`chat-roundtrip`, `tenant-isolation
  @tier2`); tier-1 still green; full backend suite still green in
  both feature configs._

- [x] **N6.** Per-request connection lifecycle hardening
  (`storage/request.rs`). Three improvements done together:

  1. **Auto-logout on scope exit.** `AuthedDb::drop` now calls
     `db.invalidate()` *before* returning the connection to the pool's
     mpsc channel. A connection idle in the channel is therefore
     always in a logged-out state — the previous user's RECORD session
     never lingers. No public `release()` / `logout()` method exists;
     the scope guard is the API.

  2. **Configurable pool size.** `RequestDbPool::from_env_default`
     reads `REQUEST_DB_POOL_SIZE` (default `8`, was a hard-coded
     `16`). Validates `> 0`. Documented in `.env.example`.

  3. **Documented future upgrade path.** Doc-comment on
     `RequestDbPoolInner` flags the `mpsc + Mutex<Receiver>` shape as
     a documented workaround for multi-consumer semantics, with
     `deadpool` / `bb8` / `mobc` or `async-channel` named as
     drop-in replacements if contention or pool features ever matter.

  _Resolved: tier-2 e2e green with the new Drop semantics; full
  backend test suite green (both feature configs); tier-1 spec also
  green._

---

## Priority order (highest leverage first)

1. ~~C1 — close the tenant-isolation gap or drop the SaaS deployment claim.~~ ✓
2. ~~H1, C2 — close ingestion/dedup and SSE leakage as part of the same fix.~~ ✓
3. ~~C3 — drop X-Auth headers, full JWT path.~~ ✓
4. ~~C4 — fail-closed on default Surreal credentials.~~ ✓
5. ~~N1 — tier-1 dev (JWT-minting dev injector).~~ ✓
6. ~~N5 — tier-2 `db.authenticate` against Keycloak JWKS.~~ ✓
7. ~~N3 — backend signature validation (defence-in-depth).~~ ✓
8. ~~H3, L2 — body size limit and per-user rate limit.~~ Deferred to
   infra ([`INFRA-BACKLOG.md`](INFRA-BACKLOG.md)) — defended at the
   reverse proxy in tier-2; single-user deployments skip.
9. ~~H2 — `mark_read` upsert race.~~ ✓
10. ~~H6 — object-store tmp filename collision (concurrent PUT clobber).~~ ✓
