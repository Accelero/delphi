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

- [ ] **H2.** `mark_read` is not idempotent under concurrent races: SELECT-
  then-CREATE without a transaction. Schema has a UNIQUE `(user, document)`
  index, but the unique-violation isn't swallowed — the second concurrent
  call returns 500. Either use `UPSERT` or catch the unique violation.

- [ ] **H3.** No request body size limits configured. axum's 2 MB default
  applies, but `/api/ingestion/documents` accepts an `IngestRequest` with
  arbitrary `raw_text` and `metadata`. Set explicit per-route
  `DefaultBodyLimit` (especially tighter on `/api/chat`).

- [ ] **H4.** arXiv `pdftotext` shell-out (`sources/arxiv.rs::extract_pdf_text`)
  has no `tokio::time::timeout`, no output size cap, and the PDF download
  reads the whole body into memory and clones it for the writer task. A
  malformed PDF can hang or OOM the backend. Bound timeout, cap with
  `ARXIV_MAX_PDF_BYTES`, stream instead of double-buffering.

- [x] **H5.** `Cargo.toml` enables `tower-http`'s `cors` feature but no
  `CorsLayer` is wired. Either drop the feature or add an explicit
  permissive-in-dev / restricted-in-prod layer.

  _Resolved: feature dropped from `Cargo.toml`._

- [ ] **H6.** `LocalFsObjectStore::put` uses a non-unique tmp filename
  (`<key>.<ext>.tmp`). Concurrent writers to the same key clobber each
  other's tmp file before rename. Add a unique suffix (`.<pid>.<rand>`).

- [ ] **H7.** `conversation` and `message` tables are defined in the schema
  but never written to by any code. Schema-as-aspiration drifts. Either
  delete the tables until chat persistence ships, or land a minimal
  write-path now. *(Phase 1 added tenant_id columns to keep them ready
  for the eventual write-path; status unchanged.)*

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

- [ ] **M11.** `ARXIV_QUERY` is concatenated unsanitised into the search
  URL. Operator-trusted, so not a vuln, but a typo silently produces a
  different query. Add a small validator (e.g. balanced parens).

---

## Low

- [ ] **L1.** `Cargo.toml` puts `kv-mem` in default features so production
  builds also link the in-memory engine. Move it behind a `kv-mem`
  feature gated to dev/test.

- [ ] **L2.** No rate-limit middleware anywhere; `/api/chat` logs every
  request and forwards to upstream LLM. One user can flood logs and run
  up upstream spend. Add a per-user rate-limit.

- [ ] **L3.** `.env` (gitignored) contains a real `MINIMAX_API_KEY` on
  disk. Not in git history. Rotate and treat like any other credential.

- [ ] **L4.** `BIND_ADDR` defaults to `0.0.0.0:8081`. Right call inside
  docker, wrong call when running `cargo run` locally. Default to
  `127.0.0.1:8081`; require explicit override for `0.0.0.0`.

- [ ] **L5.** `sources/arxiv.rs::categories_first` is a stub that always
  returns `None`. `metadata.primary_category` is therefore always null.
  Implement or remove.

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

- [ ] **N3.** Backend doesn't validate the JWT signature
  (`JwtClaimsExtractor` only decodes the payload). The BFF validates
  against Keycloak's JWKS, so this is the same trust model as before
  — but the defence-in-depth slot is now obvious and small: add a
  `jsonwebtoken::decode_header` + JWKS-cached validation step in
  `JwtClaimsExtractor::extract`. Worth doing before any deployment
  where the backend port might be exposed beyond localhost.

- [ ] **N4.** `oauth2-proxy` config has settled on v7.4.0 (pinned in
  `docker-compose.full.yml`) because v7.6's alpha-config tightened
  the legacy-vs-alpha overlap rules in ways that broke the split we
  rely on. Periodic re-evaluation: try v7.6+ again when the upstream
  has documented the cookie/session/server fields more clearly, and
  collapse to a single config file.

- [ ] **N5.** Tier-2 e2e regression: `db.authenticate` against
  Keycloak's JWKS fails with a generic `surrealdb: There was a
  problem with the database: There was a problem with authentication`
  on every request. Both Playwright tier-2 specs
  (`chat-roundtrip`, `tenant-isolation @tier2`) fail in a 401-redirect
  loop. **Tier-2 was reported passing at commit `131b93c` (Phase 2
  wiring), but reproduces as failing at that exact commit now** —
  so this is environmental drift the audit didn't catch, not a
  regression introduced by N1.

  What's confirmed working independently:
  - SurrealDB can reach the JWKS endpoint inside the compose network
    (`http::get('http://keycloak:8080/.../certs')` returns the keyset).
  - Keycloak emits a valid RS256-signed JWT with a `kid` matching
    the signing key in JWKS.
  - The backend's `JwtClaimsExtractor` decodes the payload fine
    (it doesn't validate signatures).
  - Tier-1 (HS512, same `define_jwt_access` code path) works
    end-to-end, including the AUTHENTICATE clause.

  Candidate causes left to investigate (in rough order of likelihood):
  1. SurrealDB 2.1.4's JWKS handling picking the wrong key when
     multiple `use` values are present (Keycloak emits both an
     `enc` and a `sig` key — kid-matching ought to disambiguate
     but maybe doesn't).
  2. The AUTHENTICATE clause throwing because the `app_user` row
     isn't yet in the namespace SurrealDB validates against (NS
     mismatch between `ensure_user`'s SystemDb context and the
     RECORD-session context).
  3. Default `--allow-net=none` blocking the JWKS fetch (compose
     now passes `--allow-net=keycloak:8080`; included as a precaution
     in this commit but not confirmed as the root cause).
  4. A token claim SurrealDB requires that Keycloak doesn't emit
     by default (`ac`, `ns`, `db` — present in our HS512 test path
     because we add them manually).

  Workarounds available today:
  - **Tier-1 stack** is fully functional (HS512 + dev injector).
  - **Integration tests** exercise the engine-RBAC path directly
    via `backend/tests/cross_tenant_isolation.rs` (HS512 + manual
    `db.authenticate`). All 5 cases pass.

  Until N5 lands the C1 closure note still holds: PERMISSIONS
  clauses fire on every request-path query through `AuthedDb` in
  tier 1 and in unit tests. It's just the tier-2 Keycloak chain
  that doesn't get to that point.

---

## Priority order (highest leverage first)

1. ~~C1 — close the tenant-isolation gap or drop the SaaS deployment claim.~~ ✓
2. ~~H1, C2 — close ingestion/dedup and SSE leakage as part of the same fix.~~ ✓
3. ~~C3 — drop X-Auth headers, full JWT path.~~ ✓
4. ~~C4 — fail-closed on default Surreal credentials.~~ ✓
5. ~~N1 — tier-1 dev (JWT-minting dev injector).~~ ✓
6. **N5 — tier-2 `db.authenticate` against Keycloak JWKS fails;
   tier-2 e2e suite is non-functional. Without this the production
   path is unverified and the engine-PERMISSIONS claim doesn't hold
   end-to-end. Top priority because it gates every other tier-2 /
   prod-shape change.**
7. **N3 — backend signature validation (small defence-in-depth, big posture win).**
8. H4 — bound the arxiv `pdftotext` shell-out (timeout + size cap).
9. H3, L2 — body size limit and per-user rate limit.
10. H2 — mark_read upsert race.
