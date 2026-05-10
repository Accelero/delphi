# Delphi — Code Audit Findings

Audit performed against the Rust backend, SurrealDB schema, BFF
(Traefik + oauth2-proxy + Dex) configs, frontend SPA, and architecture
docs. Findings are grouped by severity and have stable IDs so they can
be referenced from PRs and checked off as fixes land.

Mark items as `[x]` once a fix has been merged and verified.

---

## Critical

- [ ] **C1.** Tenancy gap: `docs/ARCH.md` claims every domain record carries
  `tenant_id` and that SurrealDB record-level access rules enforce isolation.
  In reality the `document`, `document_content`, `chunk`,
  `document_version`, `source_state`, and `feed_read` tables have no
  tenant field, the schema has no `DEFINE ACCESS` rules, and the backend
  connects as SurrealDB `Root` (which would bypass any rule). No storage
  method takes a tenant filter. With more than one tenant in the same DB,
  every read leaks across tenants.

- [ ] **C2.** SSE event broadcast (`api/discovery.rs::events`) is a single
  global `broadcast::Sender<NewDocumentEvent>`; every authenticated user
  subscribes to every other tenant's new-document titles in real time.
  Either partition the channel by tenant or filter on subscribe.

- [ ] **C3.** Backend trust boundary is purely network. `HeaderClaimsExtractor`
  performs no signature validation. Tier-1 dev exposes `8081:8081` to the
  host; in dev mode the injector overwrites headers, but a misbuilt prod
  image (no `dev-auth` feature) on the same compose snippet is one mistake
  away from `curl -H 'X-Auth-User-Id: admin'` superuser. Add a
  defence-in-depth extractor (JWT validation or `X-Internal-Auth`
  shared-secret) before any non-localhost deployment.

- [ ] **C4.** `docker-compose.full.yml` ships `RUST_ENV=production` together
  with `SURREAL_USER=root / SURREAL_PASS=root`. `.env.example` documents
  the same defaults. Refuse to start when `RUST_ENV=production` and
  Surreal credentials match the documented defaults; or move credentials
  to required-env (no defaults) for the prod compose.

---

## High

- [ ] **H1.** `Pipeline::ingest` dedup is global: `get_document_by_canonical`
  is not tenant-scoped, and `document.canonical_id` has a global UNIQUE
  index. Two tenants ingesting the same arXiv paper collide; the second
  silently no-ops and inherits the first's metadata. Twin of C1 in the
  ingestion direction.

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

- [ ] **H5.** `Cargo.toml` enables `tower-http`'s `cors` feature but no
  `CorsLayer` is wired. Either drop the feature or add an explicit
  permissive-in-dev / restricted-in-prod layer.

- [ ] **H6.** `LocalFsObjectStore::put` uses a non-unique tmp filename
  (`<key>.<ext>.tmp`). Concurrent writers to the same key clobber each
  other's tmp file before rename. Add a unique suffix (`.<pid>.<rand>`).

- [ ] **H7.** `conversation` and `message` tables are defined in the schema
  but never written to by any code. Schema-as-aspiration drifts. Either
  delete the tables until chat persistence ships, or land a minimal
  write-path now.

---

## Medium

- [ ] **M1.** `Storage::wipe` and `Storage::counts` are global — no tenant
  scoping. `delphi admin wipe` would nuke every tenant's data. Require an
  explicit `--tenant=` argument or `--all-tenants` confirmation flag.

- [ ] **M2.** `SurrealStorage` and `Surreal<Any>` leak across the storage
  module boundary. `auth/bootstrap.rs` runs raw SurrealQL against the
  underlying handle; `storage::SurrealStorage` is `pub`-exported as an
  "escape hatch". Move `ensure_user` / `resolve_default_tenant` etc.
  behind `Storage` trait methods so the trait is the only seam.

- [ ] **M3.** Dependency-direction inversion: `auth/middleware.rs` and
  `auth/bootstrap.rs` import `surrealdb::Surreal<Any>` directly. Per
  CLAUDE.md ("depend on abstractions, not implementations"), `auth`
  should depend on the `Storage` trait, not SurrealDB. Linked to M2.

- [ ] **M4.** `surrealdb::Datetime` leaks across the storage interface
  (`Document.published_at`, `Document.ingested_at`, `FeedCursor.ingested_at`).
  ARCH.md says "SurrealDB types do not leak across the module boundary".
  Replace with `chrono::DateTime<Utc>` in `Document` / `FeedCursor`; do
  the wire-format conversion inside the SurrealStorage impl on serialize.

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

- [ ] **M10.** `useFeedEvents` doesn't surface SSE connection state.
  Network-flap UX is "live updates silently stop". Expose a
  `connectionState` and/or refetch first page on focus after long
  hidden duration to repair gaps.

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
  `groups` claim from OIDC arrives as-is via oauth2-proxy. A user in
  group `Acme` misses the `acme` tenant row and falls back to default.
  Normalize the claim in `HeaderClaimsExtractor` (or a dedicated
  `Claims::normalize` step) before `ensure_user`.

- [ ] **L8.** `oauth2-proxy/alpha.yaml` maps the multi-value `groups`
  claim to `X-Auth-Tenant-Id` (single value). oauth2-proxy comma-joins,
  so two groups → `groupA,groupB` → no tenant matches. Pick the first
  element explicitly, or commit to a documented "first group is tenant"
  parse on the backend.

- [ ] **L9.** `roles` claim is split on `,` with no canonical separator
  policy. Document the constraint or use a named-list header
  convention.

- [ ] **L10.** `data/` is created root-owned by docker compose; local
  `cargo` / `bun` runs can't read or write it. Set `user:` in compose
  to host UID, or document the root-owned dir.

- [ ] **L11.** `storage/mod.rs` re-exports `SurrealStorage` as `pub` but
  `surreal_from_env` as `pub(crate)`. After the lib/bin split,
  `pub(crate)` suffices for both. Keep the public surface honest.

---

## Module-structure / coupling notes

- [ ] **S1.** `auth` module imports `surrealdb::Surreal<Any>` directly
  (linked to M2/M3). Re-shape so `auth` depends on the `Storage` trait,
  not on a SurrealDB handle.

- [ ] **S2.** `api/mod.rs::serve` is the composition root and reaches
  into `storage::surreal_from_env`, `llm::llm_from_env`,
  `object_store::from_url`, etc. Consider moving wiring into a separate
  `compose.rs` so `api` stays purely about HTTP routing.

---

## Priority order (highest leverage first)

1. C1 — close the tenant-isolation gap or drop the SaaS deployment claim.
2. H1, C2 — close ingestion/dedup and SSE leakage as part of the same fix.
3. C3, C4 — defence-in-depth on the trust boundary and fail-closed on
   default Surreal credentials.
4. H4 — bound the arxiv `pdftotext` shell-out (timeout + size cap).
5. M2 / M3 / M4 / S1 — push SurrealDB types behind the storage trait.
6. H3, L2 — body size limit and per-user rate limit.
