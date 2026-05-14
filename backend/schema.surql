-- delphi storage schema (SurrealDB single-store)
--
-- Applied automatically on every `delphi serve` startup (idempotent).
-- Re-applying is safe: every statement uses IF NOT EXISTS / IF EXISTS.
--
-- Vector dimension is hardcoded to 384 (BAAI/bge-small-en-v1.5).
-- Changing models with a different dim requires dropping and recreating
-- the chunk_embedding index.
--
-- ===========================================================================
-- Multi-tenancy
--
-- Every domain row carries a `tenant_id: record<tenant>` field with
-- DEFAULT $auth.tenant_id and ASSERT $value != NONE. Enforcement is
-- engine-side only — application code does NOT thread tenant ids
-- through storage calls:
--
-- 1. On CREATE under a RECORD session (request path via JWT-bound
--    AuthedDb), the DEFAULT clause fills tenant_id from $auth, so the
--    application doesn't need to set it. The ASSERT then guarantees a
--    tenant is present.
-- 2. PERMISSIONS clauses on every domain table compare
--    `tenant_id = $auth.tenant_id` — a request that authenticates as a
--    record user via `DEFINE ACCESS … TYPE RECORD WITH JWT` cannot
--    read or write cross-tenant; engine refuses.
-- 3. Root / system sessions (boot, scheduler, admin) DO NOT bypass
--    DEFAULT — `$auth` is NONE under root, so $auth.tenant_id is NONE
--    and the ASSERT fails unless the caller explicitly sets tenant_id.
--    This is intentional: the cross-tenant write path must be explicit.
--
-- Why `$auth.tenant_id` not `$token.tenant_id`: the JWT's `tenant_id`
-- claim is a slug emitted by the IdP (e.g. `tenant-a`). Comparing it
-- to a record link requires either a `type::thing()` cast (only safe
-- if the IdP emits the full `tenant:slug` form) or a subquery per row.
-- Denormalizing the user's tenant onto `app_user.tenant_id` lets us
-- compare record-to-record directly and keeps the IdP unaware of
-- SurrealDB's record-id formatting.
--
-- System users (`Root` / NS / DB owner-editor) bypass `PERMISSIONS`
-- entirely. The backend's `SystemDb` connection is a system user, so
-- the privileged paths (boot, ensure_user pre-flight, scheduler, admin
-- CLI) operate above-RBAC by design — that's the contained trust
-- surface (see docs/architecture/multitenancy.md).
--
-- DEFINE ACCESS itself is *not* in this file: it requires the
-- HS512/RSA key or JWKS URL from runtime env, applied by `SystemDb`
-- at startup (see `system::define_jwt_access`).
--
-- If applied to a pre-tenancy DB with existing rows, the NOT NULL
-- ASSERT on the new tenant_id column will fail — pre-customer,
-- expected, wipe and re-apply.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- analyzer used by full-text indexes
-- ---------------------------------------------------------------------------
DEFINE ANALYZER IF NOT EXISTS text_en
    TOKENIZERS blank, class
    FILTERS lowercase, snowball(english);

-- ===========================================================================
-- AUTH / TENANCY
--
-- `app_user` (not `user`) avoids collision with Surreal's `DEFINE USER`.
-- Identity is `(iss, sub)` from the OIDC token, NOT email — emails change.
-- Membership is its own table (not a Surreal RELATE edge) so we can attach
-- role/billing fields cleanly. Listed first so domain tables can FK to it.
-- ===========================================================================
DEFINE TABLE IF NOT EXISTS tenant SCHEMAFULL
    PERMISSIONS FOR select WHERE id = $auth.tenant_id;
DEFINE FIELD IF NOT EXISTS slug               ON tenant TYPE string
    ASSERT string::len($value) > 0 AND $value = string::lowercase($value);
DEFINE FIELD IF NOT EXISTS name               ON tenant TYPE string;
DEFINE FIELD IF NOT EXISTS plan               ON tenant TYPE string DEFAULT 'free';
DEFINE FIELD IF NOT EXISTS billing_email      ON tenant TYPE option<string>;
DEFINE FIELD IF NOT EXISTS stripe_customer_id ON tenant TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at         ON tenant TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS metadata           ON tenant FLEXIBLE TYPE object DEFAULT {};
DEFINE INDEX IF NOT EXISTS tenant_slug        ON tenant FIELDS slug UNIQUE;

-- `tenant_id` is denormalized from `membership` so PERMISSIONS clauses on
-- other tables can compare to `$auth.tenant_id` without a subquery. Set by
-- `ensure_user` on every login. v1 enforces single-tenant per user at the
-- application layer; if/when multi-tenant lands, the model becomes
-- "active tenant" with a token claim selecting which membership to bind.
DEFINE TABLE IF NOT EXISTS app_user SCHEMAFULL
    PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE FIELD IF NOT EXISTS iss          ON app_user TYPE string;
DEFINE FIELD IF NOT EXISTS sub          ON app_user TYPE string;
DEFINE FIELD IF NOT EXISTS email        ON app_user TYPE string;
DEFINE FIELD IF NOT EXISTS display_name ON app_user TYPE option<string>;
DEFINE FIELD IF NOT EXISTS tenant_id    ON app_user TYPE record<tenant>;
DEFINE FIELD IF NOT EXISTS created_at   ON app_user TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_seen_at ON app_user TYPE option<datetime>;
DEFINE INDEX IF NOT EXISTS app_user_identity ON app_user FIELDS iss, sub UNIQUE;
DEFINE INDEX IF NOT EXISTS app_user_email    ON app_user FIELDS email;

-- membership: which tenant(s) does an app_user belong to.
-- v1 enforces single-membership at the API; schema permits multi.
DEFINE TABLE IF NOT EXISTS membership SCHEMAFULL
    PERMISSIONS FOR select WHERE user = $auth.id AND tenant_id = $auth.tenant_id;
DEFINE FIELD IF NOT EXISTS user      ON membership TYPE record<app_user>;
-- legacy `tenant` field (pre-tenancy schema) — drop on apply if present.
REMOVE FIELD IF EXISTS tenant ON membership;
DEFINE FIELD IF NOT EXISTS tenant_id ON membership TYPE record<tenant>;
DEFINE FIELD IF NOT EXISTS role      ON membership TYPE string
    ASSERT $value INSIDE ['owner','admin','member','viewer'];
DEFINE FIELD IF NOT EXISTS joined_at ON membership TYPE datetime DEFAULT time::now();
-- legacy index names (pre-tenancy schema) — drop on apply if present.
REMOVE INDEX IF EXISTS membership_user_tenant ON membership;
REMOVE INDEX IF EXISTS membership_tenant ON membership;
DEFINE INDEX IF NOT EXISTS membership_user_tenant_id ON membership FIELDS user, tenant_id UNIQUE;
DEFINE INDEX IF NOT EXISTS membership_user           ON membership FIELDS user;
DEFINE INDEX IF NOT EXISTS membership_tenant_id      ON membership FIELDS tenant_id;

-- ---------------------------------------------------------------------------
-- document
-- One row per ingested artefact. Canonical record of identity and metadata.
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS document SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;

DEFINE FIELD OVERWRITE tenant_id    ON document TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS canonical_id ON document TYPE string;
DEFINE FIELD IF NOT EXISTS source_type  ON document TYPE string;
DEFINE FIELD IF NOT EXISTS source_uri   ON document TYPE string;
DEFINE FIELD IF NOT EXISTS storage_uri  ON document TYPE option<string>;
DEFINE FIELD IF NOT EXISTS title        ON document TYPE option<string>;
DEFINE FIELD IF NOT EXISTS authors      ON document TYPE array<string> DEFAULT [];
DEFINE FIELD IF NOT EXISTS published_at ON document TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS ingested_at  ON document TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS language     ON document TYPE option<string>;
-- Author/publisher-written short prose (paper abstract, book flap copy,
-- article deck). Distinct from `document_content.text`, which holds the
-- full body. Optional: not every source provides one.
DEFINE FIELD IF NOT EXISTS summary      ON document TYPE option<string>;
DEFINE FIELD IF NOT EXISTS content_hash ON document TYPE string;
DEFINE FIELD IF NOT EXISTS version      ON document TYPE int DEFAULT 1;
DEFINE FIELD IF NOT EXISTS metadata     ON document FLEXIBLE TYPE object DEFAULT {};
-- RAG v1: document-level embedding for "similar papers" + paper-rank reranking.
-- Populated at ingest by SPECTER2 over `title + [SEP] + abstract`. 768-dim.
-- Surfacing in the UI is deferred (see docs/architecture/rag.md "Deferred").
DEFINE FIELD IF NOT EXISTS paper_embedding       ON document TYPE option<array<float>>;
DEFINE FIELD IF NOT EXISTS paper_embedding_model ON document TYPE option<string>;

-- canonical_id is unique *per tenant*. Closes audit finding H1: pre-tenancy
-- schema had a global UNIQUE which silently merged cross-tenant rows.
REMOVE INDEX IF EXISTS document_canonical_id ON document;
DEFINE INDEX IF NOT EXISTS document_tenant_canonical ON document FIELDS tenant_id, canonical_id UNIQUE;
DEFINE INDEX IF NOT EXISTS document_tenant_id        ON document FIELDS tenant_id;
DEFINE INDEX IF NOT EXISTS document_source_type      ON document FIELDS source_type;
DEFINE INDEX IF NOT EXISTS document_content_hash     ON document FIELDS content_hash;
-- RAG v1: HNSW over SPECTER2 paper embeddings (768-dim). Used later by
-- "similar papers" features; doesn't drive chat retrieval (that's
-- chunk-level).
DEFINE INDEX IF NOT EXISTS document_paper_embedding ON document FIELDS paper_embedding HNSW DIMENSION 768 DIST COSINE TYPE F32 EFC 200 M 16;

-- ---------------------------------------------------------------------------
-- document_content
-- Extracted text. Separated from `document` because it is large and changes
-- independently (re-extraction with a better parser).
-- tenant_id denormalized for query-speed and to make PERMISSIONS rules
-- direct (no traversal through `doc`).
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS document_content SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;

DEFINE FIELD OVERWRITE tenant_id    ON document_content TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS doc          ON document_content TYPE record<document>;
DEFINE FIELD IF NOT EXISTS format       ON document_content TYPE string;
DEFINE FIELD IF NOT EXISTS text         ON document_content TYPE string;
DEFINE FIELD IF NOT EXISTS extractor    ON document_content TYPE string;
DEFINE FIELD IF NOT EXISTS extracted_at ON document_content TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS document_content_doc       ON document_content FIELDS doc UNIQUE;
DEFINE INDEX IF NOT EXISTS document_content_tenant_id ON document_content FIELDS tenant_id;
DEFINE INDEX IF NOT EXISTS document_content_text      ON document_content FIELDS text SEARCH ANALYZER text_en BM25;

-- ---------------------------------------------------------------------------
-- chunk
-- Vector + text + payload in one record. Source of truth for retrieval.
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS chunk SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;

DEFINE FIELD OVERWRITE tenant_id       ON chunk TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS doc             ON chunk TYPE record<document>;
DEFINE FIELD IF NOT EXISTS ordinal         ON chunk TYPE int;
DEFINE FIELD IF NOT EXISTS char_start      ON chunk TYPE int;
DEFINE FIELD IF NOT EXISTS char_end        ON chunk TYPE int;
-- RAG v1: single-page `page` + single-rect `bbox` superseded by `bboxes`
-- (an array of `{page, x, y, w, h}` line rectangles). Drop the legacy
-- fields — no rows in the wild yet since no chunker had run.
REMOVE FIELD IF EXISTS page ON chunk;
REMOVE FIELD IF EXISTS bbox ON chunk;
DEFINE FIELD IF NOT EXISTS bboxes          ON chunk FLEXIBLE TYPE option<array<object>>;
DEFINE FIELD IF NOT EXISTS text            ON chunk TYPE string;
DEFINE FIELD IF NOT EXISTS embedding       ON chunk TYPE array<float>;
DEFINE FIELD IF NOT EXISTS embedding_model ON chunk TYPE string;
DEFINE FIELD IF NOT EXISTS chunk_strategy  ON chunk TYPE string;
DEFINE FIELD IF NOT EXISTS created_at      ON chunk TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS chunk_doc       ON chunk FIELDS doc;
DEFINE INDEX IF NOT EXISTS chunk_tenant_id ON chunk FIELDS tenant_id;
DEFINE INDEX IF NOT EXISTS chunk_unique    ON chunk FIELDS doc, ordinal, embedding_model, chunk_strategy UNIQUE;
DEFINE INDEX IF NOT EXISTS chunk_text      ON chunk FIELDS text SEARCH ANALYZER text_en BM25;
DEFINE INDEX IF NOT EXISTS chunk_embedding ON chunk FIELDS embedding HNSW DIMENSION 384 DIST COSINE TYPE F32 EFC 200 M 16;

-- ---------------------------------------------------------------------------
-- document_version (append-only history)
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS document_version SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;

DEFINE FIELD OVERWRITE tenant_id      ON document_version TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS doc            ON document_version TYPE record<document>;
DEFINE FIELD IF NOT EXISTS version        ON document_version TYPE int;
DEFINE FIELD IF NOT EXISTS content_hash   ON document_version TYPE string;
DEFINE FIELD IF NOT EXISTS change_summary ON document_version TYPE option<string>;
DEFINE FIELD IF NOT EXISTS recorded_at    ON document_version TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS document_version_unique    ON document_version FIELDS doc, version UNIQUE;
DEFINE INDEX IF NOT EXISTS document_version_tenant_id ON document_version FIELDS tenant_id;

-- ---------------------------------------------------------------------------
-- source_state
-- Per-adapter incremental-fetch cursor. Opaque JSON object so each adapter
-- owns its own shape (e.g. `{ "since": "<ISO8601>" }`). Keyed by
-- (tenant_id, adapter) so v2 can run per-tenant scheduler ingest.
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS source_state SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;

DEFINE FIELD OVERWRITE tenant_id  ON source_state TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS adapter    ON source_state TYPE string;
DEFINE FIELD IF NOT EXISTS cursor     ON source_state FLEXIBLE TYPE object DEFAULT {};
DEFINE FIELD IF NOT EXISTS updated_at ON source_state TYPE datetime DEFAULT time::now();

REMOVE INDEX IF EXISTS source_state_adapter ON source_state;
DEFINE INDEX IF NOT EXISTS source_state_tenant_adapter ON source_state FIELDS tenant_id, adapter UNIQUE;

-- ===========================================================================
-- (Sessions table removed: the BFF — Traefik + oauth2-proxy — owns session
-- storage in front of the backend. The backend is stateless w.r.t. user
-- sessions; identity arrives on every request as the IdP-issued JWT.)
-- ===========================================================================

-- ===========================================================================
-- CONVERSATIONS / MESSAGES
--
-- Schema reserved for the chat-history persistence work. Currently unused
-- by code (see audit finding H7); kept here so the eventual landing is
-- migration-free.
-- ===========================================================================
DEFINE TABLE IF NOT EXISTS conversation SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id AND user = $auth.id
        FOR create WHERE tenant_id = $auth.tenant_id AND user = $auth.id;
REMOVE FIELD IF EXISTS tenant ON conversation;
DEFINE FIELD OVERWRITE tenant_id  ON conversation TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD OVERWRITE user       ON conversation TYPE record<app_user>
    DEFAULT $auth.id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS title      ON conversation TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at ON conversation TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at ON conversation TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS metadata   ON conversation FLEXIBLE TYPE object DEFAULT {};
REMOVE INDEX IF EXISTS conversation_tenant_user ON conversation;
DEFINE INDEX IF NOT EXISTS conversation_tenant_user ON conversation FIELDS tenant_id, user;
DEFINE INDEX IF NOT EXISTS conversation_updated_at  ON conversation FIELDS updated_at;

DEFINE TABLE IF NOT EXISTS message SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;
REMOVE FIELD IF EXISTS tenant ON message;
DEFINE FIELD IF NOT EXISTS conversation   ON message TYPE record<conversation>;
DEFINE FIELD OVERWRITE tenant_id      ON message TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS role           ON message TYPE string
    ASSERT $value INSIDE ['system','user','assistant','tool'];
DEFINE FIELD IF NOT EXISTS content        ON message TYPE string;
DEFINE FIELD IF NOT EXISTS parts          ON message FLEXIBLE TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS model          ON message TYPE option<string>;
DEFINE FIELD IF NOT EXISTS finish_reason  ON message TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at     ON message TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS message_conversation   ON message FIELDS conversation;
REMOVE INDEX IF EXISTS message_tenant_created ON message;
DEFINE INDEX IF NOT EXISTS message_tenant_created ON message FIELDS tenant_id, created_at;
