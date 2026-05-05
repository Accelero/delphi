-- delphi storage schema (SurrealDB single-store)
--
-- Applied by `delphi_storage init`.
-- Re-applying is safe: every statement uses IF NOT EXISTS.
--
-- Vector dimension is hardcoded to 384 (BAAI/bge-small-en-v1.5).
-- Changing models with a different dim requires dropping and recreating
-- the chunk_embedding index.

-- ---------------------------------------------------------------------------
-- analyzer used by full-text indexes
-- ---------------------------------------------------------------------------
DEFINE ANALYZER IF NOT EXISTS text_en
    TOKENIZERS blank, class
    FILTERS lowercase, snowball(english);

-- ---------------------------------------------------------------------------
-- document
-- One row per ingested artifact. Canonical record of identity and metadata.
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS document SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS canonical_id ON document TYPE string;
DEFINE FIELD IF NOT EXISTS source_type  ON document TYPE string;
DEFINE FIELD IF NOT EXISTS source_uri   ON document TYPE string;
DEFINE FIELD IF NOT EXISTS storage_uri  ON document TYPE option<string>;
DEFINE FIELD IF NOT EXISTS title        ON document TYPE option<string>;
DEFINE FIELD IF NOT EXISTS authors      ON document TYPE array<string> DEFAULT [];
DEFINE FIELD IF NOT EXISTS published_at ON document TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS ingested_at  ON document TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS language     ON document TYPE option<string>;
DEFINE FIELD IF NOT EXISTS content_hash ON document TYPE string;
DEFINE FIELD IF NOT EXISTS version      ON document TYPE int DEFAULT 1;
DEFINE FIELD IF NOT EXISTS metadata     ON document FLEXIBLE TYPE object DEFAULT {};

DEFINE INDEX IF NOT EXISTS document_canonical_id ON document FIELDS canonical_id UNIQUE;
DEFINE INDEX IF NOT EXISTS document_source_type  ON document FIELDS source_type;
DEFINE INDEX IF NOT EXISTS document_content_hash ON document FIELDS content_hash;

-- ---------------------------------------------------------------------------
-- document_content
-- Extracted text. Separated from `document` because it is large and changes
-- independently (re-extraction with a better parser).
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS document_content SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS doc          ON document_content TYPE record<document>;
DEFINE FIELD IF NOT EXISTS format       ON document_content TYPE string;
DEFINE FIELD IF NOT EXISTS text         ON document_content TYPE string;
DEFINE FIELD IF NOT EXISTS extractor    ON document_content TYPE string;
DEFINE FIELD IF NOT EXISTS extracted_at ON document_content TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS document_content_doc  ON document_content FIELDS doc UNIQUE;
DEFINE INDEX IF NOT EXISTS document_content_text ON document_content FIELDS text SEARCH ANALYZER text_en BM25;

-- ---------------------------------------------------------------------------
-- chunk
-- Vector + text + payload in one record. Source of truth for retrieval.
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS chunk SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS doc             ON chunk TYPE record<document>;
DEFINE FIELD IF NOT EXISTS ordinal         ON chunk TYPE int;
DEFINE FIELD IF NOT EXISTS char_start      ON chunk TYPE int;
DEFINE FIELD IF NOT EXISTS char_end        ON chunk TYPE int;
DEFINE FIELD IF NOT EXISTS page            ON chunk TYPE option<int>;
DEFINE FIELD IF NOT EXISTS bbox            ON chunk FLEXIBLE TYPE option<object>;
DEFINE FIELD IF NOT EXISTS text            ON chunk TYPE string;
DEFINE FIELD IF NOT EXISTS embedding       ON chunk TYPE array<float>;
DEFINE FIELD IF NOT EXISTS embedding_model ON chunk TYPE string;
DEFINE FIELD IF NOT EXISTS chunk_strategy  ON chunk TYPE string;
DEFINE FIELD IF NOT EXISTS created_at      ON chunk TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS chunk_doc       ON chunk FIELDS doc;
DEFINE INDEX IF NOT EXISTS chunk_unique    ON chunk FIELDS doc, ordinal, embedding_model, chunk_strategy UNIQUE;
DEFINE INDEX IF NOT EXISTS chunk_text      ON chunk FIELDS text SEARCH ANALYZER text_en BM25;
DEFINE INDEX IF NOT EXISTS chunk_embedding ON chunk FIELDS embedding HNSW DIMENSION 384 DIST COSINE TYPE F32 EFC 200 M 16;

-- ---------------------------------------------------------------------------
-- document_version (append-only history)
-- ---------------------------------------------------------------------------
DEFINE TABLE IF NOT EXISTS document_version SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS doc            ON document_version TYPE record<document>;
DEFINE FIELD IF NOT EXISTS version        ON document_version TYPE int;
DEFINE FIELD IF NOT EXISTS content_hash   ON document_version TYPE string;
DEFINE FIELD IF NOT EXISTS change_summary ON document_version TYPE option<string>;
DEFINE FIELD IF NOT EXISTS recorded_at    ON document_version TYPE datetime DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS document_version_unique ON document_version FIELDS doc, version UNIQUE;

-- ===========================================================================
-- AUTH / TENANCY
--
-- `app_user` (not `user`) avoids collision with Surreal's `DEFINE USER`.
-- Identity is `(iss, sub)` from the OIDC token, NOT email — emails change.
-- Membership is its own table (not a Surreal RELATE edge) so we can attach
-- role/billing fields cleanly.
-- ===========================================================================
DEFINE TABLE IF NOT EXISTS tenant SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS slug               ON tenant TYPE string
    ASSERT string::len($value) > 0 AND $value = string::lowercase($value);
DEFINE FIELD IF NOT EXISTS name               ON tenant TYPE string;
DEFINE FIELD IF NOT EXISTS plan               ON tenant TYPE string DEFAULT 'free';
DEFINE FIELD IF NOT EXISTS billing_email      ON tenant TYPE option<string>;
DEFINE FIELD IF NOT EXISTS stripe_customer_id ON tenant TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at         ON tenant TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS metadata           ON tenant FLEXIBLE TYPE object DEFAULT {};
DEFINE INDEX IF NOT EXISTS tenant_slug        ON tenant FIELDS slug UNIQUE;

DEFINE TABLE IF NOT EXISTS app_user SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS iss          ON app_user TYPE string;
DEFINE FIELD IF NOT EXISTS sub          ON app_user TYPE string;
DEFINE FIELD IF NOT EXISTS email        ON app_user TYPE string;
DEFINE FIELD IF NOT EXISTS display_name ON app_user TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at   ON app_user TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_seen_at ON app_user TYPE option<datetime>;
DEFINE INDEX IF NOT EXISTS app_user_identity ON app_user FIELDS iss, sub UNIQUE;
DEFINE INDEX IF NOT EXISTS app_user_email    ON app_user FIELDS email;

DEFINE TABLE IF NOT EXISTS membership SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS user      ON membership TYPE record<app_user>;
DEFINE FIELD IF NOT EXISTS tenant    ON membership TYPE record<tenant>;
DEFINE FIELD IF NOT EXISTS role      ON membership TYPE string
    ASSERT $value INSIDE ['owner','admin','member','viewer'];
DEFINE FIELD IF NOT EXISTS joined_at ON membership TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS membership_user_tenant ON membership FIELDS user, tenant UNIQUE;
DEFINE INDEX IF NOT EXISTS membership_user        ON membership FIELDS user;
DEFINE INDEX IF NOT EXISTS membership_tenant      ON membership FIELDS tenant;

-- ===========================================================================
-- SESSIONS — backing store for `tower-sessions`. The `data` field holds the
-- full serialized session payload (including any tokens stored by axum-oidc);
-- `expiry_date` lets the periodic cleanup task DELETE expired rows.
-- ===========================================================================
DEFINE TABLE IF NOT EXISTS session SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS data        ON session FLEXIBLE TYPE object;
DEFINE FIELD IF NOT EXISTS expiry_date ON session TYPE datetime;
DEFINE INDEX IF NOT EXISTS session_expiry ON session FIELDS expiry_date;

-- ===========================================================================
-- CONVERSATIONS / MESSAGES
--
-- Schema is defined here so the chat-history persistence change can land
-- cleanly without a separate migration. The chat handler does NOT write to
-- these tables yet — that's the next plan.
-- ===========================================================================
DEFINE TABLE IF NOT EXISTS conversation SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS tenant     ON conversation TYPE record<tenant>;
DEFINE FIELD IF NOT EXISTS user       ON conversation TYPE record<app_user>;
DEFINE FIELD IF NOT EXISTS title      ON conversation TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at ON conversation TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at ON conversation TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS metadata   ON conversation FLEXIBLE TYPE object DEFAULT {};
DEFINE INDEX IF NOT EXISTS conversation_tenant_user ON conversation FIELDS tenant, user;
DEFINE INDEX IF NOT EXISTS conversation_updated_at  ON conversation FIELDS updated_at;

DEFINE TABLE IF NOT EXISTS message SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS conversation   ON message TYPE record<conversation>;
DEFINE FIELD IF NOT EXISTS tenant         ON message TYPE record<tenant>;  -- denormalized for tenant-scoped queries
DEFINE FIELD IF NOT EXISTS role           ON message TYPE string
    ASSERT $value INSIDE ['system','user','assistant','tool'];
DEFINE FIELD IF NOT EXISTS content        ON message TYPE string;
DEFINE FIELD IF NOT EXISTS parts          ON message FLEXIBLE TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS model          ON message TYPE option<string>;
DEFINE FIELD IF NOT EXISTS finish_reason  ON message TYPE option<string>;
DEFINE FIELD IF NOT EXISTS created_at     ON message TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS message_conversation   ON message FIELDS conversation;
DEFINE INDEX IF NOT EXISTS message_tenant_created ON message FIELDS tenant, created_at;
