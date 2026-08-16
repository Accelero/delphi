CREATE TABLE IF NOT EXISTS tenant (
  tenant_id text PRIMARY KEY,
  name text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app_user (
  tenant_id text NOT NULL,
  user_id text NOT NULL,
  email text,
  display_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz,
  PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS chat_conversation (
  tenant_id text NOT NULL,
  user_id text NOT NULL,
  conversation_id text NOT NULL,
  title text NOT NULL DEFAULT 'New chat',
  next_message_ordinal bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  PRIMARY KEY (tenant_id, conversation_id)
);

CREATE TABLE IF NOT EXISTS chat_message (
  tenant_id text NOT NULL,
  user_id text NOT NULL,
  conversation_id text NOT NULL,
  message_id text NOT NULL,
  role text NOT NULL CHECK (role IN ('system', 'user', 'assistant', 'tool')),
  content text NOT NULL DEFAULT '',
  parent_message_id text,
  citations jsonb NOT NULL DEFAULT '[]',
  turn_id text,
  interrupted boolean NOT NULL DEFAULT false,
  finish_reason text,
  ordinal bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, message_id),
  UNIQUE (tenant_id, conversation_id, ordinal)
);

CREATE TABLE IF NOT EXISTS chat_turn (
  tenant_id text NOT NULL,
  turn_id text NOT NULL,
  user_id text NOT NULL,
  conversation_id text NOT NULL,
  user_message_id text,
  assistant_message_id text,
  parent_message_id text,
  status text NOT NULL CHECK (status IN ('committed', 'interrupted', 'failed')),
  worker_id text,
  error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, turn_id)
);

CREATE INDEX IF NOT EXISTS chat_conversation_owner_updated_idx
  ON chat_conversation (tenant_id, user_id, updated_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS chat_message_owner_order_idx
  ON chat_message (tenant_id, user_id, conversation_id, ordinal);

CREATE INDEX IF NOT EXISTS chat_turn_conversation_created_idx
  ON chat_turn (tenant_id, conversation_id, created_at);

CREATE TABLE IF NOT EXISTS document (
  tenant_id text NOT NULL,
  document_id text NOT NULL,
  owner_user_id text NOT NULL,
  document_version bigint NOT NULL DEFAULT 1,
  state text NOT NULL CHECK (state IN ('active', 'deleted', 'tombstoned', 'failed', 'staging', 'validating', 'indexing', 'ready')),
  title text,
  metadata jsonb NOT NULL DEFAULT '{}',
  object_key text,
  object_etag text,
  object_size_bytes bigint,
  content_sha256 text,
  content_type text,
  filename text,
  source_type text,
  source_uri text,
  storage_key text,
  declared_size bigint,
  ready_at timestamptz,
  failed_at timestamptz,
  failed_reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  PRIMARY KEY (tenant_id, document_id)
);

CREATE INDEX IF NOT EXISTS document_owner_active_idx
  ON document (tenant_id, owner_user_id, updated_at DESC)
  WHERE state = 'active';

CREATE INDEX IF NOT EXISTS document_owner_updated_idx
  ON document (tenant_id, owner_user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_job (
  tenant_id text NOT NULL,
  user_id text NOT NULL,
  job_id text NOT NULL,
  document_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('validating', 'extracting', 'chunking', 'embedding', 'publishing', 'ready', 'failed')),
  current_stage text,
  pipeline_version bigint NOT NULL CHECK (pipeline_version > 0),
  attempt bigint NOT NULL CHECK (attempt >= 1),
  error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, job_id),
  UNIQUE (tenant_id, document_id, pipeline_version)
);

CREATE INDEX IF NOT EXISTS ingestion_job_status_updated_idx
  ON ingestion_job (tenant_id, status, updated_at);

CREATE TABLE IF NOT EXISTS outbox_event (
  event_id text PRIMARY KEY,
  subject text NOT NULL,
  event_type text NOT NULL,
  tenant_id text NOT NULL,
  aggregate_id text NOT NULL,
  aggregate_version bigint NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'publishing', 'published', 'failed')),
  publish_attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE INDEX IF NOT EXISTS outbox_event_publish_idx
  ON outbox_event (status, next_attempt_at, created_at)
  WHERE status IN ('pending', 'failed', 'publishing');
