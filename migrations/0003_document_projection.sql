-- Document read model, rebuilt by folding the DOCUMENT_EVENTS log.
--
-- Schema rules for this file are correctness requirements, not style:
--   * text, never varchar(n)
--   * no CHECK constraints -- a projection that can reject an event can be
--     poisoned by one
--   * no foreign keys -- events can arrive in an order that violates them
--   * jsonb for anything not queried directly
--
-- The tables dropped here are from the pre-event-sourcing document path. They
-- carry no data worth keeping; dev databases and buckets are reset.

DROP TABLE IF EXISTS document CASCADE;
DROP TABLE IF EXISTS upload_session CASCADE;
DROP TABLE IF EXISTS ingestion_job CASCADE;
DROP TABLE IF EXISTS outbox_event CASCADE;

CREATE TABLE document (
  tenant_id       text   NOT NULL,
  document_id     text   NOT NULL,
  owner_user_id   text   NOT NULL,

  version         bigint NOT NULL,      -- dense, user-visible
  stream_seq      bigint NOT NULL,      -- JetStream seq of last applied event

  state           text   NOT NULL,      -- 'active' | 'deleted'
  index_state     text   NOT NULL,      -- 'pending' | 'current' | 'failed'
  index_version   bigint,

  current_blob    text,                 -- upload_id of the serving blob
  filename        text,
  content_type    text,
  byte_size       bigint,
  checksum        text,

  title           text,
  tags            jsonb  NOT NULL DEFAULT '[]',
  description     text,
  metadata        jsonb  NOT NULL DEFAULT '{}',

  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,

  PRIMARY KEY (tenant_id, document_id)
);

CREATE INDEX document_owner_updated_idx
  ON document (tenant_id, owner_user_id, updated_at DESC);

-- GC probes this per object; without it every probe is a sequential scan.
CREATE INDEX document_current_blob_idx
  ON document (tenant_id, current_blob);

CREATE TABLE projection_checkpoint (
  name        text   PRIMARY KEY,
  stream_seq  bigint NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projection_failure (
  name        text   NOT NULL,
  stream_seq  bigint NOT NULL,
  subject     text   NOT NULL,
  payload     jsonb  NOT NULL,
  error       text   NOT NULL,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (name, stream_seq)
);

-- One row per upload attempt, from preflight to terminal outcome.
-- Operational: drives GET /uploads/{id} and the "someone is uploading" hint.
-- Never consulted for correctness.
CREATE TABLE upload_attempt (
  tenant_id      text NOT NULL,
  upload_id      text NOT NULL,
  document_id    text NOT NULL,
  owner_user_id  text NOT NULL,
  mode           text NOT NULL,          -- 'create' | 'replace'
  status         text NOT NULL,          -- 'uploading' | 'scanning'
                                         -- | 'accepted' | 'rejected'
  filename       text NOT NULL,
  byte_size      bigint NOT NULL,
  version        bigint,                 -- set on accepted
  superseded     boolean NOT NULL DEFAULT false,
  reason         text,                   -- set on rejected
  started_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, upload_id)
);

CREATE INDEX upload_attempt_document_idx
  ON upload_attempt (tenant_id, document_id) WHERE status IN ('uploading', 'scanning');

-- Swept by age; the index keeps the sweep from scanning the whole table.
CREATE INDEX upload_attempt_started_idx
  ON upload_attempt (started_at);
