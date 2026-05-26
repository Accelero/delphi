# Delphi — Architecture

This is the implementation entrypoint: **how** Delphi is built. It stays
slim and links to deeper documents for subsystem detail. For product
requirements, see [`../specs/SPEC.md`](../specs/SPEC.md).

## Contents

- [Topology](#topology)
- [Runtime services](#runtime-services)
- [Auth and tenancy](#auth-and-tenancy)
- [Backend modules](#backend-modules)
- [Frontend](#frontend)
- [Deployment tiers](#deployment-tiers)
- [Architecture references](#architecture-references)

## Topology

```text
Browser
  ├─ SPA HTTP/SSE ──▶ Traefik / oauth2-proxy ──▶ Delphi backend
  │                         │                         │
  │                         ├─ OIDC provider           ├─ SurrealDB
  │                         └─ Redis session store     ├─ object storage
  │                                                   ├─ LLM providers / sidecars
  │                                                   └─ TEI embedding sidecars
  └─ direct PUT/GET ────────────────────────────────▶ object storage
```

NATS is **not in the stack yet**. It is the selected future backbone for
horizontal scaling of live eventing and ingest work queues; see
[`scaling-nats.md`](./scaling-nats.md).

## Runtime services

- **Frontend.** React/Vite SPA. Tier 1 serves via Vite; Tier 2 serves a
  production bundle from Caddy behind Traefik.
- **Backend.** Rust/Axum HTTP service. JSON APIs plus SSE streams for
  chat/feed events.
- **SurrealDB.** Single persistence engine for tenants/users, documents,
  content, chunks, embeddings, conversations, messages, upload sessions,
  and future graph data.
- **Object storage.** S3-compatible store, MinIO in compose. Browser
  uploads/downloads use presigned direct-to-store URLs minted by the
  backend.
- **OIDC + BFF.** Traefik and oauth2-proxy protect the app. The browser
  keeps an HttpOnly session cookie; oauth2-proxy stores sessions in
  Redis and forwards the IdP-issued JWT to the backend as
  `Authorization: Bearer`.
- **LLM and embedding providers.** Chat uses the configured LLM client.
  Title generation and metadata extraction use separate utility-LLM
  factories. Embeddings use TEI sidecars when enabled.

## Auth and tenancy

Production identity is the bearer JWT, not projected `X-Auth-*` headers.
The backend validates JWT signatures and standard claims via
`JwtClaimsExtractor` and a `JwtValidator` (`HS512` in Tier 1/dev/tests,
JWKS in Tier 2/prod). It then passes the same JWT to SurrealDB
`db.authenticate`, where the `app_session` access method validates it
again and binds `$auth` to the `app_user` record.

Request handlers run on `AuthedDb`, a pool-borrowed SurrealDB connection
authenticated as the caller. SurrealDB `PERMISSIONS` clauses enforce
`tenant_id = $auth.tenant_id` on domain tables. Privileged `SystemDb`
paths are intentionally narrow: schema boot, JWT access definition,
first-login provisioning, scheduler cursor writes, rejection writes, and
admin tooling.

Tenant records are provisioned from trusted IdP claims on first login in
the current operational model. That means SaaS deployments must treat the
IdP and BFF configuration as the tenant-admission control plane.

Detailed reference: [`multitenancy.md`](./multitenancy.md).

## Backend modules

- **`api/`** owns Axum routing and thin handlers.
- **`auth/`** owns JWT extraction, validation, cold-path provisioning,
  and `AuthContext`.
- **`storage/`** owns SurrealDB access through `SystemDb`,
  `RequestDbPool`, `AuthedDb`, and the `Storage` trait.
- **`ingestion/`** owns upload sessions, validation, completion,
  metadata autofill, legacy JSON ingestion, RAG decoration, and feed
  notification.
- **`object_store/`** owns S3-compatible storage and access minting.
- **`chat/`** owns the v4 `TurnBus`, in-process live turn transport, and
  chat worker.
- **`llm/`, `embedder/`, `chunker/`, `text_extractor/`** own provider
  seams and document-processing utilities.
- **`sources/`** owns source-adapter registry and scheduler.

## Frontend

The SPA assumes requests are already authenticated by the edge. Key
surfaces:

- Upload workflow: `UploadManager`, Uppy multipart driver, persistent
  tracker, and `/upload` route.
- Discovery feed: infinite query, best-effort SSE live prepend, PDF
  viewer using direct object URLs.
- Chat: conversation sidebar plus server-authoritative SSE stream hook;
  history comes from storage, live turns from `TurnBus`.

## Deployment tiers

- **Tier 1 (`docker-compose.yml`).** Inner-loop development: SurrealDB,
  backend with `dev-auth`, frontend dev server, MinIO, sidecars as
  configured. Dev auth mints a JWT with the same claim shape and signing
  policy the backend/SurrealDB validate.
- **Tier 2 (`docker-compose.full.yml`).** Production-shaped local stack:
  Traefik, oauth2-proxy, Keycloak, Redis, SurrealDB, backend without
  `dev-auth`, built frontend served by Caddy, MinIO, sidecars.
- **Production.** Tier 2 shape with managed IdP/secrets/TLS/storage/DB as
  appropriate. Redis remains an oauth2-proxy session-store dependency;
  NATS will become the app-owned event/work backbone when the scaling
  migration lands.

## Architecture references

| Document | Status | Owns |
|---|---:|---|
| [`scaling-nats.md`](./scaling-nats.md) | planned | Horizontal scaling with NATS for chat, ingest work queues, and feed fan-out. |
| [`chat-v4.md`](./chat-v4.md) | implemented, NATS-ready | Server-authoritative chat streaming and the `TurnBus` contract. |
| [`ingestion.md`](./ingestion.md) | implemented | Direct upload, validation, completion, metadata merge, object storage. |
| [`ingestion-unify-on-upload.md`](./ingestion-unify-on-upload.md) | planned | Converging adapter/bot ingestion onto the upload path and retiring legacy JSON ingestion. |
| [`object-access.md`](./object-access.md) | implemented | Direct-to-storage upload/download access minting. |
| [`object-validator.md`](./object-validator.md) | implemented reference | Uploaded-object validation and parser hardening. |
| [`metadata-extractor.md`](./metadata-extractor.md) | implemented | LLM-backed metadata autofill seam and config. |
| [`title-llm.md`](./title-llm.md) | implemented | Dedicated utility LLM for first-turn chat titles. |
| [`rag.md`](./rag.md) | implemented v1 | Chunking, embeddings, retrieval, and grounded citations. |
| [`discovery-feed.md`](./discovery-feed.md) | implemented | Feed pagination and current process-local live updates. |
| [`multitenancy.md`](./multitenancy.md) | implemented | JWT-bound SurrealDB record access and tenant isolation. |
| [`storage-backend.md`](./storage-backend.md) | decision record | SurrealDB rationale and storage constraints. |
| [`testing.md`](./testing.md) | active | Test tiers and repo layout. |
| [`error-handling.md`](./error-handling.md) | planned | Future structured error envelope. |
