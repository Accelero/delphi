# Delphi Microservice Migration Plan

Status: planned. This is the master plan for the greenfield rewrite. The old
implementation is preserved under `old/` as reference only.

Detailed system plans:

- Chat migration status is now tracked in
  `doc/content/docs/architecture/chat-migration.md`.
- [Ingestion microservice migration](./ingestion-microservice-migration.md)
- [Feed microservice migration](./feed-microservice-migration.md)

## 1. Direction

Delphi is rebuilt as a set of Rust services around NATS/JetStream. The first
target stack is Tier 2 only: full auth through Traefik, oauth2-proxy,
Keycloak, Redis session store, production frontend bundle, SurrealDB, NATS,
and independently runnable services.

Initial decisions:

- Rust for backend services and workers.
- React, Tailwind, and shadcn/ui for the frontend.
- WebSocket for browser realtime.
- NATS/JetStream from the first chat implementation; no in-process fallback.
- SurrealDB remains the first durable store, with new clean schemas.
- Storage boundaries must keep later Postgres/Qdrant migration possible.
- Tier 1/dev-auth is deferred until the architecture stabilizes.
- Old code is reference material, not the target structure.

## 2. Target Topology

```text
Browser
  +- HTTP commands/queries ----> api-service
  `- WebSocket realtime -------> realtime-service

api-service ------------------> SurrealDB
api-service ------------------> NATS / JetStream

realtime-service <------------> NATS / JetStream
chat-worker <-----------------> NATS / JetStream + SurrealDB + LLM provider
ingestion workers <-----------> NATS / JetStream + SurrealDB + object store + embedders
feed service/realtime <-------> NATS / JetStream + SurrealDB
```

Runtime services:

- `api-service`: authenticated HTTP commands and read models.
- `realtime-service`: browser WebSocket connections and authorized NATS
  fan-out.
- `chat-worker`: chat turn execution, stop handling, LLM streaming, and
  atomic message commit.
- `ingestion-*` workers: upload validation, extraction, chunking,
  embedding, publish, failure handling, and reconciliation.
- `feed-service`: durable feed reads and feed-specific event shaping once
  the feed product surface is redesigned.

Shared crates:

- `auth`: JWT validation and `AuthContext`.
- `config`: environment-only service config.
- `contracts`: versioned NATS and HTTP wire types.
- `storage`: repository traits plus SurrealDB implementation.
- `nats`: stream/KV bootstrap, subject naming, publishing helpers, consumer
  helpers, and idempotency utilities.

## 3. Foundation Phase

Build only the platform required to validate chat.

Deliverables:

- New Rust workspace and service binaries.
- T2 compose stack with NATS/JetStream added.
- Traefik routes:
  - `/api/*` to `api-service`.
  - `/ws/*` to `realtime-service`.
  - `/healthz` public.
  - `/` to frontend.
- Keycloak realm and oauth2-proxy config adapted from `old/ops`.
- Frontend production Caddy bundle.
- Shared auth validation against Keycloak JWKS.
- SurrealDB bootstrap mechanism.
- NATS stream/KV bootstrap mechanism.
- `/api/auth/me` and health endpoints.

Manual gate:

- Start Tier 2 from a clean checkout.
- Login through Keycloak.
- Frontend calls `/api/auth/me`.
- All services report healthy.
- NATS and SurrealDB are reachable from service containers.

## 4. Phase 1: Chat

Build chat as the first complete vertical slice. This proves full auth,
frontend routing, WebSocket realtime, NATS live state, worker execution, and
SurrealDB persistence.

Detailed current plan: `doc/content/docs/architecture/chat-migration.md`.

Key outcomes:

- Conversation CRUD through `api-service`.
- WebSocket `/ws/chat` through `realtime-service`.
- `CHAT_COMMANDS`, `CHAT_EVENTS`, `CHAT_CONTROL`, and `CHAT_LOCKS`.
- Dedicated `chat-worker` consuming turn commands.
- Multi-tab live convergence through NATS/JetStream.
- Conversation-scoped stop.
- Late-join replay and reconnect resync.
- React chat UI with smoothed streaming and old scroll semantics preserved.
- New backend, frontend, and T2 e2e tests.

Manual gate:

- Single-tab chat round trip.
- Two-tab live fan-out.
- Late join mid-turn.
- Stop from a non-submitting tab.
- Refresh after finish shows committed history.
- Refresh after cancel shows no cancelled turn.
- Two realtime replicas and two chat-worker replicas work without sticky
  routing.

## 5. Phase 2: Ingestion

Rebuild ingestion as a NATS/JetStream saga with idempotent stages and
invisible-until-ready document visibility.

Detailed plan:
[ingestion-microservice-migration.md](./ingestion-microservice-migration.md).

Key outcomes:

- Upload API remains direct-to-object-storage.
- Upload completion starts an async ingestion job instead of doing all heavy
  work in the HTTP request.
- Durable work queues for validation, extraction, chunking, embedding, and
  publish.
- Dedicated embedding workers with independent scale.
- Document visibility controlled by final `state = ready` flip.
- State-derived completion barrier instead of counters.
- Reconciler handles stuck jobs and worker crashes.
- Storage boundary keeps future Postgres/Qdrant split feasible.

Manual gate:

- Upload a PDF through the authenticated frontend.
- Watch job status progress.
- Kill workers mid-stage and observe retry/reconcile.
- Duplicate message delivery does not duplicate chunks or embeddings.
- Corpus reads only show ready documents.

## 6. Phase 3: Feed

Rebuild feed last because it needs product-level rework, not only transport
replacement.

Detailed plan: [feed-microservice-migration.md](./feed-microservice-migration.md).

Key outcomes:

- Define whether feed is document-centric, source-centric, activity-centric,
  or a combined activity/document surface.
- Durable feed reads from storage.
- Live feed updates through NATS and `realtime-service`.
- Missed live events recover through normal feed queries.
- Feed remains tenant-isolated and independent of process-local broadcasts.

Manual gate:

- Ready documents appear in feed.
- Two tabs see live feed updates.
- Missed events recover after refresh/query.
- Feed behavior is stable enough to replace old discovery feed.

## 7. Cross-Cutting Rules

Auth:

- All user-facing HTTP and WebSocket routes run behind oauth2-proxy.
- Services validate JWTs locally through JWKS.
- Every command/event carries tenant and user context from trusted service
  context, not from client-supplied fields.

NATS:

- Use versioned JSON payloads.
- Use deterministic message ids for deduplication where a command may be
  republished.
- Treat JetStream delivery as at-least-once; every handler is idempotent.
- Publish next event only after durable local state is written.
- Ack current work only after next event publish is acknowledged.

Storage:

- SurrealDB is the first implementation, not the permanent contract.
- Services talk through repository interfaces for domain operations.
- Do not expose SurrealDB query syntax or record-id details across service
  boundaries unless explicitly part of public API.

Frontend:

- Build real app surfaces first, not landing pages.
- Use shadcn/ui and lucide icons.
- Keep operational UI compact and task-focused.
- Preserve known-good chat streaming and scroll behavior from the old
  frontend.

Testing:

- Write new tests.
- Validate every phase with unit, integration, T2 e2e, and manual gates.
- Do not port old tests mechanically; preserve scenarios, not old structure.

## 8. Deferred Work

- Tier 1/dev-auth stack.
- Old data migration.
- Real production NATS cluster topology.
- Postgres/Qdrant migration.
- Knowledge graph.
- Feed product extensions beyond the initial rework.
- In-app user administration.
