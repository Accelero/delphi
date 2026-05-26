# Storage Backend

Decision record + deep reference for the storage layer. Sister doc to
[`ARCH.md`](./ARCH.md). The authoritative **schema** lives in
[`backend/schema.surql`](../../backend/schema.surql); the authoritative
**implementation** lives in `backend/src/storage/`. This document
captures the *why* — what stays here is decision history, not specs the
code already encodes.

## Decision

**SurrealDB is the single store.** Documents, extracted content, chunk
embeddings, version history, sessions, conversations, knowledge edges
(later) — everything Delphi persists lives in one engine.

Application code uses typed storage boundaries: request handlers operate
through `AuthedDb` / the `Storage` trait, and privileged paths use
`SystemDb`. SurrealDB-specific types are contained inside `storage/`
except for narrow system/bootstrap paths.

## Why SurrealDB

- **One store, one query language, ACID across models.** Vector + graph
  + tabular + full-text in a single transaction. No CDC pipeline, no
  dual-write, no async-ack consistency window.
- **Fewer moving parts.** One container in dev, one workload in prod.
- **Cross-model queries** that the heterogeneous design would make
  expensive (e.g., "papers semantically similar to X, mentioned by chunks
  cited in papers I've annotated") become single SurrealQL expressions.
- **Runtime-polymorphic engine.** `Surreal::<Any>::connect(url)` selects
  the engine at startup from the URL — `ws://` in prod, `memory` in
  tests, `rocksdb:` for embedded single-process. Same code path,
  different durability/scale envelope.

## What we give up vs. CNPG-canonical

- **Continuous WAL → S3 + PITR.** SurrealDB's backup story is logical
  dumps + volume snapshots. RPO is "minutes to nightly," not "seconds."
  Acceptable for the current target audience; not acceptable for systems
  that can't afford to lose minutes of writes.
- **Maturity.** Younger ecosystem, smaller community, less battle-tested
  at scale.

We accept these in exchange for a much simpler architecture. If they
become real problems we migrate to a heterogeneous design via the
`Storage` trait.

## Rejected alternatives

- **CNPG (Postgres) + Qdrant + CDC (Debezium + Redpanda).** Stronger
  per-component, mature backup, well-understood. But: more services, async
  sync layer, ack problem, dual-store consistency window. Overbuilt for a
  single-tenant research tool. Documented as the v2 destination if
  SurrealDB ever falls short.
- **Postgres + pgvector + Apache AGE (multi-model in Postgres).** Keeps
  Postgres' backup story and collapses to one store, but pgvector and AGE
  are weaker at their respective jobs than SurrealDB's native engines.
  Worth revisiting if backup RPO becomes a hard requirement and we're
  willing to trade graph/vector quality for it.

## What the abstraction deliberately does NOT cover

- **Backend-specific power features.** SurrealDB graph/live-query features
  are intentionally not abstracted until a subsystem needs them.
- **Auth bootstrap and system operations** stay off the request `Storage`
  trait. They use `SystemDb`, the explicit privileged handle.

## Identity and dedup

- **Record id / `doc_id`** is the stable identity for manual uploads.
- **`canonical_id`** is optional and used for natural-source dedup
  (arXiv ID, DOI, normalized URL hash) when present.
- **Dedup is per tenant and unique-when-set** through the storage-computed
  `dedup_key`.
- **`content_hash`** is source-dependent: SHA-256 for JSON ingestion,
  opaque S3 ETag for manual upload records.

## Vector dimension trade-off

The HNSW index hardcodes `DIMENSION 384` to match `BAAI/bge-small-en-v1.5`.
Switching to a model with a different dimension requires:

1. Drop the `chunk_embedding` index.
2. Re-define it with the new `DIMENSION`.
3. Re-embed all chunks (an ingester batch job, not a storage-layer
   concern).

Acceptable in v1. If we need model swaps to be smoother we'll templatise
the schema or split chunks per-model into separate tables.

## Consistency model

Everything is one engine, one transaction. When `upsert_document` or
`upsert_chunks` returns, the data is durable, indexed, and queryable.
The current upload `/complete` path commits document + content + session
delete in one transaction. The future NATS ingest pipeline keeps the same
"invisible until publish" invariant; see [`scaling-nats.md`](./scaling-nats.md).

## Schema-application strategy

The backend applies `schema.surql` on every `serve()` startup. Every
statement is `IF NOT EXISTS`, so re-application is a no-op when the
schema is current and a one-time setup when it isn't.

This stops being safe the day we need a *destructive* migration (rename
or drop). At that point the path is: numbered migration files +
`schema_version` row + version guard at startup. Not before — premature
migration tooling has a real maintenance tax.

## Backup and restore

**Not implemented yet.** Added when we deploy to a long-lived
environment.

Planned shape (covered in detail in a future ops doc):

- **Nightly job:** `surreal export` → `zstd` → object store.
- **Pre-delete hook** on the deployment artifact: same export, runs
  automatically on teardown so a final backup is taken before resources
  are torn down.
- **Post-install hook** on first boot of an empty DB: restore from the
  latest object.
- **Quarterly restore drill**: nuke a staging environment, redeploy,
  verify data returns.

For dev (compose) there is no backup; nuking the volume is fine.

## Open questions

- **Vector dim flexibility.** Currently hardcoded 384. Acceptable until
  we want to experiment with multiple models simultaneously.
- **Graph edges.** The schema does not yet define relations like
  `cites`, `mentions`, `part_of`. Added when the Knowledge subsystem is
  designed.
- **Tenant isolation enforcement.** Implemented through `tenant_id`
  defaults/assertions and SurrealDB `PERMISSIONS` clauses.
- **Live queries / push notifications.** SurrealDB supports them and
  they could replace the dropped CDC pipeline if we ever want push-based
  sync to derived stores. Not used in v1.

## References

- Functional spec: [`../specs/SPEC.md`](../specs/SPEC.md)
- Architecture overview: [`ARCH.md`](./ARCH.md)
- Authoritative schema: [`backend/schema.surql`](../../backend/schema.surql)
- SurrealDB docs: <https://surrealdb.com/docs/surrealdb>
