# Storage Backend

Status: decided
Last updated: 2026-05-03

## Scope

This document defines the storage layer that underpins all subsystems
(Monitoring, Deep Research, Knowledge, Discussion). It covers what is stored,
how the schema is shaped, and how the abstraction is structured to allow
swapping the underlying database later.

Out of scope: extraction pipelines, chunking strategy details, retrieval
ranking, knowledge-graph construction. Those are separate documents.

## Decision

**SurrealDB is the single store.** Documents, extracted content, chunk
embeddings, version history, and (later) graph edges all live in one engine.

Application code does not import SurrealDB directly. It depends on the
**`Storage` protocol** in `services/storage/delphi_storage/backend.py`. The
SurrealDB implementation is one concrete backend behind that protocol; a
Postgres + Qdrant backend can be added later by implementing the same
interface, without touching callers.

### Why SurrealDB

- **One store, one query language, ACID across models.** Vector + graph
  + tabular + full-text in a single transaction. No CDC pipeline, no dual-
  write, no async ack problem.
- **Fewer moving parts.** One container in dev, one workload in k3s.
- **Cross-model queries** that the heterogeneous design would make
  expensive (e.g., "papers semantically similar to X, mentioned by chunks
  cited in papers I've annotated") become single SurrealQL expressions.

### What we give up vs. CNPG-canonical

- **Continuous WAL → S3 + PITR.** SurrealDB's backup story is logical
  dumps + volume snapshots. RPO is "minutes to nightly," not "seconds."
  Acceptable for a personal research tool; not acceptable for systems
  that can't afford to lose a day of writes.
- **Maturity.** Younger ecosystem, smaller community, less battle-tested
  at scale.

We accept these in exchange for a much simpler architecture. If they
become real problems we migrate to a heterogeneous design via the
abstraction layer.

### Rejected alternatives

- **CNPG (Postgres) + Qdrant + CDC (Debezium + Redpanda).** Stronger
  per-component, mature backup, well-understood. But: more services,
  async sync layer, ack problem, dual-store consistency window. Overbuilt
  for a single-user research tool. Documented as the v2 destination if
  SurrealDB ever falls short.
- **Postgres + pgvector + Apache AGE (multi-model in Postgres).** Keeps
  CNPG's backup story and collapses to one store, but pgvector and AGE
  are weaker at their respective jobs than SurrealDB's native engines.
  Worth revisiting if backup RPO becomes a hard requirement and we're
  willing to trade graph/vector quality for it.

## Abstraction layer

`services/storage/delphi_storage` is a Python package that all other
services import. The contract is in `backend.py`:

```python
class Storage(Protocol):
    def connect(self) -> None: ...
    def init_schema(self) -> None: ...
    def upsert_document(self, doc: Document) -> str: ...
    def upsert_content(self, doc_id: str, content: Content) -> None: ...
    def upsert_chunks(self, doc_id: str, chunks: list[Chunk]) -> list[str]: ...
    def search_vector(self, query_vec, top_k=20, filters=None) -> list[ChunkSearchResult]: ...
    def search_keyword(self, query, top_k=20, filters=None) -> list[ChunkSearchResult]: ...
    # …get/list/delete variants
```

Construction is via `storage_from_env()` (in `config.py`), which picks an
implementation based on `STORAGE_BACKEND`. Currently only `surreal` is
supported; adding a new backend means writing a class that satisfies the
protocol and registering it in `storage_from_env`.

This means the rest of the codebase reads:

```python
from delphi_storage import storage_from_env, Document, Chunk
storage = storage_from_env()
storage.connect()
doc_id = storage.upsert_document(doc)
storage.upsert_chunks(doc_id, chunks)
```

No import of `surrealdb` outside the storage package. The day we add a
heterogeneous backend, every other service is unaffected.

### What the abstraction deliberately does NOT cover

- **Backend-specific power features.** SurrealDB's graph traversals,
  live queries, custom functions are accessible only through the
  `SurrealStorage` class directly. Higher-level subsystems that want
  them can import `SurrealStorage` explicitly and accept the coupling.
  The protocol stays small.
- **Async I/O.** The current interface is synchronous. If we need async
  later, we add an `AsyncStorage` protocol alongside, not in place of.

## Schema

Defined in `services/storage/delphi_storage/schema.surql`. Applied by
`python -m delphi_storage init`. Idempotent.

### Tables

**`document`** — one row per ingested artifact.

| Field          | Type                       | Notes                                        |
| -------------- | -------------------------- | -------------------------------------------- |
| canonical_id   | string                     | Unique. arXiv ID, DOI, URL hash, etc.        |
| source_type    | string                     | `arxiv`, `pdf`, `webpage`, `note`, …         |
| source_uri     | string                     | Where it came from.                          |
| storage_uri    | option<string>             | Where the file lives, if any.                |
| title          | option<string>             |                                              |
| authors        | array<string>              |                                              |
| published_at   | option<datetime>           |                                              |
| ingested_at    | datetime                   | Default `time::now()`.                       |
| language       | option<string>             |                                              |
| content_hash   | bytes                      | SHA-256 of normalized content; dedup key.    |
| version        | int                        |                                              |
| metadata       | flexible object            | Source-type-specific extras.                 |

Indexes: unique on `canonical_id`, plain on `source_type`, plain on
`content_hash`.

**`document_content`** — extracted text, separated from `document` because
it's large and is re-extracted independently.

| Field        | Type                    | Notes                                    |
| ------------ | ----------------------- | ---------------------------------------- |
| doc          | record<document>        | Unique (1:1 with document).              |
| format       | string                  | `text`, `markdown`, `html`.              |
| text         | string                  | The extracted content.                   |
| extractor    | string                  | Name + version, e.g. `pdfplumber@0.11`. |
| extracted_at | datetime                |                                          |

Indexes: unique on `doc`, full-text BM25 on `text` via the `text_en` analyzer.

**`chunk`** — vector + text + payload in one record. The retrieval workhorse.

| Field           | Type                | Notes                                       |
| --------------- | ------------------- | ------------------------------------------- |
| doc             | record<document>    |                                             |
| ordinal         | int                 | 0-based index within doc.                   |
| char_start      | int                 | Offset into `document_content.text`.        |
| char_end        | int                 |                                             |
| page            | option<int>         | For paginated sources (PDF).                |
| bbox            | option<object>      | `{x, y, w, h}` for jump-to-region.          |
| text            | string              |                                             |
| embedding       | array<float>        | The vector itself.                          |
| embedding_model | string              | E.g. `fastembed/bge-small-en-v1.5`.         |
| chunk_strategy  | string              | E.g. `fixed-512-50`.                        |
| created_at      | datetime            |                                             |

Indexes:
- Plain on `doc`.
- Unique on `(doc, ordinal, embedding_model, chunk_strategy)`. Lets multiple
  chunk sets coexist during model migrations.
- Full-text BM25 on `text`.
- HNSW vector index on `embedding`, dimension **384** (bge-small-en-v1.5),
  cosine distance, `EFC=200`, `M=16`.

**`document_version`** — append-only history. A new row each time
`content_hash` changes. PK: `(doc, version)`.

### A note on `embedding` dimension

The HNSW index hardcodes 384 to match `BAAI/bge-small-en-v1.5`. Switching
to a model with a different dimension requires:

1. Drop the `chunk_embedding` index.
2. Re-define it with the new `DIMENSION`.
3. Re-embed all chunks (handled by an ingester batch job, not the storage
   layer).

This is acceptable in v1. If we need model swaps to be smoother we'll
templatize the schema or split chunks per-model into separate tables.

## Identity and dedup

- **`canonical_id`** is the user-facing identifier (arXiv ID, DOI,
  normalized URL hash). UNIQUE.
- **`content_hash`** = SHA-256 of normalized extracted text. Re-ingesting
  unchanged content is a no-op; a changed hash bumps `version` and writes
  a `document_version` row.
- SurrealDB record IDs (e.g. `document:abc…`) are an internal detail.
  External code passes them around as opaque strings (`doc_id: str`).

## Consistency model

Everything is one engine, one transaction. When `upsert_document` or
`upsert_chunks` returns, the data is durable, indexed, and queryable.
There is no async pipeline, no eventual consistency window, no ack
mechanism to build.

## Backup and restore

This is the operational price for the simpler architecture. **Not
implemented yet** — added when we deploy to k3s.

Planned (covered in detail in a future ops doc):

- **Nightly CronJob** in k3s: `surreal export` → `zstd` → S3.
- **Pre-delete Helm hook**: same export, runs automatically on
  `pulumi destroy` / `helm uninstall` so a final backup is taken before
  resources are torn down.
- **Post-install Helm hook**: on first boot of an empty DB, restore from
  the latest S3 object.
- **Quarterly restore drill**: nuke, redeploy, verify data returns.

For dev (compose) there is no backup; nuking the volume is fine.

## Open questions

- **Vector dim flexibility.** Currently hardcoded 384. Acceptable until
  we want to experiment with multiple models simultaneously.
- **Graph edges.** The schema does not yet define relations like
  `cites`, `mentions`, `part_of`. Added when the Knowledge subsystem
  is designed.
- **Multi-tenancy.** Currently single-user. SurrealDB has namespace +
  database + scope auth; revisit when relevant.
- **Live queries / push notifications.** SurrealDB supports them and
  they could replace the dropped CDC pipeline if we ever want push-based
  sync to external derived stores. Not used in v1.

## References

- Original spec: [`research-tool-spec.md`](../research-tool-spec.md)
- Dev environment: [`dev-environment.md`](./dev-environment.md)
- SurrealDB docs: <https://surrealdb.com/docs/surrealdb>
