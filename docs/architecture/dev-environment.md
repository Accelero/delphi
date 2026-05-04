# Dev Environment

Status: in-progress
Last updated: 2026-05-03

## Scope

How to run, develop against, and debug the system locally. Production
deployment (k3s, Helm, S3 backups) is covered separately.

## Topology

The compose stack is the SurrealDB single-store from
[`storage-backend.md`](./storage-backend.md). Two services:

```
┌──────────────┐         ┌──────────────────────┐
│  application │ ──────▶ │      SurrealDB       │
│   (future)   │         │ (rocksdb, persistent) │
└──────────────┘         └──────────────────────┘

  one-shot CLI:
   - storage  (init / status / wipe)
```

| Service     | Image                          | Host port |
| ----------- | ------------------------------ | --------- |
| `surrealdb` | `surrealdb/surrealdb:v2.1.4`   | 8000      |
| `storage`   | local build (one-shot CLI)     | —         |

The `storage` service is gated behind the `tools` profile and only runs
when invoked via `docker compose run`.

## Bring-up

```sh
make up           # starts surrealdb, then runs `storage init`
make status       # prints row counts per table
```

The `init` step is idempotent. Re-running it is safe.

After `make up`:

- HTTP / RPC endpoint: <http://localhost:8000> (RPC at `ws://localhost:8000/rpc`)
- SurrealQL shell: `make surql`
- Default credentials: `root` / `root` (dev only — change in prod)
- Namespace / database: `delphi` / `main`

## Storage abstraction

The Rust crate `delphi_storage` (in `services/storage/`) is the only
module that talks to SurrealDB. Other services depend on it as a library:

```rust
use delphi_storage::{storage_from_env, Document, Content, Chunk, Filters};

let storage = storage_from_env().await?;
let doc_id = storage.upsert_document(&Document {
    canonical_id: "arxiv:2301.04104".into(),
    source_type: "arxiv".into(),
    source_uri: "https://arxiv.org/abs/2301.04104".into(),
    content_hash: sha256_hex(&text),
    title: Some("DreamerV3".into()),
    authors: vec!["Hafner".into(), "...".into()],
    ..Default::default()
}).await?;

storage.upsert_content(&doc_id, &Content {
    text: text.clone(),
    format: "markdown".into(),
    extractor: "pdfplumber@0.11".into(),
}).await?;

storage.upsert_chunks(&doc_id, &chunks).await?;

let results = storage.search_vector(&query_vec, 20, &Filters::default()).await?;
```

Backend selection happens via the `STORAGE_BACKEND` env var. Currently
only `surreal` is implemented. Adding a new backend means writing a type
that implements the `Storage` trait and registering it in
`storage_from_env`.

## What lives where

### SurrealDB

Schema in `services/storage/delphi_storage/schema.surql`:

- `document` — identity, metadata.
- `document_content` — extracted text + BM25 full-text index.
- `chunk` — text + embedding + offsets, with HNSW vector index and BM25.
- `document_version` — append-only history.

All indexes (unique constraints, full-text, vector HNSW) are defined in
the schema and applied by `make init`.

### Storage layer

In `services/storage/`:

- `Cargo.toml` — crate manifest.
- `schema.surql` — applied by `init` (embedded in the binary via
  `include_str!`).
- `src/models.rs` — `Document`, `Content`, `Chunk`, `ChunkSearchResult`,
  `Filters`.
- `src/backend.rs` — `Storage` trait.
- `src/surreal.rs` — `SurrealStorage` implementation.
- `src/config.rs` — `storage_from_env()` factory.
- `src/bin/delphi_storage.rs` — CLI: `init`, `status`, `wipe`.

## Common operations

```sh
make up        # start surrealdb + apply schema
make down      # stop, keep volume
make nuke      # stop + delete the data volume
make init      # re-apply schema (idempotent)
make status    # row counts
make wipe      # delete data, keep schema
make surql     # open interactive SurrealQL shell
make logs      # tail container logs
```

Manual SurrealQL examples:

```sql
-- in `make surql`
USE NS delphi DB main;

INFO FOR DB;                           -- list tables/indexes
SELECT count() FROM document;
SELECT * FROM chunk LIMIT 5;

-- vector search (after data is loaded)
SELECT id, text, vector::distance::knn() AS score
FROM chunk
WHERE embedding <|10|> [0.1, 0.2, ...]
ORDER BY score ASC
LIMIT 10;

-- full-text search
SELECT id, text, search::score(0) AS score
FROM chunk
WHERE text @0@ "world models"
ORDER BY score DESC
LIMIT 10;
```

## Debugging

**Schema apply failed.** Check `docker compose logs surrealdb` for parse
errors. The most common cause is a SurrealDB version mismatch — confirm
the image tag in `docker-compose.yml` matches the syntax in `schema.surql`.

**Connection from host fails but works inside compose.** Use
`ws://localhost:8000/rpc` from the host (the port-mapped endpoint) and
`ws://surrealdb:8000/rpc` from inside the compose network. Both serve
the same instance.

**Vector search returns nothing or errors on dimension.** The HNSW index
hardcodes 384. If your ingester used a different model dim, either drop
and recreate the index with the new dim, or use the matching model
(`BAAI/bge-small-en-v1.5` for 384).

**Need to inspect raw data.** `make surql` opens a Surreal shell with
the right namespace/database and pretty-printing on.

## Resetting state

| What                              | Command                  |
| --------------------------------- | ------------------------ |
| Restart, keep data                | `make down && make up`   |
| Drop data, keep schema            | `make wipe`              |
| Wipe everything (volume + schema) | `make nuke && make up`   |
| Re-apply schema after a change    | `make init`              |

## Parity with production

| Concern              | Dev (compose)               | Prod (k3s, planned)                  |
| -------------------- | --------------------------- | ------------------------------------ |
| SurrealDB            | one container, RocksDB      | StatefulSet, RocksDB on PVC          |
| Auth                 | `root`/`root` inline        | Secret + dedicated user              |
| Backup               | none (nuke the volume)      | Nightly CronJob + Helm pre-delete hook → S3 |
| Restore              | n/a                         | Helm post-install hook from latest S3 |
| TLS                  | none                        | Ingress with cert-manager            |
| Persistence sizing   | docker volume               | PVC with planned capacity            |

The application contract — schema, the `Storage` interface — is identical
across environments. Only the operator-managed concerns differ.

## Next steps

- **Ingester service** that turns PDFs / web pages into rows in
  `document` + `document_content` + `chunk`. Computes embeddings via
  fastembed and writes via the storage layer.
- **Helm chart** with `values-dev.yaml` and `values-prod.yaml` overlays.
- **Backup hooks** (CronJob + Helm pre-delete + post-install restore).
- **Static chart validation** in CI (`helm lint`, `kubeconform`).
- **k3d job in GitHub Actions** for chart install/upgrade tests before
  homelab deploys.
