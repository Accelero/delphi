# Delphi — Product Specification

This is the high-level product specification: **what** Delphi must do,
what constraints shape the product, and which detailed specs own each
area. For implementation architecture, see
[`../architecture/ARCH.md`](../architecture/ARCH.md).

## Contents

- [Product scope](#product-scope)
- [Pillars](#pillars)
- [Cross-cutting requirements](#cross-cutting-requirements)
- [Detailed specifications](#detailed-specifications)
- [Out of scope](#out-of-scope)

## Product scope

Delphi is an authenticated research and knowledge-organization web app.
It helps users discover, ingest, explore, and analyse document corpora,
starting with scholarly papers and generalising to private or
industry-scale document collections.

The same codebase must support:

- **Single-user / private deployments** — one operator, one corpus.
- **Multi-tenant SaaS deployments** — many tenants on shared
  infrastructure with hard tenant isolation.

Delphi is not a public content site. SEO and unauthenticated browsing are
not product requirements.

## Pillars

### Discovery

Bring relevant documents into the user's world.

- Source adapters poll external catalogues under a service identity.
- Semantic filters decide which source results are admitted.
- Accepted documents appear in a per-tenant feed with best-effort live
  updates.
- Active deep-research agents are a later complement to passive source
  polling.

### Exploration

Work with the existing corpus.

- Documents are stored with metadata, original artefact links, extracted
  text, and enrichment outputs.
- Search spans metadata, keyword/full-text, and vector retrieval.
- Corpus RAG chat answers questions with citations into retrieved
  document chunks.

### Analysis

Work with one or more individual documents.

- The same chat surface supports document-focused analysis sessions.
- Multi-document context is a later extension of that surface.

### Knowledge Management

Later milestone. Discovery, exploration, and analysis produce structured
knowledge: claims, entities, relationships, summaries, wiki-like notes,
and graph links. This layer becomes an additional retrieval source for
LLMs and agents.

## Cross-cutting requirements

- **Authentication required.** Every product surface except health checks
  runs behind authenticated identity.
- **Tenant isolation.** Multi-tenant deployments must not rely on
  application query discipline alone; storage must enforce tenant
  boundaries.
- **Untrusted ingestion.** Uploaded files, source-adapter payloads, and
  LLM-derived metadata are untrusted inputs until validated.
- **Direct object transfer.** Large file bytes should move directly
  between browser and object storage; the backend mints short-lived access
  handles and performs only bounded validation/read-back work.
- **Server-authoritative chat.** Chat state must converge across tabs and
  reconnects; no tab is special.
- **Scale path.** The current single-replica live-event model must have a
  clear migration to shared eventing and durable work queues. That plan is
  [`../architecture/scaling-nats.md`](../architecture/scaling-nats.md).

## Detailed specifications

| Spec | Owns |
|---|---|
| [`chat.md`](./chat.md) | Chat behaviour: history, multi-tab live updates, late join, stop, atomic commit, citations. |
| [`ingestion.md`](./ingestion.md) | Document admission behaviour: upload/reference inputs, metadata merge, validation, rejection, user-visible tracking. |

## Out of scope

- Public, unauthenticated corpus browsing.
- In-app user administration, sign-up, password reset, or role editing.
- SEO and public-site concerns.
- Collaborative multi-user chat in one conversation.
- Durable knowledge graph and agentic reasoning in the current milestone.
