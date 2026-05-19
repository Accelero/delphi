# Delphi — Functional Specification

Delphi is a knowledge organization and research application: a RAG-centric LLM
tooling suite that helps researchers **discover**, **explore**, and **analyse**
a corpus of documents. The first target audience is academic researchers
working with scholarly papers. The same primitives generalise to
industry-scale knowledge management over arbitrary document corpora.

The product is a webapp. It is an authenticated tool, not a public site; SEO
is not a concern. It must run both as a single-user private deployment and as
a multi-tenant SaaS.

## Pillars

Delphi is organised around three user-facing pillars plus a cross-cutting
knowledge layer.

### 1. Discovery

Bringing new, relevant documents into the user's world.

- **Source adapters.** Pluggable connectors that pull documents from external
  catalogues on a schedule. The first reference adapter targets Semantic
  Scholar and monitors newly published papers. The adapter contract is
  generic so further sources (arXiv, PubMed, internal repositories, etc.) can
  be added without touching core logic.
- **Semantic filters.** Each adapter is gated by user-defined filters
  expressed as research questions and/or topics derived from the existing
  corpus. Only documents that pass the filter reach the user.
- **Feed & notifications.** Documents that pass the filter appear in a
  per-user feed. Notification channels alert the user when new items match.
- **Deep research tool.** An agentic LLM workflow that performs targeted web
  search for scholarly material on demand. It complements the passive
  adapters with active, user-initiated retrieval.

### 2. Exploration

Working with the existing corpus.

- **Corpus storage.** Each ingested document persists its metadata, full
  content, vector embedding, and a link back to its origin URL.
- **Traditional search.** Metadata, keyword, and field-scoped queries over
  the corpus.
- **RAG chat with the corpus.** A chat interface backed by retrieval over the
  vector index, letting the user ask questions answered with citations into
  the corpus. The chat surface itself (multi-tab equality, stop, late-join,
  persistence semantics) is specified in
  [`specs/chat.md`](./specs/chat.md).

### 3. Analysis

Working with individual documents.

- **Document chat.** The user pulls a paper from the corpus into a chat
  surface and converses with an LLM "research buddy" about it: clarifying
  passages, extracting claims, comparing to other documents, drafting notes.
  Reuses the chat surface specified in
  [`specs/chat.md`](./specs/chat.md).
- **Multi-document context.** Analysis sessions can span more than one
  document so the LLM can reason across a small working set.

### 4. Knowledge Management (later milestone)

A persistent layer built from what discovery, exploration, and analysis
produce.

- **Automated extraction.** LLM passes process papers and chat sessions to
  extract structured knowledge: claims, entities, relationships, summaries.
- **Knowledge base.** Stored either as a wiki-style markdown graph
  (Obsidian-like) or as a property graph in the database. Users can browse,
  edit, and link entries.
- **LLM enrichment.** The knowledge base is exposed to LLMs as an additional
  retrieval source alongside the document corpus.
- **Agentic reasoning (advanced).** Long-running agents may use the
  knowledge base to perform automated research, hypothesis generation, and
  cross-document synthesis.

## Deployment modes

Delphi is built so the same codebase serves two shapes of deployment:

- **Single-user / private.** One researcher, one deployment, no tenancy
  concerns beyond authentication.
- **Multi-tenant SaaS.** Many tenants and users on a shared deployment, with
  hard data separation between tenants.

The application's own logic is **tenancy-agnostic**. Authentication,
identity, and tenant separation are delegated to infrastructure (reverse
proxy + external OIDC + database-level access control). The application
trusts the identity context it is handed and applies it consistently.

## Out of scope (for now)

- Public, unauthenticated browsing of corpora.
- In-app user administration (sign-up, password reset, role editing) — this
  is owned by an external admin panel.
- SEO and any concerns specific to public web surfaces.
