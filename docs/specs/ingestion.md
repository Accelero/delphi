# Document Ingestion — Functional Specification

How documents enter Delphi's corpus. Sub-spec of [`../SPEC.md`](../SPEC.md)
(the Discovery and Analysis pillars both depend on ingestion). This
document describes the **end-goal state** — what ingestion should do when
complete. For how it is built today and what is still missing, see
[`../architecture/ingestion.md`](../architecture/ingestion.md) and
[`../architecture/ingestion-roadmap.md`](../architecture/ingestion-roadmap.md).

## Purpose

Ingestion turns an arbitrary source document (a PDF, a text/markdown
file, eventually more types) into a first-class **corpus document**: a
row with structured metadata, extracted full text, a stored original
artefact, and — once enrichment runs — embeddings and a knowledge-graph
footprint. Everything downstream (search, RAG chat, document analysis,
the knowledge layer) reads what ingestion produces.

## Who ingests

One API surface, no privileged client path. The same endpoints serve:

- **The user**, via the SPA's upload interface (drag-and-drop or file
  picker, single or many files at once).
- **Source adapters** (Discovery pillar) running under a service
  identity, pulling documents from external catalogues.
- **External integrations / custom adapters**, authenticating as OAuth
  clients with the `ingester` capability.

Identity is always a JWT; scope is always the tenant claim; every
document is treated as untrusted on arrival regardless of who sent it.

## The two ways a document arrives

1. **File upload (bytes).** The client uploads the raw file. Delphi
   stores the original, extracts its text, derives its metadata, and
   commits the corpus document.
2. **Reference-only (no bytes).** A caller (typically an adapter) hands
   Delphi already-known metadata plus a source URL, with no file body.

Both converge on the same corpus document shape and the same persistence
path.

## Metadata: supplied, then enriched

A document's descriptive metadata (title, authors, summary, language,
publication date, and arbitrary extra fields) comes from up to two
sources, merged with a fixed precedence:

1. **User/caller prefill** — whatever was supplied at upload time. For a
   single file the user may fill a metadata form; for a batch they
   typically supply nothing.
2. **Automated extraction** — Delphi reads the document's text and
   **uses an LLM to derive the missing metadata** (title, authors,
   abstract/summary, language, publication date, and any
   source-specific fields). This is the end-goal differentiator: a user
   can drop a stack of unlabelled PDFs and get a fully-catalogued corpus
   back.

**Precedence is absolute: user prefill always wins.** Extraction only
fills fields the user left blank. A field that neither source provides
is left unset unless it is mandatory.

## Identity and dedup

- Every document has a stable **identity** that is independent of its
  metadata, so it can be referenced before metadata exists.
- Documents that carry a **natural identifier** (a DOI, an arXiv id,
  etc.) are **deduplicated** on it per tenant — re-ingesting the same
  paper updates rather than duplicates. Documents without a natural
  identifier (ad-hoc file uploads) are never deduplicated; each upload
  is its own document.

## Validation and trust

Ingestion is a security boundary. Before a document is committed:

- Declared metadata is bounds-checked (allowed content types, size caps,
  metadata shape/size).
- The uploaded bytes are checked against what was declared (real size,
  magic-byte content-type sniffing, format-parse, polyglot rejection).
- LLM-derived metadata is treated as **untrusted output** and validated
  before it is trusted.
- A document that fails any check is **rejected and deleted**, never
  quarantined in the corpus. The originating client learns the reason
  through a short-lived status channel.

Optional, deployment-specific defences (e.g. antivirus scanning) layer
into the same validation stage without changing the contract.

## Asynchronous, non-blocking experience

Uploading must not trap the user. Once a file (or batch) is handed off:

- The UI is **immediately free** — the user can navigate elsewhere while
  ingestion proceeds.
- Each in-flight document is **tracked** with live state (uploading →
  validating/enriching → ready, or failed-with-reason) in a persistent,
  dismissable surface.
- Failures are surfaced per-document without affecting siblings.

## End-to-end outcome

A successful ingestion yields a corpus document with:

- a stable identity and (when applicable) a dedup key,
- the original artefact stored in object storage,
- extracted full text,
- descriptive metadata (user-supplied and/or LLM-derived),
- and, after enrichment, vector embeddings and knowledge-layer links.

At which point the document is discoverable in the feed, searchable,
and available to RAG chat and document analysis — the entry point to
every other pillar.

## Out of scope (for this spec)

- The retrieval / embedding / knowledge-extraction pipelines that
  *consume* an ingested document — specified with their own pillars.
- The mechanics of source adapters (poll schedules, source contracts).
- Storage-provider and deployment-shape specifics.
