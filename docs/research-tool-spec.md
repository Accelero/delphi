# Personal Research Pipeline — System Specification

## 1. Purpose

A personal research tool that automates discovery of new academic papers (primarily from arXiv), ranks them against ongoing research projects, maintains a durable knowledge base of papers and notes, and exposes everything to LLMs for interactive reading and synthesis.

The user is an active ML researcher tracking multiple projects, each with evolving research questions and knowledge gaps. The system runs on a single user's machine (with optional sync across devices) and is not intended to be multi-tenant.

## 2. Goals and non-goals

### Goals

- Daily ingestion of new arXiv papers in user-specified categories.
- Per-project, per-research-question ranking of new and existing papers.
- A durable knowledge base of papers (metadata + PDFs) and user-authored notes, both kept in plain, portable formats.
- Single-source-of-truth for paper identity, with all systems referencing papers by stable identifier.
- LLM-accessible reading and writing across the knowledge base via MCP or equivalent.
- Detection of new versions of papers already in the library, with surfaced summaries of meaningful changes.
- Interactive paper-reading workflow that supports building intuition fast.

### Non-goals

- Multi-user or team features.
- Replacing a reference manager's citation export — Zotero handles citation styles and exports.
- A polished consumer UI. Email digest, CLI, and existing apps (Obsidian, Zotero) are the primary surfaces.
- Real-time alerts (within minutes of arXiv publication). Daily digest is sufficient.
- Coverage beyond arXiv and Semantic Scholar's index (no scraping of paywalled publishers).

## 3. Architectural overview

### 3.1 Layers

The system has four logical layers, each with one responsibility:

**Data layer** — durable storage of papers, metadata, embeddings, and notes. Three stores:

- *Paper store*: Zotero (SQLite + linked PDF attachments) for papers the user has actively engaged with.
- *Note store*: Obsidian vault (markdown files on disk) for user-authored notes and synthesis.
- *Index store*: LanceDB (single-file embedded vector database) for embeddings and metadata of all known papers, including those not yet in Zotero.

**Pipeline layer** — scheduled jobs that ingest, embed, score, and detect updates. Implemented as Python scripts run via cron.

**Interface layer** — how the user interacts with the system: daily email digest, Obsidian for note-taking, Zotero for reference management, an LLM (Claude) with MCP servers for chat-with-corpus.

**Resolver layer** — a single small library that maps from paper identifier to file path or URL, used by all other layers.

### 3.2 Identifier strategy

Every paper has one canonical identifier:

- arXiv papers: arXiv ID without version suffix (e.g., `2301.04104`).
- Non-arXiv papers with a DOI: the DOI string.
- Other papers: a Zotero-generated item key (fallback only).

Versions are tracked separately. A paper has an identifier; each version has an identifier + version number.

All systems reference papers by identifier. Paths are an implementation detail handled by the resolver.

### 3.3 PDF storage

PDFs live in a single flat directory: `~/research/papers/`.

Filenames embed identifier and version: `{arxiv_id}v{N}.pdf` (e.g., `2301.04104v3.pdf`).

A symlink `{arxiv_id}.pdf` points to the latest local version, for convenience.

Zotero references PDFs via linked attachments pointing at this directory (not Zotero's managed `storage/` folder).

The user does not download every paper. PDFs are fetched only when the user actively engages with a paper (opens it, annotates it, adds it to Zotero). The discovery pipeline holds metadata + abstract + embedding only.

## 4. Data model

### 4.1 Index store (LanceDB)

Single table `papers`:

| Column            | Type      | Notes                                              |
| ----------------- | --------- | -------------------------------------------------- |
| arxiv_id          | string    | Primary key (or DOI for non-arXiv).                |
| version           | int       | Latest known version on arXiv.                     |
| title             | string    |                                                    |
| authors           | list[str] |                                                    |
| abstract          | string    |                                                    |
| categories        | list[str] | arXiv categories, e.g., `cs.LG`.                   |
| published_at      | timestamp | v1 publication date.                               |
| updated_at        | timestamp | Latest revision date from arXiv.                   |
| embedding         | vector    | SPECTER2 embedding of title + abstract (768-dim).  |
| has_local_pdf     | bool      | True if a PDF exists in `~/research/papers/`.      |
| in_zotero         | bool      | True if added to Zotero (i.e., user engaged with). |
| ingested_at       | timestamp | When this row was created.                         |
| last_checked_at   | timestamp | Last time we queried arXiv for updates.            |

Secondary table `versions` (append-only history):

| Column         | Type      |
| -------------- | --------- |
| arxiv_id       | string    |
| version        | int       |
| abstract       | string    |
| updated_at     | timestamp |
| recorded_at    | timestamp |

### 4.2 Project store

Projects and research questions live as YAML files in `~/research/projects/`. One file per project:

```yaml
# ~/research/projects/world-models.yaml
id: world-models
name: Sample-efficient world models for long-horizon planning
status: active
created: 2026-04-01

questions:
  - id: partial-observability
    text: How do current world models handle partial observability?
    seed_papers: [2301.04104, 2310.06770]
    notes: |
      Particularly interested in approaches that don't require explicit
      belief-state estimation.

  - id: pixel-vs-latent
    text: Pixel-space vs latent-space rollouts — when does each scale better?
    ruled_out: false
    notes: |
      Currently leaning latent, but want to be challenged.

categories:
  - cs.LG
  - cs.AI
  - cs.RO

excluded_terms:
  - LLM agent
  - prompt engineering
```

The `seed_papers` list bootstraps the question's relevance signal — their embeddings are averaged into a question centroid for similarity scoring.

### 4.3 Note store (Obsidian)

Each paper note is a markdown file with frontmatter:

```markdown
---
arxiv_id: 2301.04104
version: 3
title: DreamerV3
authors: [Hafner, ...]
read_at: 2026-04-15
projects: [world-models]
questions: [partial-observability]
status: read
---

# Summary
...

# Claims
- ...

# Evidence
- ...

# Limitations
- ...

# Relation to my project
- ...

# Follow-ups
- ...
```

Atomic concept notes (not tied to a single paper) follow the same vault but without the paper-specific frontmatter.

The vault is indexed alongside papers in LanceDB, so "search my notes" and "search arXiv" use the same machinery.

## 5. Pipelines

### 5.1 Daily ingestion pipeline

Scheduled: daily, e.g., 6am local time.

Steps:

1. Query arXiv API for papers in the user's tracked categories submitted in the last 48 hours (overlap by one day to handle cron failures).
2. For each new paper not already in the index store, insert a row with metadata and a placeholder embedding.
3. Embed abstracts (initially with a local model like SPECTER or a sentence-transformer; swap to Semantic Scholar's SPECTER2 vectors when available, accepting the 1-3 day lag).
4. For each active project, score new papers against each research question:
   - Compute cosine similarity between paper embedding and question centroid.
   - Top 200 candidates per question proceed to LLM rerank.
5. Cross-encoder rerank (optional, if quality demands it) reduces 200 to top 50.
6. Per-question LLM scoring: for each (paper, question) pair in the top 50, an LLM call returns `{score: 0-5, justification: "..."}`. Cheap models acceptable.
7. Persist scores in a `scores` table keyed by `(arxiv_id, project_id, question_id, scored_at)`.
8. Generate digest (see 5.2).

### 5.2 Daily digest

Output format: markdown email or file in `~/research/digests/{date}.md`.

Structure:

- Per project, per question: top N papers with title, authors, one-line LLM justification, link to arXiv.
- "New versions" section: papers in Zotero with newer versions on arXiv, with LLM-generated diff summary.
- "Tangential" section: papers that scored highly on multiple questions or against general project centroid but not on a specific question.

### 5.3 Update detection pipeline

Scheduled: weekly.

Steps:

1. Query arXiv for the latest version of every arXiv ID in Zotero (i.e., papers the user has engaged with).
2. For each paper where the latest version is greater than the local version:
   - Download the new PDF to `~/research/papers/{arxiv_id}v{N}.pdf`.
   - Update the symlink `{arxiv_id}.pdf` to point at the new file.
   - Append a row to the `versions` table with the new abstract.
   - Run an LLM diff between the previous and new abstracts.
   - Re-embed and update the index store.
3. Generate an "updates" section in the next daily digest with the diffs.

### 5.4 Backfill / on-demand pipelines

- **Add to library**: when the user moves a paper into Zotero, the pipeline downloads the PDF (current version), creates a stub Obsidian note from a template, and updates `in_zotero = true` and `has_local_pdf = true` in the index.
- **Re-embed**: when the embedding model is upgraded, a script re-embeds all papers in the index store.
- **Citation expansion**: on demand, given a seed paper, query Semantic Scholar for citations and references, and ingest the metadata of related papers into the index store (without PDFs).

## 6. Interfaces

### 6.1 Resolver library

A small Python module exposing:

```python
def resolve_path(arxiv_id: str, version: int | None = None) -> Path | str:
    """Return local path if available, arXiv URL otherwise."""

def has_local(arxiv_id: str, version: int | None = None) -> bool:
    """Whether a local PDF exists."""

def fetch(arxiv_id: str, version: int | None = None) -> Path:
    """Download the PDF if not already local; return local path."""
```

All other components (pipeline scripts, MCP servers, etc.) call this rather than computing paths.

### 6.2 LLM access via MCP

Two MCP servers exposed to Claude:

**Knowledge server** — read/write access to the unified corpus:

- `search_papers(query, project_id?, question_id?, top_k=20) -> [paper_ref]`
- `get_paper(arxiv_id, version?) -> {metadata, abstract, sections?}`
- `get_pdf(arxiv_id, version?) -> path` (LLM can then read the file)
- `list_projects() -> [project]`
- `list_questions(project_id) -> [question]`
- `search_notes(query, top_k=20) -> [note_ref]`
- `get_note(path) -> markdown`
- `create_note(path, frontmatter, body) -> note_ref`
- `append_to_note(path, body) -> note_ref`

**Pipeline server** — trigger and inspect pipelines:

- `add_to_library(arxiv_id) -> {status}`
- `score_paper(arxiv_id, project_id) -> {score, justifications}`
- `latest_digest() -> markdown`
- `check_updates() -> [update_summary]`

These are deliberately small surfaces; the LLM does the rest with markdown reads/writes.

### 6.3 Daily digest

Plain markdown file, optionally emailed. The user reads this in their normal workflow (email client, markdown viewer, or Obsidian if dropped in the vault).

### 6.4 Direct interaction

- **Zotero**: the user adds papers, annotates PDFs, organizes collections as normal. Pipelines watch Zotero's database (via its API or by polling the SQLite file) for changes.
- **Obsidian**: the user writes notes as normal. The vault is a folder of markdown files; no special integration needed beyond optional plugins.
- **CLI** (optional): a small `research` command for manual operations (`research add 2301.04104`, `research score`, `research check-updates`).

## 7. Component choices

### 7.1 Required

- **Python 3.11+** for pipeline code.
- **LanceDB** for the index store. Single-file, embedded, vectors + metadata.
- **Zotero 7** with linked attachments, configured to point at `~/research/papers/`.
- **Obsidian** for the note vault.
- **arXiv API** (`arxiv` Python package wraps it) for ingestion and update checks.
- **Semantic Scholar API** for SPECTER2 embeddings, citation graph, recommendations.

### 7.2 Replaceable

- **Embedding model**: SPECTER2 via Semantic Scholar API is the default. Local fallback: `allenai/specter2` via `sentence-transformers`. Swappable behind a single function.
- **LLM for scoring**: any model accessible via API. Cheap models (Haiku-tier) for per-paper scoring, stronger models for diff generation and chat.
- **Cross-encoder reranker**: optional, not in v1. Add if retrieval quality demands.
- **Sync mechanism**: Syncthing default for `~/research/`. Zotero's own sync for the metadata DB.

### 7.3 Explicitly avoided

- Heavy retrieval frameworks (Haystack, LlamaIndex). The pipeline is small enough that direct code is clearer.
- Closed-system knowledge bases (Recall, Heptabase). Ownership and scriptability matter more than polish.
- Per-system path storage. Only the resolver knows about paths.

## 8. Operational considerations

### 8.1 Failure modes

- **arXiv API rate-limited or down**: pipeline retries with backoff; failed days are picked up by the 48-hour overlap window.
- **Embedding API down**: queue papers for embedding; pipeline continues with metadata-only ingestion.
- **PDF fetch fails**: index entry marks `has_local_pdf = false`; resolver falls back to URL.
- **Disk full**: papers folder bounded by the user's engagement (only Zotero papers are stored locally). Periodic cleanup script (see 8.3).
- **Corrupted index**: rebuild from Zotero + arXiv re-fetch. The index is derived; the sources of truth (Zotero, Obsidian, arXiv) are not.

### 8.2 Sync across devices

- `~/research/` (papers, projects, vault, digests) syncs via Syncthing.
- Zotero metadata syncs via Zotero's built-in sync.
- LanceDB index is rebuildable; sync optional. If syncing, ensure no concurrent writes (run pipeline on one machine only).

### 8.3 Maintenance

- Monthly: review papers in the index store with `in_zotero = false` and no recent scoring activity. Optionally archive or drop.
- Monthly: review old versions of papers in `~/research/papers/`. Keep versioned files for papers with notes attached; consider pruning unused v1s when v2+ exists.
- On embedding model upgrade: re-embed all papers (one-time batch job, ~minutes for 100k papers).

### 8.4 Privacy

The system runs locally. Paper metadata and abstracts are sent to:

- arXiv (when fetching new papers).
- Semantic Scholar (when fetching SPECTER2 embeddings or citations).
- Whichever LLM provider is configured (when scoring or chatting).

User-authored notes are not sent anywhere unless the user explicitly invokes a feature that involves them (e.g., chat with corpus, which sends relevant note chunks to the LLM provider).

## 9. Build phases

### Phase 1 — Foundations (smallest viable system)

Goal: ingest, embed, search.

- Resolver library.
- LanceDB index with the schema in 4.1.
- Daily arXiv ingestion script (categories from a config file).
- Local embedding model (SPECTER2 via sentence-transformers).
- Simple CLI: `research search <query>` returns ranked papers.

Output: a queryable corpus of arXiv papers in your areas, no projects yet.

### Phase 2 — Projects and digests

Goal: per-project relevance scoring and daily output.

- Project YAML schema and loader.
- Per-question centroid scoring.
- LLM-based per-paper-per-question scoring on top 50.
- Daily digest generation as markdown.

Output: a daily digest you actually read.

### Phase 3 — Engagement and notes

Goal: tie Zotero, Obsidian, and the index together.

- Zotero integration: detect added papers, download PDFs, update index.
- Obsidian note template; auto-generate stubs for new Zotero entries.
- Index notes alongside papers.

Output: durable knowledge base with paper-note linking.

### Phase 4 — Updates and LLM access

Goal: keep things fresh and make the corpus chat-accessible.

- Weekly update detection pipeline.
- LLM-generated diffs for new versions.
- Knowledge MCP server.
- Pipeline MCP server.

Output: a corpus you can chat with via Claude, that stays current.

### Phase 5 — Polish

- Cross-encoder reranking if quality is poor.
- Citation expansion via Semantic Scholar.
- Bandit-style exploration in recommendations.
- Better digest formatting.
- Optional web UI for the rate-papers loop.

## 10. Open questions

These are decisions worth deferring until there's data:

- Embedding choice in steady state: SPECTER2 vs. a more recent scientific embedding model. Re-evaluate every ~6 months.
- Whether to maintain per-question classifiers (logistic regression on positives/negatives) on top of centroid scoring. Add if centroid alone produces too much noise.
- How aggressively to prune the index store. The instinct is "never delete, disk is cheap" but there's a quality-of-search argument for filtering out clearly-irrelevant papers ingested early.
- Where the chat interface lives in steady state. Claude with MCP servers is the v1 answer; if a better interface emerges (or a custom one becomes worth building), revisit.
- Whether the per-paper LLM scoring should be replaced by a fine-tuned classifier once enough labeled data accumulates.

## 11. Success criteria

The system is working if, after three months of use:

- The user reads more papers per week, with higher hit rate of "this was actually relevant," than before.
- The user maintains notes for papers they read, without significant friction.
- New versions of important papers are noticed within a week of release.
- The user can ask "what does my corpus say about X" and get a useful answer with citations to specific papers and notes.
- The user has not had to manually move files, fix broken paths, or reconcile mismatched identifiers.

The system is failing if:

- The daily digest is consistently ignored.
- The user maintains notes outside the system (in some other tool) because the integration is too clunky.
- Search returns mostly noise or mostly the same few papers.
- Sync breaks or corrupts data more than once a quarter.
