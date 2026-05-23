# LLM Metadata Autofill — the `LlmExtractor` (design)

Status: **implemented.** Realises [ingestion-roadmap.md §1](./ingestion-roadmap.md).
Companion to [`title-llm.md`](./title-llm.md) — second consumer of the
non-chat "utility LLM" pattern.

**One-liner.** Replace the `NoopExtractor` with an `LlmExtractor` that reads
the extracted document head + the user's prefill and returns structured
`ExtractedMetadata` (title, authors, summary, language, published_at, +
`extra` for venue/DOI). The model is the **chat model by default**, but
configurable to any OpenAI-compatible endpoint (cloud or a local sidecar)
via `DELPHI_EXTRACT_*`. The user's manual metadata always wins.

---

## 1. What already exists (we only add the impl)

The seam, the pipeline stage, and the merge are all built and tested
(`ingestion/autofill.rs`, `ingestion/completion.rs`):

- **Trait.** `MetadataExtractor::extract(&ExtractionContext) -> Result<ExtractedMetadata>`.
  `ExtractionContext { text: &str, prefill: &DocumentPrefill }`.
- **Output shape.** `ExtractedMetadata { title: Option<String>, authors:
  Vec<String>, summary: Option<String>, language: Option<String>,
  published_at: Option<DateTime<Utc>>, extra: serde_json::Value }`.
- **Pipeline (completion.rs).** Stage 6 calls `extractor.extract(...)`;
  stage 7 **validates the untrusted output** (`validate_descriptive_metadata`)
  and drops it on failure; stage 8 `merge_metadata` applies **prefill-wins**.
  All three already exist — stage 6 is `.unwrap_or_default()`, so an
  extractor `Err` is non-fatal.
- **Merge ("manual wins").** `merge_metadata` (autofill.rs:85): per field
  `prefill.or(autofill)`; `authors` = prefill-if-non-empty; `extra` =
  shallow object merge, prefill keys win. **Already the requested policy.**
- **Wiring point.** The `/complete` handler reads `state.metadata_extractor`
  (`uploads.rs:525`). Swapping `NoopExtractor` for `LlmExtractor` in
  `api/mod.rs::serve` is the only call-site change.

So this change is: one new `impl MetadataExtractor` + one LLM factory + one
`AppState` field + the `api/mod.rs` swap.

## 2. Decisions

| # | Decision | Choice | Why |
|---|---|---|---|
| 1 | Model | **Chat model by default, configurable** | Extraction runs once per upload (not per token) → cost is rounding error even on a flagship chat model; it's already configured and capable. `DELPHI_EXTRACT_BASE_URL` overrides to any OpenAI-compatible endpoint (a 3B–7B local sidecar for offline/data-local, or a different cloud model). |
| 2 | **Not** the 0.5B title model | Excluded | Title gen is forgiving (any plausible string); metadata is structured + quality-critical (a hallucinated author/date passes validation and becomes canonical, feeding dedup + future canonical_id promotion). 0.5B is unreliable at multi-field JSON. |
| 3 | Structured output | **Prompt-for-JSON + defensive parse** | The `LlmClient` trait exposes only `stream_chat → text deltas`; no `response_format`/JSON-mode hook. We accumulate the stream, strip code fences, slice the outermost `{…}`, and `serde_json` into an all-`Option` wire struct. Exposing JSON mode on the trait is a separate enhancement. |
| 4 | Input bound | **head only, char-capped** (`DELPHI_EXTRACT_MAX_INPUT_CHARS`, default 12000 ≈ ~3k tokens) | Title/authors/abstract/venue/date/DOI live on page 1. Feeding the head is enough and keeps any model (incl. the 4096-ctx sidecar) in budget. |
| 5 | Failure mode | **graceful empty** | LLM error / timeout / unparseable JSON ⇒ `warn!` + `ExtractedMetadata::default()`. The upload already committed; autofill is best-effort (matches stages 5–7's degrade-don't-lose contract). |
| 6 | `AppState` shape | **bespoke `metadata_llm` field** | Next to `llm` / `title_llm`. Promote all three to a keyed utility-LLM registry only if a 4th consumer appears (title-llm.md §10). |

## 3. Config block (`DELPHI_EXTRACT_*`)

| Var | Default | Meaning |
|---|---|---|
| `DELPHI_EXTRACT_ENABLED` | `true` | `false` ⇒ keep `NoopExtractor` (autofill off; prefill-only). |
| `DELPHI_EXTRACT_BASE_URL` | *(unset)* | **Unset ⇒ reuse the chat model.** Set ⇒ build an OpenAI-compatible client against this endpoint (cloud or local sidecar). |
| `DELPHI_EXTRACT_MODEL` | *(req. iff BASE_URL set)* | Model id for the override endpoint. |
| `DELPHI_EXTRACT_API_KEY` | `sk-noauth` | Key for the override endpoint (sidecars ignore it). |
| `DELPHI_EXTRACT_MAX_INPUT_CHARS` | `12000` | Head of the extracted text fed to the model. |
| `DELPHI_EXTRACT_TIMEOUT_SECS` | `30` | Hard bound on the extraction call. |

Same defaulting posture as [`title-llm.md`](./title-llm.md) §4: the default
(reuse the configured chat model) is correct, so this block applies defaults
in the factory rather than failing closed like `DELPHI_PROVIDER`.

## 4. The extractor (`ingestion/llm_extractor.rs`)

```rust
pub struct LlmExtractor { llm: Arc<dyn LlmClient>, max_input_chars: usize, timeout: Duration }
```

`extract`:
1. Truncate `ctx.text` to `max_input_chars` (char boundary safe).
2. Build `[System(prompt), User(head)]`. Prompt: *"Extract bibliographic
   metadata from the opening text… respond with ONLY one JSON object with
   keys title/authors/summary/language/published_at/venue/doi… extract only
   what is explicitly present, never guess, use null / [] when unknown."*
3. `tokio::time::timeout(timeout, accumulate(stream_chat(messages)))`.
4. Parse: strip ```` ```json ```` fences, slice first `{` … last `}`,
   `serde_json::from_str` into a wire struct (all `Option`, `authors`
   defaulted). `published_at` parsed flexibly (`YYYY-MM-DD`, year-only,
   RFC3339) → `DateTime<Utc>` or `None`. `venue`/`doi` (non-null) →
   `extra` object.
5. Any step fails ⇒ `warn!` + `ExtractedMetadata::default()`.

Empty input text short-circuits to default (no LLM call) — relevant while
stage 4's sniff is bypassed (see §6).

### LLM factory (`llm` module)

`extractor_llm_from_env(&chat_llm) -> Result<Arc<dyn LlmClient>>`: reuses
`OpenAiCompatLlm` + the `env_or`/`require_env` helpers added for the title
client. Unset `DELPHI_EXTRACT_BASE_URL` ⇒ `Ok(chat_llm.clone())`.

## 5. Wiring (`state.rs`, `api/mod.rs`)

- `AppState.metadata_llm: Arc<dyn LlmClient>` (bespoke field).
- `serve`: build `metadata_llm = extractor_llm_from_env(&llm)?`; if
  `DELPHI_EXTRACT_ENABLED` ⇒ `metadata_extractor = Arc::new(LlmExtractor::from_env(metadata_llm.clone()))`,
  else `NoopExtractor`.
- Test harness shares one fake across `llm`/`title_llm`/`metadata_llm`.

## 6. Dependency on the validator re-enable (§0)

`run_completion` stage 4 is **bypassed** today (`completion.rs:75`): the
sniffed content-type is hard-coded `application/octet-stream`, so stage 5
`extract_text` returns `NotImplemented` → **empty text** → `LlmExtractor`
short-circuits to empty. **The extractor therefore produces nothing on real
uploads until [ingestion-roadmap.md §0](./ingestion-roadmap.md) restores the
stage-4 sniff** (real `application/pdf` ⇒ real text). This is the planned
next step ("validators next"); the extractor lands dormant-but-correct and
is unit-tested against the fake LLM in the meantime.

## 7. Testing

- Fake `LlmClient` scripted to emit a JSON object ⇒ assert it maps onto
  `ExtractedMetadata` (incl. `published_at` parse and `extra` venue/doi).
- Fake emitting fenced ```` ```json ```` / surrounding prose ⇒ defensive
  parse still recovers the object.
- Fake emitting garbage ⇒ `extract` returns default (graceful), no panic.
- Empty `ctx.text` ⇒ default, no LLM call.
- Prefill-wins is already covered in `autofill.rs::tests`.

## 8. File-change checklist

- [x] `backend/src/ingestion/llm_extractor.rs` — `LlmExtractor` + `from_env` + tests
- [x] `backend/src/ingestion/mod.rs` — re-export `LlmExtractor`
- [x] `backend/src/llm/{rig_impl,mod}.rs` — `extractor_llm_from_env`
- [x] `backend/src/state.rs` — `AppState.metadata_llm`
- [x] `backend/src/api/mod.rs` — build `metadata_llm` + `LlmExtractor`, swap `metadata_extractor`
- [x] `backend/tests/common/mod.rs` — share fake across the three LLM fields
- [x] `docker-compose.yml` / `docker-compose.full.yml` — `DELPHI_EXTRACT_*` passthrough
- [x] `.env.example` — `DELPHI_EXTRACT_*` block
