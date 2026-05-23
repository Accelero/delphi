# Chat Title Generation — Dedicated Cheap LLM (design)

Status: **implemented.** Self-contained: everything an implementer needs is
inline below. Companion to [`chat-v4.md`](./chat-v4.md).

**One-liner.** Move first-turn chat-title generation off the chat model
onto a small, local, CPU generation **sidecar** (OpenAI-compatible),
selectable by a `DELPHI_TITLE_*` config block that **defaults to the
sidecar**. Additive backend change: one new factory + one `AppState`
field; no trait or caller churn.

---

## 1. Context (all of it)

### 1.1 The LLM interface is ours, not rig's

The whole codebase talks to LLMs through one hand-rolled trait
(`backend/src/llm/mod.rs`). rig is the *implementation* (per-provider HTTP
+ streaming-format parsing), hidden inside the private `rig_impl` module;
no rig type crosses the `llm` module boundary.

```rust
// backend/src/llm/mod.rs  (the entire public surface)
pub enum Role { System, User, Assistant }
pub struct LlmMessage { pub role: Role, pub content: String }
pub enum LlmDelta { Text(String) }                 // v1: text only
pub type DeltaStream = Pin<Box<dyn Stream<Item = Result<LlmDelta>> + Send>>;

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream>;
}
pub use rig_impl::llm_from_env;
```

Consequence: **a new model is just another `impl LlmClient`** registered
in the factory. The title client adds nothing to the interface.

### 1.2 How titles work today

`backend/src/chat/worker.rs`:

- `generate_title(llm: &dyn LlmClient, user_msg, assistant_msg) -> Option<String>`
  (worker.rs:453): 2-message prompt — system *"You produce concise chat
  titles… 60 characters or less…"* + user `User:…\n\nAssistant:…\n\nTitle:`;
  streams the reply, `clean_title` strips quotes/trims.
- Fired **detached** after `finish` (worker.rs:275–311), only on a
  conversation's first turn (`!conversation_had_title && !assistant_buf.is_empty()`).
  **Best-effort:** any failure is `warn!`-logged; the conversation just
  stays untitled (feed renders `title ?? canonical_id`). On success it
  `rename_conversation`s (durable) then pushes an `sse::title` frame to
  live tabs. **Today it calls `req.llm` — the chat model.** This is the
  one line that changes.

### 1.3 The two construction sites we touch

```rust
// backend/src/api/mod.rs::serve
let llm = llm_from_env().context("constructing llm client")?;   // :68
// ...
let state = AppState {            // :136
    llm,
    // chunk_embedder: embedders.chunk, document_embedder: …, etc.
};
```

```rust
// backend/src/chat/worker.rs::turn_request  (builds TurnRequest from AppState)
TurnRequest {
    // …
    llm: app.llm.clone(),         // :548
    chunk_embedder: app.chunk_embedder.clone(),
    pool: app.request_db_pool.clone(),
    turn_bus: app.turn_bus.clone(),
}
```

`AppState` (state.rs) holds `pub llm: Arc<dyn LlmClient>`; `TurnRequest`
(worker.rs:79) holds `pub llm: Arc<dyn LlmClient>`.

### 1.4 The OpenAI-compatible template already in the tree

`MinimaxLlm` (rig_impl.rs:162–187) already builds a client against an
arbitrary OpenAI-compatible endpoint — exactly what a local sidecar
exposes:

```rust
let client = CompletionsClient::builder()
    .api_key(api_key)
    .base_url(base_url)            // e.g. https://api.minimax.io/v1
    .build()?;
let agent = client.agent(model).preamble("You are delphi…").build();
```

So the title client needs **no new rig integration** — it reuses this
path with a different `base_url`.

### 1.5 The sidecar precedent

Embeddings already run as CPU sidecars in **both** compose tiers
(`tei-chunk` → `BAAI/bge-small-en-v1.5`, `tei-paper` → `allenai/specter2_base`;
HF Text-Embeddings-Inference). The backend reaches them over HTTP via
`DELPHI_EMBEDDER_*_ENDPOINT`; each auto-downloads its model from a
`--model-id` into a named volume; each tier runs its own containers
(distinct names + volumes). The title sidecar copies this shape exactly.

---

## 2. Motivation

Reusing the chat model for titles couples a throwaway summary to the
primary model: an expensive chat model makes every first turn pay an
extra full round-trip on it; offline/local deployments can't title at
all; and chat content is shipped to the cloud for a cosmetic task. A
dedicated local model decouples all three. (Cost alone is *not* the
driver — `gemini-3.5-flash` titles are rounding error; **data-locality,
offline operation, and decoupling** are.)

---

## 3. Decisions (decided, with rationale)

| # | Decision | Choice | Why |
|---|---|---|---|
| 1 | Engine | **llama.cpp server** (`ghcr.io/ggml-org/llama.cpp:server`) | OpenAI-compatible `/v1`; `-hf` auto-downloads the GGUF on boot — the closest analogue to the TEI `--model-id` pattern we already run; lightweight. |
| 2 | Model | **Qwen2.5-0.5B-Instruct, Q4_K_M GGUF** | ~0.4 GB RAM, fast on CPU, ample for a ≤60-char title. |
| 3 | Config namespace | **`DELPHI_TITLE_*`** | Its own category under the [[env-config-convention]] `DELPHI_<CATEGORY>_*` scheme; a title LLM is a distinct concern from the chat `DELPHI_PROVIDER`. |
| 4 | Default-on, incl. T1 | **Yes**, with `DELPHI_TITLE_ENABLED=false` escape hatch | Parity rule (both tiers identical). The flag lets a dev run a lighter T1 (titles fall back to chat model) without deleting the service. |
| 5 | Runtime fallback to chat model on sidecar error | **Deferred** | Keeps v1 minimal; the existing best-effort "no title on failure" path already degrades safely. |

**Alternatives considered & rejected.**
- *In-process inference (candle / llama.cpp bindings in the backend).*
  Rejected: bloats every stateless replica with model weights, contends
  with the Tokio request runtime, and heavies the build — counter to the
  "stateless, scale-ready backend" design. Sidecar keeps inference out of
  process, exactly like embeddings.
- *Ollama as the engine.* Viable (OpenAI-compatible) but the model isn't
  auto-pulled on first request — it needs an `ollama pull` init container
  (à la `minio-init`). llama.cpp's `-hf` is one fewer moving part and
  matches TEI.
- *Reuse the chat model (status quo).* That's exactly what we're moving
  away from, for the §2 reasons.

---

## 4. Config block (`DELPHI_TITLE_*`)

| Var | Default | Meaning |
|---|---|---|
| `DELPHI_TITLE_ENABLED` | `true` | Master switch. `false` ⇒ title generation reuses the chat `llm` (today's behavior); no sidecar needed. |
| `DELPHI_TITLE_PROVIDER` | `openai` | Client family. v1 supports OpenAI-compatible only (covers the sidecar *and* any cloud OpenAI-compatible endpoint via `BASE_URL`); other values error at startup. |
| `DELPHI_TITLE_BASE_URL` | `http://title-llm:80/v1` | Sidecar endpoint (internal compose DNS). |
| `DELPHI_TITLE_MODEL` | `Qwen2.5-0.5B-Instruct` | Model id the server advertises. |
| `DELPHI_TITLE_API_KEY` | `sk-noauth` | Dummy; the local server ignores it. |

**Deliberate exception to the no-hardcoded-defaults rule.** Per
[[env-config-convention]], the *chat* `DELPHI_PROVIDER` / `DELPHI_PROVIDER_MODEL`
are required-with-no-default and fail loudly, because there is no
universally-correct chat model and a silent fallback hides a misconfig.
The title model is the inverse: there **is** a correct default — the
sidecar we bundle in both stacks — and the feature is explicitly
"default to the sidecar." So `DELPHI_TITLE_*` ships working defaults
applied *in the factory* (not via `require_env`). Overriding them
(disable, or point at a cloud/shared endpoint) is the opt-in. Call this
asymmetry out in `.env.example` so it doesn't read as drift.

---

## 5. The sidecar service (both compose files)

Add to **`docker-compose.yml`** (T1) and **`docker-compose.full.yml`**
(T2), mirroring the TEI services (own container name + own model-cache
volume per tier):

```yaml
  title-llm:
    image: ghcr.io/ggml-org/llama.cpp:server
    container_name: delphi-title-llm          # delphi-full-title-llm in T2
    command:
      - "-hf"
      - "Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M"
      - "--host"
      - "0.0.0.0"
      - "--port"
      - "80"
      - "--ctx-size"
      - "4096"
    environment:
      LLAMA_CACHE: /models
    volumes:
      - title_llm_models:/models              # title_llm_models_full in T2
    # T1 only — expose for curl debugging, mirroring tei-chunk:8082/tei-paper:8083
    ports:
      - "8084:80"
```

- Backend gains `depends_on: [title-llm]` (alongside the TEI deps).
- New named volume `title_llm_models` (T1) / `title_llm_models_full` (T2)
  in each file's `volumes:` block.
- T2 does **not** publish the port (internal only), matching how T2 hides
  the TEI containers.

> First boot downloads the GGUF (~0.4 GB) into the volume; subsequent
> boots are cache hits. Same first-run cost as the TEI models.

---

## 6. Backend changes (file by file)

All additive. Estimated diff: ~1 new factory fn, ~1 generalized struct,
3 one-line wiring edits.

### 6.1 `llm` module — generalize the OpenAI-compat impl + add the factory

In `rig_impl.rs`, generalize `MinimaxLlm` into a reusable
`OpenAiCompatLlm { agent: Agent<OpenAiChatCompletionModel> }` constructed
from `(model, api_key, base_url)`; `MinimaxLlm::from_env` becomes a thin
caller (preserves its `DELPHI_PROVIDER_MINIMAX_*` env). Then add the
title factory:

```rust
/// Title-generation client. Defaults to the local sidecar; returns the
/// chat client unchanged when DELPHI_TITLE_ENABLED=false. Defaults are
/// applied HERE (not require_env) — see title-llm.md §4.
pub fn title_llm_from_env(chat_llm: &Arc<dyn LlmClient>) -> Result<Arc<dyn LlmClient>> {
    if !env_flag("DELPHI_TITLE_ENABLED", true) {
        return Ok(chat_llm.clone());              // reuse chat model
    }
    match env_or("DELPHI_TITLE_PROVIDER", "openai").as_str() {
        "openai" => {
            let base_url = env_or("DELPHI_TITLE_BASE_URL", "http://title-llm:80/v1");
            let model    = env_or("DELPHI_TITLE_MODEL", "Qwen2.5-0.5B-Instruct");
            let api_key  = env_or("DELPHI_TITLE_API_KEY", "sk-noauth");
            Ok(Arc::new(OpenAiCompatLlm::new(&model, api_key, base_url)?))
        }
        other => Err(Error::UnknownBackend(format!("DELPHI_TITLE_PROVIDER={other}"))),
    }
}
```

(`env_flag`/`env_or` are tiny local helpers — defaulting reads, the
mirror of the existing `require_env`.) Re-export `title_llm_from_env`
from `llm/mod.rs`.

### 6.2 `state.rs` — new field

```rust
pub struct AppState {
    pub llm: Arc<dyn LlmClient>,
    /// Cheap, usually-local client for first-turn title generation.
    /// Defaults to the title sidecar; equals `llm` when titles are
    /// configured to reuse the chat model. See title-llm.md.
    pub title_llm: Arc<dyn LlmClient>,
    // …unchanged…
}
```

### 6.3 `api/mod.rs::serve` — build it next to `llm`

```rust
let llm = llm_from_env().context("constructing llm client")?;
let title_llm = title_llm_from_env(&llm).context("constructing title llm client")?;
// …
let state = AppState { llm, title_llm, /* … */ };
```

### 6.4 `chat/worker.rs` — thread it and use it

- `TurnRequest`: add `pub title_llm: Arc<dyn LlmClient>`.
- `turn_request(...)`: add `title_llm: app.title_llm.clone()`.
- Detached title task (worker.rs:286): `let llm = req.title_llm.clone();`
  (one-token change — `req.llm` → `req.title_llm`). `generate_title`'s
  body is untouched.

---

## 7. Failure & fallback semantics

- **Sidecar down/unreachable** ⇒ `generate_title` returns `None` (logged)
  ⇒ conversation stays untitled. Identical to today's degradation, just a
  different client. The chat turn itself is **never** affected (title runs
  detached, after `finish`).
- **Bare `cargo run` (no docker)** ⇒ no sidecar ⇒ titles silently
  skipped. Documented; set `DELPHI_TITLE_ENABLED=false` to fall back to
  the chat model in that setup.
- **Runtime "retry on chat model" fallback** — deferred (decision 5).

---

## 8. Testing

- **Unit (`tests/common/fake_llm.rs`):** the existing fake `LlmClient`
  scripted to return a canned title; assert `clean_title` handling and
  that the result reaches `rename_conversation`. No sidecar needed —
  inject the fake as `title_llm`.
- **Disabled path:** with `DELPHI_TITLE_ENABLED=false`, assert
  `title_llm_from_env` returns the same `Arc` as the chat client
  (`Arc::ptr_eq`).
- **Default path:** assert defaults resolve to the sidecar base URL/model
  when env is unset.
- **e2e (manual, T1 first):** start T1, send a first message, confirm the
  sidecar served the title (llama.cpp logs a `/v1/chat/completions` hit)
  and the sidebar updates live via the `sse::title` frame.
- No new provider streaming path ⇒ the `api/sse.rs` snapshot tests are
  unaffected.

---

## 9. Rollout / verification

1. Add the sidecar to both compose files + the two named volumes.
2. Land the backend changes; `cargo check` (both feature configs) +
   `cargo test`.
3. `make up` (T1): confirm `title-llm` pulls the model and goes healthy;
   `curl localhost:8084/v1/models`; send a chat first turn; verify title.
4. `.env.example`: document the `DELPHI_TITLE_*` block and the
   default-exception note.
5. Later, when validating prod-shape: rebuild T2 (sidecar internal-only).

---

## 10. Future / generalization

The "secondary `LlmClient` behind the same trait, pointed at a local
sidecar" pattern generalizes. The ingestion **`LlmExtractor`**
([ingestion-roadmap.md](./ingestion-roadmap.md) §1, metadata autofill) is
another non-chat LLM consumer that could share this sidecar instead of
burning the chat model. If a second consumer lands, promote `title_llm`
to a small **utility-LLM registry** (a keyed map on `AppState`) rather
than one bespoke field per feature. Also note: if we ever standardize on
OpenAI-compatible endpoints everywhere (cloud flash + local sidecars all
speak it), the `OpenAiCompatLlm` wrapper is the seam that would let us
drop rig for a single small `reqwest` SSE client with zero caller change.

---

## 11. File-change checklist

- [x] `docker-compose.yml` — `title-llm` service + `title_llm_models` volume + backend `depends_on`
- [x] `docker-compose.full.yml` — same, `-full` names, port not published
- [x] `backend/src/llm/rig_impl.rs` — generalize `MinimaxLlm` → `OpenAiCompatLlm`; add `title_llm_from_env` + `env_flag`/`env_or`
- [x] `backend/src/llm/mod.rs` — re-export `title_llm_from_env`
- [x] `backend/src/state.rs` — `AppState.title_llm`
- [x] `backend/src/api/mod.rs` — build `title_llm`, add to `AppState`
- [x] `backend/src/chat/worker.rs` — `TurnRequest.title_llm`, set in `turn_request`, use in detached title task
- [x] `.env.example` — `DELPHI_TITLE_*` block + default-exception note
- [x] tests — disabled-path `Arc::ptr_eq` + `env_or`/`env_flag` helpers (the
      test harness shares one fake across `llm`/`title_llm`, so the existing
      chat integration tests also exercise the detached title path)
