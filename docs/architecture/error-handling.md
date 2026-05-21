# Delphi — Error Handling (design, not yet implemented)

Status: **planned**. Sister doc to [`ARCH.md`](../ARCH.md). Describes a
single, consistent error contract spanning the Rust backend and the React
SPA. Nothing here is wired yet — this is the spec we implement in a
follow-up. Tracked as a deferred item; see "Rollout" below.

## Motivation

A user reported "uploading a `.txt` just fails — no detail." The root
cause was a real bug (the content-type allowlist did an exact-string
match and rejected `text/plain; charset=utf-8`; fixed by normalizing the
type before the allowlist check), but the *experience* was the deeper
problem: the backend returned a structured
`400 {"error":"DisallowedContentType"}`, and every layer above threw the
detail away —

- `api.ts::request()` collapsed it to a generic `ApiError(status, statusText)`;
- `UploadManager.onCreate` caught it and mapped everything to the opaque
  code `create_failed`;
- the upload tracker rendered a fixed string, *"The file failed to
  upload."*

The user could not tell *what* went wrong, *why*, or *what to do about
it*, and a developer reading a bug report had no code to grep for. This
doc fixes the class, not the instance: **every error gets a stable code,
a human-readable description, and (where applicable) a hint on how to
avoid it — produced once at the backend and carried intact to the UI.**

## Goals

- **One envelope, everywhere.** Every non-2xx JSON response from the
  backend has the same shape. The SPA has exactly one place that parses
  errors.
- **Three audiences, one object.** A `code` for developers/support, a
  `message` for the end user (what happened), a `hint` for the end user
  (how to avoid it next time).
- **Stable, greppable codes.** `INGEST_CONTENT_TYPE_UNSUPPORTED`, not a
  free-text string that drifts.
- **Safe by construction.** Internal diagnostics (SurrealDB errors,
  sniffed bytes, stack context) are *logged*, never echoed to the client.
- **Correlation.** A `trace_id` ties the user-visible error to the server
  log line that has the full detail.
- **Incremental.** Drop-in per module; no big-bang rewrite. The first
  module (ingestion) proves the pattern.

## The wire envelope

A single JSON object under the top-level `error` key, modelled on
[RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457) but
trimmed to what the SPA needs. `Content-Type: application/problem+json`.

```jsonc
{
  "error": {
    "code": "INGEST_CONTENT_TYPE_UNSUPPORTED",  // stable, SCREAMING_SNAKE, namespaced
    "message": "This file type isn't supported.", // user-facing: what happened
    "hint": "Upload a PDF, plain-text, or Markdown file.", // user-facing: how to avoid (optional)
    "trace_id": "01J9Z3K8M2Qwe…",              // correlation id; also in server logs
    "fields": { "content_type": "application/zip" } // optional machine context (no secrets)
  }
}
```

Rules:

- `code` and `message` are **required**; `hint`, `trace_id`, `fields` are
  optional.
- `message` and `hint` are end-user safe — no internal identifiers, no
  attacker-controlled echoes (e.g. never inline sniffed file bytes).
- `fields` carries small, safe, structured context the UI may use (e.g.
  the offending value to highlight a form field). Never secrets, never
  raw upstream error text.
- The HTTP status still matters (401 drives the auth redirect, 409 drives
  refetch, etc.); `code` refines it.

## Code taxonomy

`<DOMAIN>_<CONDITION>`. Domains map to the API pillars plus cross-cutting
buckets:

| Prefix | Domain |
|---|---|
| `AUTH_` | identity / session / role |
| `INGEST_` | upload + document ingestion |
| `CORPUS_` | documents, chunks, view-urls |
| `CHAT_` | conversations, streaming, stop |
| `DISCOVERY_` | feed, sources, filters |
| `STORAGE_` | object store / DB transport (mapped to generic user text) |
| `VALIDATION_` | generic request-shape failures |
| `INTERNAL_` | catch-all 5xx (message is always generic) |

Codes live in **one registry** so they can't drift or collide — a Rust
enum (below) is the source of truth, and a generated table is published
for support. Examples for the surface that motivated this doc:

| Code | HTTP | message | hint |
|---|---|---|---|
| `INGEST_CONTENT_TYPE_UNSUPPORTED` | 400 | This file type isn't supported. | Upload a PDF, plain-text, or Markdown file. |
| `INGEST_FILE_TOO_LARGE` | 400 | This file is too large to upload. | The limit is 200 MB. |
| `INGEST_FILE_CONTENT_MISMATCH` | 422 | The file's contents don't match its type. | Re-export the original and upload that. |
| `INGEST_FILE_CORRUPT_PDF` | 422 | This PDF couldn't be read. | Try re-saving or re-exporting the PDF. |
| `INGEST_DUPLICATE` | 422 | This document is already in your library. | Open the existing copy instead. |
| `INGEST_ROLE_REQUIRED` | 403 | You don't have permission to upload. | Ask an administrator for the *ingester* role. |
| `INGEST_UPLOAD_EXPIRED` | 410 | This upload timed out. | Start the upload again. |
| `STORAGE_UNAVAILABLE` | 502 | Storage is temporarily unavailable. | Wait a moment and try again. |
| `INTERNAL_ERROR` | 500 | Something went wrong on our end. | Try again; if it persists, contact support with the code below. |

## Backend design

### One type, one conversion

```rust
// backend/src/error/api.rs  (new module)

/// A fully-formed, client-safe API error. Constructed only from the code
/// registry; never by stringly-typed call sites.
pub struct ApiError {
    pub code: ErrorCode,            // enum — the registry
    pub status: StatusCode,         // derived from code by default; overridable
    pub message: String,            // user-facing
    pub hint: Option<String>,       // user-facing
    pub fields: Option<serde_json::Value>,
    pub trace_id: String,
    /// Internal-only: logged, never serialized into the response body.
    pub detail: Option<String>,
}

#[derive(Clone, Copy)]
pub enum ErrorCode { /* INGEST_CONTENT_TYPE_UNSUPPORTED, … */ }

impl ErrorCode {
    fn default_status(self) -> StatusCode { /* per-code table */ }
    fn message(self) -> &'static str { /* per-code table */ }
    fn hint(self) -> Option<&'static str> { /* per-code table */ }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // log self.detail + trace_id + code at the right level here,
        // then serialize ONLY the client-safe envelope.
    }
}
```

The registry (`ErrorCode` → status/message/hint) is the single table a
reviewer reads to audit the whole user-facing error surface.

### Mapping domain errors

Each domain error gets one `From`/`map_*` into `ApiError`. The handler
bodies stop hand-rolling `(StatusCode, Json(json!{…}))` and instead `?`
or `.map_err()` into `ApiError`. Concretely, the existing types fold in:

- `MetadataReject` → `INGEST_*` (already an enum — a clean 1:1 table).
- `ObjectReject` → `INGEST_FILE_*` (its `reason_code()` becomes the basis
  for the code; the structured payload stays internal as `detail`).
- `CompletionError` → `INGEST_DUPLICATE` / `INGEST_FILE_*` / `INTERNAL_ERROR`.
- `crate::error::Error` (SurrealDB / IO / adapter) → `STORAGE_UNAVAILABLE`
  or `INTERNAL_ERROR`, with the upstream text captured in `detail` only.

`trace_id` is generated at the request boundary (middleware) and stored in
request extensions, so `ApiError::new` can pick it up and every log line
in the request shares it.

### What changes in `crate::error`

`Error` stays the internal plumbing error. `ApiError` is the new
**boundary** type. Internals keep returning `Result<_, Error>`; only the
HTTP layer converts to `ApiError`. This preserves the module rules
(handlers depend on the error module's public interface; internals never
learn about HTTP).

## Frontend design

### One parser

```ts
// lib/api.ts
export type ApiErrorBody = {
  code: string;
  message: string;
  hint?: string;
  trace_id?: string;
  fields?: Record<string, unknown>;
};

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
    public hint?: string,
    public traceId?: string,
  ) { super(message); }
}

async function request<T>(path, init?) {
  const res = await fetch(/* … */);
  if (!res.ok) {
    const body = await res.json().catch(() => null) as { error?: ApiErrorBody } | null;
    const e = body?.error;
    throw new ApiError(
      res.status,
      e?.code ?? "INTERNAL_ERROR",
      e?.message ?? "Something went wrong.",
      e?.hint,
      e?.trace_id,
    );
  }
  // …
}
```

### Carry the object, not a code string

`UploadManager` (and peers) stop storing a bare `reason: string`. The
failed task carries the parsed `ApiError` (or a small `{code, message,
hint}`), so the tracker can render:

```
✗  paper.txt — This file type isn't supported.
   Upload a PDF, plain-text, or Markdown file.
   (INGEST_CONTENT_TYPE_UNSUPPORTED)
```

The code is shown small/monospace for support; the message + hint are the
prominent text. A shared `<ErrorNotice/>` component renders this for the
upload tracker, chat errors, and feed errors alike.

### Cross-cutting transport failures

A network failure (the `fetch` rejects before any response — e.g. a
direct-to-S3 PUT blocked by CORS) has no envelope. The frontend maps it
to a synthetic `NETWORK_ERROR` / `STORAGE_UNREACHABLE` with a generic
message + hint, so even the no-response case is legible rather than a
bare "failed".

## Logging & correlation

- The boundary middleware mints `trace_id` (ULID) and attaches it to the
  tracing span and the response envelope.
- `ApiError::into_response` logs `code`, `trace_id`, `status`, and the
  internal `detail` at `warn` (4xx) or `error` (5xx). The body sent to
  the client never contains `detail`.
- A user reporting "I got error `INTERNAL_ERROR` / trace `01J9Z…`" lets
  support jump straight to the matching log line.

## Rollout

Incremental, one module at a time, each an independent PR:

1. **Infra** — `error/api.rs`: `ApiError`, `ErrorCode` registry,
   `IntoResponse`, the `trace_id` middleware, the frontend `ApiError`
   parser + `<ErrorNotice/>`. No behavior change yet.
2. **Ingestion** (reference implementation) — convert the four
   `/uploads*` handlers + `MetadataReject`/`ObjectReject`/`CompletionError`
   mappings; wire the upload tracker to render message + hint + code.
   This is the surface that exposed the gap.
3. **Corpus / Chat / Discovery / Auth** — fold each in turn; delete the
   ad-hoc `(StatusCode, Json(json!{…}))` sites as they're replaced.
4. **Drift guard** — a test asserts every `ErrorCode` has a non-empty
   message and a valid status, and (once ts-rs lands, see
   [`testing.md`](./testing.md)) that the code list is mirrored to the
   frontend.

## Known gap this will make legible

The content-type fix already shipped (normalize charset/case/aliases
before the allowlist check). One case is still a hard reject: a file the
browser can't type at all arrives as `application/octet-stream` and is
refused. Today that surfaces as a generic failure. Two follow-ups, decided
later:

- With this error contract, it at least becomes a clear
  `INGEST_CONTENT_TYPE_UNSUPPORTED` + hint instead of "failed".
- Optionally, treat a generic/empty declared type as *unknown* and let the
  byte-level `validate_uploaded_object` sniff decide (PDF magic bytes /
  UTF-8 text), since that validator is the real gate. That's a
  security-posture change (more bytes reach S3 before rejection) and is
  deferred to its own decision.
```
