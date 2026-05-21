# Review — `upload-ui.md` (manual + autofilled document ingestion)

Reviewer pass against the actual codebase (backend `src/ingestion`,
`src/object_store`, `src/storage`, `src/text_extractor`, `schema.surql`,
the frontend tree) and the sister docs (`ingestion-v2.md`, `SECURITY.md`).
Plan section references use the doc's own `§` numbering; file/line cites are
real.

---

## Verdict

**Not implementable as-is by a fresh agent.** The frontend half (Phase 2) is
well specified and could be built almost verbatim. The backend half is built
on several claims that are *false against the current code*, and a fresh agent
following the plan literally would ship a regression. The three load-bearing
problems: (1) the plan's "client never sends `canonical_id`" model collides
head-on with the existing required-`canonical_id` wire shape **and** with the
`commit_upload` conflict pre-check, which would raise a false
`CanonicalIdConflict` on every manual upload after the first; (2) the new
stage-5 "extract text" stage downloads object bytes into the backend, directly
violating the ingestion-v2 invariant that *bytes never traverse the backend*,
and it leans on a `text_extractor` module that only does PDF and returns
`Vec<Word>`, not text; (3) deleting `LocalFsObjectStore` removes the only
in-process multipart implementation the existing integration tests use, while
the plan's replacement (`MemObjectStore`) has no multipart methods at all. Each
is a blocking issue below with a concrete fix. Once those are resolved the rest
is sound.

---

## Blocking issues

### B1. `canonical_id`-optional model contradicts the existing wire shape and validator

§Required-vs-optional, §1.0, §1.4, §2.7, and "Decisions / `canonical_id` is
optional" all assert the client never sends `canonical_id` and it defaults to
`NONE`. But:

- `CreateUploadRequest` (`backend/src/ingestion/validation/metadata.rs:24-31`)
  declares `canonical_id: String` (required, non-`Option`), plus
  `source_type: String` and `source_uri: String` as required.
- `validate_ingestion_metadata` (`metadata.rs:127-151`) rejects an empty
  `canonical_id` as `MalformedRequest` and then requires it to match
  `canonical_id_pattern`. It does the same for `source_uri`
  (`is_plausible_uri`, must be `http(s)://`).
- The plan's §2.7 `CreateUploadRequest` TS type omits `canonical_id`,
  `source_type`'s siblings, **and `source_uri` entirely**, while sending
  `source_type: "manual"`.

So a manual upload built per §2.7 (`source_type:"manual"`, no `canonical_id`,
no `source_uri`) is rejected with a 400 by the *current* validator. The plan
never reconciles this. ingestion-v2 §"POST /uploads" actually says the SPA
*generates* `canonical_id = "manual:<uuid>"` and a `source_uri` — the opposite
of this plan.

**Fix:** Pick one model and make it consistent end to end. Recommended,
matching the existing `manual:<uuid>` convention and avoiding the schema churn
in B2: keep the SPA generating `canonical_id = "manual:<ulid>"` and a
`source_uri` (e.g. `urn:delphi:manual:<ulid>` — but note `is_plausible_uri`
only allows `http(s)`, so either relax that check or have the SPA send a
placeholder https URL). If instead the team truly wants optional
`canonical_id`, the plan must: change `CreateUploadRequest.canonical_id` to
`Option<String>`, make `source_type`/`source_uri` server-defaulted for
`manual`, update every `validate_ingestion_metadata` branch and its tests
(`metadata.rs:233-417`), the `ok_req()` fixture, and the integration
`create_body` helper (`backend/tests/ingestion_uploads.rs:26-31`). The plan
lists none of these edits.

### B2. `commit_upload` conflict pre-check makes optional `canonical_id` actively unsound

§1.0 relaxes `canonical_id` to `option<string>` and §1.4 leaves it unset for
manual uploads. But `commit_upload`
(`backend/src/storage/surreal.rs:837-847`) does:

```
SELECT id FROM document WHERE canonical_id = $cid LIMIT 1
```

and returns `Err(CanonicalIdConflict)` if any row matches. With
`canonical_id = NONE` (or, today, with `doc.canonical_id` being a `String` that
would become `""`), **every manual upload after the first matches the previous
NONE/empty row** and is rejected 422 `canonical_id_conflict`. The same
unconditional pre-check also runs in `upsert_document`
(`surreal.rs:224-246`). The plan's §1.0 only worries about the *UNIQUE index*
on NONE; it never mentions this app-level pre-check, which is the real
landmine.

`Document.canonical_id` is also currently `String`
(`storage/models.rs:77`), and `DocumentWire.canonical_id` is `String`
(`surreal.rs:59`). Making the column `option<string>` without changing these
two types means a `None` serialises as the empty string `""`, not `NONE` —
defeating the entire "unique-when-set" intent and feeding the false-conflict
bug above.

**Fix:** The plan must spell out, as concrete edits: (a) change
`Document.canonical_id` and `DocumentWire.canonical_id` to `Option<String>`
with `skip_serializing_if`; (b) change `CreateUploadSessionParams.canonical_id`
and `UploadSession.canonical_id` (`storage/models.rs:275,301`) to
`Option<String>`; (c) make the `commit_upload`/`upsert_document` conflict
pre-check **skip entirely when `canonical_id` is `None`**, and the
`get_document_by_canonical` path tolerate it; (d) update the
`create_upload_session` SQL bind (`surreal.rs:785`) which currently binds a
non-null string. Without (c) the feature does not work for the second upload.

### B3. Stage 5 "extract text" violates the bytes-never-traverse-the-backend invariant

§Ingestion-workflow step 5 and §1.1 stage 5 add `extract_text(object_store,
key, content_type)` that pulls the object body into the backend so the LLM has
something to read, then persists it to `document_content`. This is in direct
conflict with ingestion-v2's stated single invariant ("bytes go **direct from
client to S3** … bytes never traverse the backend"; ingestion-v2.md:11-13,
277-279, 500-520) and with `SECURITY.md`'s layered model where the validator
only HEADs + ranged-GETs a sniff window (`object.rs:103-133`).

Downloading the *full* object for text extraction:
- reintroduces an unbounded in-backend download the whole design avoided (DoS /
  memory pressure; `validate_uploaded_object` deliberately caps at
  `sniff_window_bytes` and never pulls the body),
- runs the bytes through a parser **inside the request handler** synchronously
  during `/complete`, lengthening the `validating` window unboundedly,
- contradicts §Open-questions' own "autofill latency" note, which assumes the
  text is cheap.

It is also technically unimplementable as described: `text_extractor`
(`src/text_extractor/mod.rs`) is **PDF-only** (it shells out to
`pdftotext -bbox-layout`), takes full `Bytes`, and returns `Vec<Word>` with
bounding boxes — not "raw text". There is no text/markdown path, and there is
no flat-text accessor. §1.1 calls it "a thin wrapper over the existing
`text_extractor` module" — it is not thin and the module's contract doesn't fit.

**Fix:** Decide explicitly whether text extraction belongs in the *upload*
pipeline at all. Three coherent options, pick one and write it down:
1. **Defer text extraction to the existing async ingest/RAG pipeline**
   (`src/ingestion/pipeline.rs` + `rag.rs`), keeping `/complete` to validate +
   commit only. Autofill then runs later against extracted text; the
   `MetadataExtractor` seam moves to that stage. Cleanest; preserves the
   invariant.
2. If text must be available at commit, **bound it explicitly**: reuse the
   validator's `pdf_max_input_bytes` cap and the sandboxed-`pdftotext`
   discipline (`object.rs:159-184` reserves exactly this), add a
   text/markdown branch, and document the new in-backend download as a
   deliberate, capped exception to the invariant in `SECURITY.md`. The plan
   currently does neither.
3. Keep stage 5 but feed the extractor only the **already-fetched sniff
   window** plus declared metadata (no extra download). Weak for real LLM
   extraction but honest about the invariant.

Whichever is chosen, the plan must stop describing `text_extractor` as a
drop-in raw-text source and specify the actual adapter (PDF→`Vec<Word>`→join,
text/markdown→bounded read, output `Content { text, format, extractor }` for
the `document_content` insert — note `upsert_content` already exists,
`surreal.rs:303`).

### B4. Removing `LocalFsObjectStore` deletes the only in-process multipart test backend; `MemObjectStore` can't replace it

§Phase 0 deletes `local_fs.rs` and tells tests to "replace LocalFs with
`MemObjectStore` for non-multipart tests, MinIO testcontainer for multipart."
But:

- `MemObjectStore` (`src/object_store/mem.rs`) implements **only**
  put/get/delete/exists/get_range/head. It has **no** `create_multipart_upload`,
  `presign_upload_part`, `complete_multipart_upload`, `upload_part_direct`,
  `list_objects`, or `list_multipart_uploads`. The trait defaults return
  `NotImplemented`.
- `LocalFsObjectStore` is the *only* impl with a working in-process multipart
  shim + `upload_part_direct` (`src/object_store/local_fs.rs:178-359`,
  emitting the `local-multipart://` URLs the tests assert on).
- The existing integration suite drives full create→sign→complete entirely
  in-process via `TestApp::build_with_local_fs()`
  (`backend/tests/common/mod.rs:96-141`) and asserts
  `url.starts_with("local-multipart://")`
  (`backend/tests/ingestion_uploads.rs:261`). Every one of these tests
  (`ingestion_uploads.rs`, and `upload_session_cross_tenant.rs` which also uses
  the helper) breaks the moment LocalFs is gone.

So Phase 0 as written turns ~10 fast in-process integration tests into
either MinIO-testcontainer tests (slow, needs Docker in CI — contradicts the
testing-doc "no testcontainers" Mem-engine ethos) or deletes coverage.

**Fix:** Either (a) **keep a minimal in-process multipart shim** — move the
LocalFs multipart logic into `MemObjectStore` (add the four multipart methods +
`upload_part_direct` + listing, backed by the in-memory map) so the existing
tests keep running without Docker; or (b) explicitly accept the migration of
those tests to the MinIO testcontainer and enumerate each test that moves,
its new gating env var, and the CI change. The plan currently hand-waves "adapt
to the test backend split" (§Phase 0 Edit bullet) without resolving that
`MemObjectStore` is multipart-incapable. Recommend (a); it is the smaller
change and matches testing.md's "no docker for unit/integration" stance.

### B5. `GET /uploads/:doc_id` cannot return `ready` with a `doc_id`, but the whole frontend tracker depends on it

§2.3 polling, §2.7 (`UploadStatus = … | { state: "ready"; doc_id }`), and §2.2
("`validating` keeps polling; `ready` → terminal") build the entire task
tracker on `GET /uploads/:id` resolving to `ready`. But the current handler
**cannot do that**: on commit the `upload_session` row is deleted in the same
transaction (`surreal.rs:850-856`), and the status handler's own comment admits
"We don't have a `get_document_by_doc_id` lookup keyed on the session's
`doc_id`" and the `Ready` arm is dead code (`uploads.rs:643-665`,
`StatusResponse::Ready` marked `#[allow(dead_code)]` at `uploads.rs:190`).
After a successful commit, polling `GET /uploads/:id` returns **404** (session
gone, no rejection row), which §2.3's loop would map to a `failed`
"check the feed" timeout — i.e. a successful upload shows as failed.

ingestion-v2.md §"GET /uploads/:id" resolution order step 2 *specifies* a
"document exists with `doc_id` via a tenant-scoped lookup → ready" — but that
lookup was never implemented because `doc_id` (the session string) was not the
document record id (the doc got a fresh `CREATE ONLY document` id;
`surreal.rs:853`).

This is exactly what the plan's §1.4 "`doc_id` is the record id" change is
*supposed* to fix — but the plan never connects the two. If `CREATE
document:<doc_id>` lands (B6), the status handler must also gain a
`get_document(document:<doc_id>)`-by-record-id branch returning `ready`.

**Fix:** Add to the plan an explicit edit to `get_upload_status`: after the
session/rejection checks, look up `document:<doc_id>` (now that §1.4 makes the
record id deterministic) under AuthedDb and return `StatusResponse::Ready {
doc_id }`. Add the `Storage` method (or reuse `get_document` with a
`RecordId::from(("document", doc_id))`). Otherwise the frontend never observes
`ready`. Alternatively, since the SPA already learns the doc id from the
`/complete` 200 response (`CompleteResponse::Ready`, `uploads.rs:526-532`), the
plan could have the manager treat the `/complete` 200 as the terminal `ready`
signal and **not poll at all** on the happy path — but §2.3 explicitly polls
after complete, so the plan must choose.

### B6. §1.4 `CREATE document:<doc_id>` is a real change the plan understates, and it interacts with `content_hash` dedup

§1.4 and "Decisions / `doc_id` is the identity" assert `/complete` does `CREATE
document:<doc_id>`. The current `commit_upload` does `CREATE ONLY document
CONTENT $data RETURN id` (`surreal.rs:851-856`) — a random record id, **not**
`document:<doc_id>`. So the claim "`document.id = "document:" + doc_id`" is
false today and requires editing the transaction SQL to `CREATE ONLY
document:⟨id⟩ CONTENT $data` (or `type::thing("document", $doc_id)`), plus
verifying `doc_id` (a lowercased ULID, `uploads.rs:257`) is a legal SurrealDB
record-id key.

Also unaddressed: `content_hash` is set from the S3 ETag
(`uploads.rs:521`), and the schema has a non-unique `document_content_hash`
index (`schema.surql:160`) but the *dedup* contract historically keyed on
`canonical_id`. With `canonical_id` now optional/absent, the plan should state
what (if anything) dedups manual uploads (ETag-based content_hash is **not**
unique-indexed, and S3 multipart ETags are not content hashes — they're
`md5-of-md5s-N`, so two identical files uploaded with different part counts get
different ETags). The plan's "content_hash from the validated object" (§Required
fields) is therefore not a reliable dedup key and should not be sold as one.

**Fix:** Spell out the `commit_upload` SQL change and a `doc_id`-key legality
check in §1.4; add a unit test as the plan promises. Separately, add a sentence
clarifying that manual uploads are **not** deduped (identity is the record id;
`content_hash` is informational and ETag-derived, not a stable content hash) —
or specify computing a real SHA-256 if dedup is wanted (but that needs the full
bytes, see B3).

### B7. `MetadataExtractor` validation seam reuses a validator that doesn't accept its type

§1.1 stages 7 and 9 call `validate_metadata(&autofilled, ctx.policy)` and
`validate_metadata(&merged, ctx.policy)`, and §1.3 says "Same metadata
validator, three call sites." But the existing validator is
`validate_ingestion_metadata(req: &CreateUploadRequest, policy)`
(`metadata.rs:94`) — it validates the *wire request* (content-type, size,
forbidden fields, canonical_id/source_uri shape), none of which apply to an
`ExtractedMetadata` (§1.2: `title/authors/summary/language/published_at/extra`).
Feeding `ExtractedMetadata` to `validate_ingestion_metadata` is a type error and
a semantic mismatch (an autofill result has no `content_type`/`size`/`source_uri`
to validate). The plan also invents a `required_fields: HashSet<Field>` knob and
a `Field` enum (§1.3) that don't exist.

**Fix:** Specify a *second*, distinct validator — e.g.
`validate_descriptive_metadata(meta: &ExtractedMetadata | &MergedMetadata,
policy) -> Result<(), MetadataReject>` — that checks descriptive-field sizes,
`title` length (reuse `max_title_chars`), `published_at` sanity, `extra`
depth/size (reuse `json_depth` + `max_metadata_bytes`), and `required_fields`.
Define the `Field` enum and where `MetadataPolicy.required_fields` is wired
(`UploadsConfig::from_env`, `uploads.rs:55-115`, plus `test_default`). Don't
claim it's "the same validator"; it shares helpers, not the entry point.

### B8. `from_url` made required + `OBJECT_STORE_URL` only-`s3://`, but the S3 store needs `INGEST_S3_ENDPOINT_*` that `from_url` never sees

§Phase 0 makes `OBJECT_STORE_URL` required and `s3://`-only, and §1.5/§1.6 add
`INGEST_S3_ENDPOINT_INTERNAL` / `INGEST_S3_ENDPOINT_PUBLIC`. But `from_url`
(`src/object_store/url.rs:18`) takes only the URL string; the S3 client needs
endpoint/region/keys/path-style. Today those come from `S3Config::from_env`
(`s3.rs:51-74`) which reads the single `INGEST_S3_ENDPOINT` — the plan renames
it to two endpoints but never says how `from_url` (or its replacement)
constructs an `S3ObjectStore` carrying *both* the internal endpoint (for
HEAD/GET/complete/abort) and the public endpoint (for presign). The plan asserts
"`presign_upload_part` signs against the public host; every other method uses
the internal one" (§1.5) without specifying that `S3ObjectStore` holds two
configured clients/endpoints, or how `from_url` is rewired to build it.

**Fix:** Specify that `S3ObjectStore::from_env` (not `from_url`) reads
`INGEST_S3_ENDPOINT_INTERNAL` + `INGEST_S3_ENDPOINT_PUBLIC` and builds either
two `aws_sdk_s3::Client`s (one per endpoint) or one client plus a
presign-only config pointed at the public endpoint. State how `from_url`
dispatches `s3://` to this constructor (it currently calls
`s3::not_yet_supported`, `url.rs:24`). Note `S3Config` currently has a single
`endpoint: Option<String>` field that must be split.

---

## Should-fix issues

### S1. Presign-against-public-endpoint correctness with path-style + Traefik `/s3` prefix is unverified and load-bearing

§1.5/§Open-questions flag it, but it's more than a verify-later: with
`force_path_style=true` the signed URL is
`http://localhost/s3/<bucket>/<key>?X-Amz-...`. SigV4 signs the **host header
and the full path** including `/s3`. MinIO behind Traefik will only validate
that signature if Traefik forwards the `/s3` prefix *unstripped* and MinIO is
configured to expect it (or Traefik strips `/s3` and the backend signs
*without* it). Getting this wrong silently produces `SignatureDoesNotMatch` on
every browser PUT. The plan should state the exact Traefik rule (StripPrefix or
not) and which path the backend signs, not defer it.

### S2. MinIO CORS is security-relevant and under-specified

§1.5 references a `/cfg/cors.json` and "CORS allows the Vite origin (:5173)"
but no CORS document content, no allowed-methods/headers list, and §Open-questions
admits `mc cors set` syntax varies by version. Since the browser PUTs
cross-origin in tier-1, CORS must allow `PUT`, the `Authorization`-free presigned
query auth, and expose `ETag` (Uppy reads the part ETag from the response — if
`ExposeHeaders: ETag` is missing, `completeMultipartUpload` gets empty etags and
fails). Specify the cors.json contents, especially `ExposeHeaders: ["ETag"]`.
This is a common, silent multipart breakage.

### S3. Per-file `enqueue` concurrency vs. one Uppy instance + per-file prefill

§2.3 says "One Uppy instance per manager" and §Decisions says each file becomes
its own session. But the single-file prefill (§2.4) must be attached to *that
file's* `createMultipartUpload` call. With one shared Uppy and `enqueue(files,
prefill)`, the manager must stash prefill per-file (e.g. via
`uppy.setFileMeta`) so concurrent `createMultipartUpload` callbacks read the
right prefill. The plan's `createMultipartUpload` (§2.3) closes over a single
`…prefill` — fine for single-file, ambiguous if a later `enqueue` with a
different prefill runs while earlier files are still creating. State that prefill
is stored on file meta at enqueue time and read from `file.meta` in the
callback.

### S4. Schema is applied idempotently at boot via `IF NOT EXISTS`; changing a field TYPE is not idempotent that way

§1.0 says the `canonical_id` type change is "applied idempotently at boot." But
the schema uses `DEFINE FIELD IF NOT EXISTS` (`schema.surql:300`), which is a
**no-op if the field already exists** — it will *not* alter an existing
`TYPE string` field to `option<string>`. To change a field type you need
`DEFINE FIELD OVERWRITE` (as the tenant_id fields use, `schema.surql:288`) or a
`REMOVE FIELD` + redefine. Same for swapping a UNIQUE index to non-unique:
`DEFINE INDEX IF NOT EXISTS` won't replace an existing index; you need `REMOVE
INDEX IF EXISTS` first (the schema already does this pattern at lines 156, 263,
363). The plan must specify `OVERWRITE`/`REMOVE` statements, not rely on `IF NOT
EXISTS` idempotency.

### S5. `upload_session_canonical` UNIQUE index + optional canonical_id needs the same NONE handling, and the create path 409 logic keys on the index name

§1.0 covers the UNIQUE-on-NONE question for `document` but the
`upload_session_canonical` index (`schema.surql:310`) has the identical problem:
two concurrent manual uploads (both `canonical_id=NONE`) would collide and the
second gets a spurious 409 in `create_upload`
(`uploads.rs:309` matches the error string `"upload_session_canonical"`). Verify
SurrealDB's NONE-in-UNIQUE behaviour (the genuinely open question) and, if NONE
is *not* excluded, drop the UNIQUE on `upload_session_canonical` for manual
uploads too. Tie the §Open-questions item to *both* indexes.

### S6. `handle_object_reject` already routes session-delete through SystemDb; the plan's stage-9 reject path must match

The current reject path (`uploads.rs:569-611`) deletes the session via
`SystemDb` (because the handler has no `Extension<AuthedDb>` in that helper) and
records the rejection via `SystemDb` (PERMISSIONS deny user writes,
`surreal.rs:878-888`). The plan's §1.1 stage-9 "merged metadata reject ⇒ wipe S3
+ log" must reuse this exact path, and the `run_completion` pipeline (which
returns `Result<DocId, CompletionError>`) needs access to both the AuthedDb
handle (for the CAS/commit) and SystemDb (for rejection logging). The plan's
`CompletionCtx` (§1.1) lists `object_store`, `key`, `content_type`, `extractor`,
`prefill`, `policy` but **not** the two DB handles or `auth` — so the sketched
`run_completion` can't actually commit or log a rejection. Specify the full
`CompletionCtx` fields.

### S7. `Content` insert at commit needs `format`/`extractor`; the transaction is currently doc-only

§1.1 stage 10 says commit writes `document` + `document_content` + session
delete "one transaction." The current `commit_upload`
(`surreal.rs:832-867`) only does `CREATE document` + `DELETE upload_session`.
Adding `document_content` means: extending `commit_upload`'s signature to take
the extracted `Content` (or a new `commit_upload_with_content`), inserting into
`document_content` with `doc = document:<doc_id>`, `format`, `text`,
`extractor` (schema requires all, `schema.surql:181-185`), and keeping it inside
the same `BEGIN/COMMIT`. The plan should state the new method signature and that
`document_content_doc` is a UNIQUE index (`schema.surql:187`) so re-commit must
not double-insert. (Also gated on B3 — if text extraction is deferred, this
stage drops.)

### S8. Frontend module placement and test-runner constraints not cross-checked against repo rules

§2.1–2.5 put `uploadManager.ts` in `lib/`, components under
`components/upload/`, route `routes/upload.tsx`. That fits the existing tree
(`frontend/src/lib`, `components/`, `routes/`). But: (a) the nav `<Link
to="/upload">` must be added to `__root.tsx` (`routes/__root.tsx:50-60`) — the
plan's §2.1 ascii diagram shows it but the prose doesn't list editing
`__root.tsx`; (b) the plan should note that per testing.md, Vitest runs under
Node (`make frontend-test`), and `uploadManager.test.ts` must fake timers for
the auto-dismiss/poll-interval assertions; (c) `ulid` is already a dependency
(package.json) — good, the plan's client-ULID task id is free. Uppy packages
(`@uppy/core`, `@uppy/aws-s3-multipart`, `@uppy/dashboard`, etc.) are **not**
yet in package.json; the plan should list them as additions.

### S9. `content_hash` from ETag breaks if MinIO returns a multipart ETag with a dash

`uploads.rs:521` does `validated.etag.trim_matches('"')`. Multipart ETags look
like `"d41d8cd...-3"` (hash-dash-partcount). The plan keeps this. It's not a
correctness bug for storage, but the doc claims `content_hash` is a dedup hash
(B6). At minimum note the value is opaque and not comparable across re-uploads.

### S10. `is_dev`/role gate in tier-1: confirm dev-auth injects the `ingester` role

All four endpoints require `ingester` (`uploads.rs:205-211`). The plan's
tier-1 e2e (§E2E, `@tier2` actually — note the spec is tagged `@tier2` but the
prose says "tier-1"; pick one) and the dev-auth header injector must include
`ingester` in `X-Auth-Roles`, or every upload 403s in dev. The plan doesn't
mention wiring the dev role. Verify the dev-auth middleware's role set and state
it.

---

## Nits

- **N1.** §1.5 header is duplicated: "### 1.5 MinIO in both compose files" and
  "### 1.5 Internal vs public endpoint" are both numbered 1.5. Renumber
  (1.5/1.6/...); §1.6 `S3ObjectStore` and §1.7 env-vars then shift, and the
  §"Implementation order" references to "1.5–1.8" don't match the section count
  (there's no §1.8).
- **N2.** §2.7 is titled "2.7 API client" but there's no §2.6 (jumps 2.5→2.7).
- **N3.** §2.7 `completeUpload` returns `post(...)` untyped; the manager's §2.3
  `completeMultipartUpload` ignores the body and starts polling — fine, but the
  return type should be `void`/`unknown` explicitly.
- **N4.** §1.2 `ExtractedMetadata.extra: serde_json::Value` "shallow object
  merge with prefill keys winning" (§Merge policy) — specify behaviour when
  `extra` isn't an object (skip-merge vs. error).
- **N5.** ingestion-v2.md still lists `LocalFsObjectStore` in its architecture
  diagram (ingestion-v2.md:62) and "Storage abstraction"
  (ingestion-v2.md:553-555); §Phase 0 Docs bullet says "strike LocalFs" — good,
  just make sure the diagram and the `from_url` scheme list (`url.rs:13-17`
  doc-comment) are included.
- **N6.** §Non-goals says a full browser reload "loses in-flight uploads" — but
  Uppy's `aws-s3-multipart` + `localStorage` can resume; the plan disclaims
  resume (fine), just note the in-memory manager intentionally forgoes Uppy's
  own persistence so the two don't half-resume.

---

## Things the plan gets right (leave these alone)

- **The frontend tracker architecture is sound.** Manager-above-the-router as a
  plain TS observable consumed via `useSyncExternalStore`, polling owned by the
  manager (not a route-scoped hook) so it survives navigation, and the submit
  button freeing the UI immediately — this all holds together and matches the
  existing `__root.tsx` layout and TanStack Router setup. The per-task state
  machine (§2.2) is clean and the single highest-value test choice
  (`uploadManager.test.ts`) is correct.
- **The three-validation-points framing** (prefill / autofill-output / merged)
  is the right shape for treating LLM output as untrusted, even though the
  validator reuse claim needs fixing (B7).
- **The `MetadataExtractor` trait + `NoopExtractor` placeholder + Phase-3
  `LlmExtractor` swap** is a clean seam wired through `AppState`
  (`state.rs` already carries `Arc<dyn ...>` collaborators) and follows the
  repo's dependency-inversion rule. The fake-LLM test plan (§Phase 3) reuses
  `backend/tests/common/fake_llm.rs`, which exists.
- **"Workflow as code" `run_completion`** with named stages is a good legibility
  call and matches the existing thin-handler style in `uploads.rs`.
- **S3-only, drop LocalFs in production** is the right end state; only the
  *test* fallout (B4) needs handling.
- **Module placement** (`pipeline.rs`, `autofill.rs`, `validation/metadata.rs`)
  respects the public-interface-only rule in `.claude/CLAUDE.md` — the new
  symbols would be re-exported through `ingestion/mod.rs` and
  `validation/mod.rs` as the existing ones are.
- **Deferring the real LLM logic to Phase 3** behind a shipped seam is the
  correct sequencing given autofill is a no-op today; the empty `required_fields`
  default (§Required-vs-optional) correctly avoids failing every multi-file
  upload before the extractor lands.
