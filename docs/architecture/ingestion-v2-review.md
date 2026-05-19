# Ingestion v2 — Plan Review

Status: review of [`ingestion-v2.md`](./ingestion-v2.md) against
[`ARCH.md`](../ARCH.md), [`SECURITY.md`](../SECURITY.md), and the
existing backend (storage, auth, object_store, ingestion modules).

Bottom line: the **overall design is sound**. The split between
`upload_session` and `document`, server-derived tenant prefix,
"delete on reject", validator-after-complete, and the age-bounded
cleaner are all the right primitives. The plan is at roughly 70% of
the precision a fresh agent would need to implement it without
re-deriving decisions; the gaps below are about specificity, not
direction.

The review is organised as: (1) **must fix** before a fresh agent
starts — places where the plan is ambiguous, contradicts existing
architecture, or under-specifies a security-relevant boundary; (2)
**should fix** — clarifications that prevent foreseeable rework; (3)
**nice to have** — small additions; (4) **what's solid** — explicit
list of things the plan already gets right, so a re-reader doesn't
second-guess them.

---

## 1. Must fix before implementation

### 1.1 Specify how `upload_session` is scoped on the engine

The plan's schema block shows `DEFINE FIELD tenant_id ON upload_session
TYPE record<tenant>;` with **no `DEFAULT $auth.tenant_id`, no `ASSERT`,
and no `PERMISSIONS` clause on the table**. Every other domain table
in `backend/schema.surql` follows the pattern documented at the top of
that file:

```sql
DEFINE TABLE upload_session SCHEMAFULL
    PERMISSIONS
        FOR select, update, delete WHERE tenant_id = $auth.tenant_id
        FOR create WHERE tenant_id = $auth.tenant_id;

DEFINE FIELD tenant_id ON upload_session TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT $value != NONE;

DEFINE FIELD user_id ON upload_session TYPE record<app_user>
    DEFAULT $auth.id
    ASSERT $value != NONE;
```

Without this, the plan's claim that "tenancy is enforced server-side"
is half-true — the *app-level* `session.tenant_id == auth.tenant_id`
check works, but a future query that forgets it leaks. ARCH.md's
guarantee ("the database itself refuses cross-tenant queries") relies
on the schema, not the handler. Make the schema the primary defence
and the handler check the (redundant, intentional) belt.

Also confirm the `user_id` field's semantics: only the originating
user can sign parts and complete, or any member of the tenant with
the ingester role? The plan is silent. Pick one and bake it into
PERMISSIONS (e.g. `... AND user_id = $auth.id` for write rules).

### 1.2 The `complete` transition must be atomic and idempotent

The plan describes `/complete` as: load session → call
`CompleteMultipartUpload` → UPDATE state="validating" → run validator
→ (pass) INSERT document + DELETE session, (fail) DeleteObject +
DELETE session.

Two races are unaddressed:

- **Concurrent completes.** Two POSTs arrive for the same session.
  Both see `state="uploading"`, both call S3's
  `CompleteMultipartUpload` (S3 itself idempotently completes once),
  both run the validator, both attempt the final INSERT. Need a
  compare-and-swap state transition in SurrealDB:

  ```sql
  UPDATE $session SET state = "validating"
      WHERE state = "uploading" RETURN AFTER;
  ```

  If the result set is empty, return 409 — another caller has it.

- **Concurrent DELETE-during-validate.** While the validator is
  running (state="validating") a DELETE /uploads/:id call arrives.
  The plan's DELETE handler does `DeleteObject` if
  state="validating". If the validator then commits its INSERT,
  the document row points at a deleted blob. Pick one:
  (a) **DELETE refuses when state="validating"** (returns 409,
      caller retries after status flips), or
  (b) Validator's pass-transition checks state is still "validating"
      atomically (`UPDATE ... WHERE state = "validating"` — if zero
      rows, abort and DeleteObject the blob itself).

(a) is simpler; (b) is more permissive. Either works, but the plan
must pick one and write it down.

Same problem with retried `/complete` calls from the SPA — make
explicit whether a retry after a transient error re-runs validation
(state="validating" stays, the second call sees it and returns
202 "in progress") or returns 409.

### 1.3 `canonical_id` collision at `complete` time

Both tables have `UNIQUE (tenant_id, canonical_id)`. So:

- T0: Alice creates upload_session with `canonical_id = "doi:X"`.
- T1: Bob (same tenant) ingests "doi:X" via the JSON endpoint — a
  `document` row exists.
- T2: Alice's upload completes, validator passes, INSERT document
  fails on the UNIQUE constraint.

The plan does not say what happens. Three reasonable options, pick
one explicitly:

- **422 with `canonical_id_conflict`.** Wipe the blob, drop the
  session, surface the existing doc_id. Probably the right answer
  for the MVP because it matches the dedup behaviour of the legacy
  endpoint (`IngestOutcome::Unchanged`).
- **Auto-merge** into the existing document if content_hash
  matches — closer to the legacy `Pipeline::ingest` behaviour
  (Created/Unchanged/Versioned). Costs more code; needs a
  `content_hash` computed from the uploaded bytes (see 1.6).
- **Reject at `/create`** by checking the document table too.
  Doesn't fix the race, just narrows it. Useful as a first-line
  UX defence even if (1) or (2) is the canonical fallback.

The plan claims "M7 (non-transactional ingest) — saga is explicit
and the rollback path on each failure is documented" — but the
saga's failure modes are not enumerated. List them: `complete` fails
mid-`CompleteMultipartUpload`, validator fails, document INSERT
fails on UNIQUE, session DELETE fails. For each: what runs, in what
order, who picks up the orphan.

### 1.4 Content-Type / Content-Length presigned-URL claim is overstated

The plan asserts S3 enforces Content-Type and per-part Content-Length
because they are baked into the signature. Two corrections:

- **Content-Type.** `CreateMultipartUpload`'s `ContentType` parameter
  sets object metadata; the individual `UploadPart` requests do not
  carry `Content-Type` and S3 does not verify that the bytes match
  the declared type. Calling it "S3 itself enforces it" is wrong;
  the validator at `/complete` is where this check actually
  happens. Rewrite the in-flight bullet in SECURITY.md and the
  ingestion-v2 plan to say *"the declared Content-Type is recorded
  on the object metadata; actual byte-level enforcement happens at
  `/complete`"*.
- **Per-part Content-Length.** SigV4 can include `content-length`
  in `SignedHeaders`, but in that case S3 requires the client to
  send **exactly** that value — not a ceiling. A practical
  consequence: a uniform 8 MB part size signed exactly won't fit
  the natural last (smaller) part; Uppy will send a part smaller
  than the signed value and S3 will reject the request. The plan's
  `content_length_limit: Option<u64>` in `presign_upload_part` is
  not implementable as a ceiling on standard S3-API. Drop it (or
  rename it to `expected_part_size_bytes` and accept exact-match
  on every part *except* the last, which is signed without
  content-length). Either way, the *backstop* against
  oversize-per-part is the multipart cap × `INGEST_UPLOAD_PART_SIZE_BYTES`
  arithmetic, the `validate_uploaded_object` HEAD check, and a
  bucket-side `aws:object-lock`-style policy at most.

This isn't a security collapse — the validator catches oversize
declared-vs-actual at complete time — but the plan's promise that
"S3 itself enforces them" needs to be walked back so the
implementing agent doesn't waste a day trying to sign content-length
ceilings.

### 1.5 Specify the connection / pool used for each DB operation

The Storage trait is request-scoped (`AuthedDb` from the pool); the
SystemDb is privileged. The plan does not say which is used where.
Concretely:

- `/create`, `/sign-part`, `/complete`, `DELETE`, `GET /:id` —
  **AuthedDb** (request handler scope, engine-side PERMISSIONS
  enforce tenancy).
- Nightly cleaner — **SystemDb** (cross-tenant SELECT FROM
  document, deletes objects without a request context).

Add a section "DB access pattern":

```text
                          | conn       | reason
--------------------------|------------|----------------------------
POST /uploads             | AuthedDb   | per-request; PERMISSIONS scope
POST /uploads/:id/...     | AuthedDb   | per-request; PERMISSIONS scope
DELETE /uploads/:id       | AuthedDb   | per-request
GET /uploads/:id          | AuthedDb   | per-request
Nightly cleaner           | SystemDb   | cross-tenant orphan sweep
```

Also extend the `Storage` trait (or create a typed `IngestionStore`
sub-module exposed off `AuthedDb`) with the upload-session methods.
The plan never lists the trait additions; the implementing agent
needs them written out:

```rust
async fn create_upload_session(&self, ...) -> Result<UploadSession>;
async fn get_upload_session(&self, doc_id: &str) -> Result<Option<UploadSession>>;
async fn transition_upload_session_state(...) -> Result<bool>;  // CAS
async fn commit_upload(&self, doc_id: &str, doc: &Document) -> Result<DocId>;  // tx
async fn delete_upload_session(&self, doc_id: &str) -> Result<()>;
```

The plan currently shows raw SurrealQL inline in the handlers; that
violates the storage-as-opaque-interface rule in `.claude/CLAUDE.md`
and contradicts how every other module in the codebase consumes the
storage layer.

### 1.6 Validator memory / download budget

`validate_uploaded_object` calls HEAD, then "sniff first N bytes",
then "format-specific parse". The plan caps `sniff_window_bytes`
and `pdf_max_pages` but **does not bound how the bytes get from S3
into the parser**. A malicious client uploads a 200 MB file with a
valid PDF magic header followed by junk — does the PDF parser get
the whole 200 MB streamed in? Loaded into memory? Spilled to a
tempfile?

Specify:

- The validator issues a **ranged GET** (or several) — not a full
  GET — for the sniff window.
- For PDF parse, either (a) stream into the parser with a hard
  byte budget bounded by `pdf_max_input_bytes` (new policy field),
  or (b) require the parser to operate on a tempfile capped by
  `pdf_max_input_bytes`.
- Process-level resource bounds (timeout already covered;
  output-size / memory cap on the parser process — the equivalent
  of the H4 hardening on `pdftotext`).

H4's resolution (kill_on_drop + timeout + output cap + size pre-check)
is the template; the validator must inherit those guarantees, not
just timeout + page count.

### 1.7 Role gate

The existing `POST /api/ingestion/documents` requires `ingester` or
`owner` in the role claim (`backend/src/ingestion/http.rs:32-94`).
The plan doesn't restate this for the new endpoints, but the new
endpoints are first-class ingestion. Either:

- Apply the same `INGESTER_ROLES` gate to `/uploads` and
  `/uploads/:id/sign-part` and `/uploads/:id/complete` — call out
  explicitly in the plan, or
- Make the no-role case do something meaningful (e.g. only allow
  uploads into a personal scratch space) — but that's a product
  decision, not an oversight to be deferred.

SECURITY.md already says "any authenticated user with the ingester
role"; the plan should match.

---

## 2. Should fix

### 2.1 Define the canonical `storage_uri` form

The cleaner does
`not exists(SELECT FROM document WHERE storage_uri = "s3://.../" + o.key)`.
That string has to be byte-identical to what the `/complete` handler
wrote. Specify the canonical form once, at the top of the storage
section:

```text
storage_uri = "s3://<bucket>/<key>"
key         = "tenants/<tenant_id>/<doc_id>"
```

Two strict rules: no querystring, no leading slash, exactly that
prefix. Add a unit test that round-trips the string between
`/complete` and the cleaner via a shared helper.

Also specify the `tenant_id` portion: is it the raw SurrealDB
record id (`tenant:acme`), the slug (`acme`), the URL-safe id
component (`acme`)? Pick the slug — record-id colons in S3 keys are
legal but ugly, and the slug is already a `[a-z0-9-]` constraint per
the schema's ASSERT.

### 2.2 Document delete → object delete

A document deleted via `Storage::delete_document` leaves its S3
blob orphaned until the cleaner's age threshold kicks in (≥4h).
For multi-tenant SaaS that's a privacy concern. Either:

- Add a best-effort `ObjectStore::delete(storage_uri)` to
  `delete_document` (with cleaner as backstop for failures), or
- State explicitly in the plan that "delete is lazy via the cleaner;
  do not deploy v2 to a tenancy mode that requires prompt-delete
  semantics" — and link this back to SECURITY.md as a deferred
  defence.

The current `LocalFsObjectStore` doesn't have this either, so it's
not a regression — but the new doc is the right place to set the
expectation.

### 2.3 Cleaner cron vs. orphan accumulation

`INGEST_CLEANUP_MIN_AGE_HOURS=4` but `INGEST_CLEANUP_CRON='0 3 * * *'`
(daily). An attacker who manages to leak orphans (e.g. spam `/create`
without completing) sees them accumulate for up to ~24h before the
nightly run, even though each one is "old" after 4h. That's an S3
bill amplification, not a security issue.

For the MVP it's fine, but the plan should either:

- Default the cron to every 4h (`0 */4 * * *`) so the worst case is
  bounded by `2 × MIN_AGE_HOURS`, or
- Document a separate "fast-path" cleanup that runs after `/create`
  rejects an orphan and bound the daily-cron use-case to
  list-multipart-uploads only.

Also: enumerate the cleaner's behaviour for "session row exists but
no S3 multipart" (DB only) and "S3 multipart exists but no session
row" (S3 only). The plan implicitly covers both, but write the
matrix.

### 2.4 Rejection reason persistence

The plan caches rejection reasons in a "short-TTL log lookup" with
no concrete backing. After process restart, the SPA polls and sees
a bare 404. For a single-process backend it's fine; for multi-replica
(SaaS), an in-process LRU silently breaks across instances.

Options:

- Keep an `ingestion_rejection` row (small, tenant-scoped, TTL'd by
  the cleaner). Cheap, explicit, multi-replica safe. Matches what
  SECURITY.md hints at when discussing the future
  `ingestion_audit_log` table.
- Return the rejection reason from `/complete` (already specified)
  and accept that the polling-after-restart case shows a bare
  "upload not found". Probably correct for MVP.

Pick one. The plan currently waves at this.

### 2.5 Per-route body limits

The plan asserts H3/I1 is closed because the new endpoints have tiny
per-route `DefaultBodyLimit`. Spell out the limits in the
configuration table:

- `/uploads` POST: 8 KB (declared metadata)
- `/uploads/:id/sign-part` POST: 256 B
- `/uploads/:id/complete` POST: max-parts × 64 B = ~640 KB
- `DELETE`, `GET`: 0

Otherwise the implementing agent will guess.

### 2.6 List of audit items: claim vs. reality

The plan says it closes M7, M8, H3/I1. SECURITY.md is a *living doc*
and AUDIT.md uses checkboxes. The plan should not pre-tick AUDIT
items — that happens after merge — but should list them as "targets"
so the reviewer can verify. Also missing from the claim list:

- **M9** is unrelated (frontend `source_uri` rendering) — don't
  claim it.
- **M11** is unrelated.
- A new audit ID may need to be opened if the validator design
  introduces a new attack surface we want to track separately.

---

## 3. Nice to have

- **State diagram.** Three states across two tables would be readable
  as a small ASCII state machine. `(no session) → uploading →
  validating → (no session, document row) | (no session, no row)`.
- **Sequence diagrams** for create-sign-complete and for
  validator-failure. Worth 15 minutes of plan time to save the agent
  from re-deriving the order.
- **Explicit list of `infer`/`tree_magic` matches per allowed
  content-type.** Polyglot files (PDF magic in a ZIP, etc.) are a
  classic sniffer dodge. Spell out the rule: "reject if sniffer
  matches >1 type in the allowlist."
- **`canonical_id` shape for `source_type = "manual"`.** What does
  a user-uploaded PDF claim as `canonical_id`? The plan's regex
  policy needs at least one explicit example: `"upload:" || uuid()`,
  or hash of bytes, or the doc_id itself. Otherwise the validator
  will reject every manual upload until someone picks.

---

## 4. What the plan gets right (do not redesign)

- **Two tables, no `state` column on `document`.** This is the
  single most important design call in the doc. Don't let a
  refactor merge them.
- **Server-derived key prefix.** Every flaw a client could exploit
  by setting `tenant_id` / `storage_uri` / `key` is closed by
  construction.
- **Pure validator functions on a policy struct.** Auditable in
  isolation; future hardening (AV, polyglot, format-specific) has a
  single landing spot.
- **Delete on reject, not quarantine.** Matches SECURITY.md's
  rationale word-for-word. Don't be tempted by a "rejected" status
  column.
- **Age-bounded cleaner with `> 2× session TTL` assertion.** Closes
  the validator-vs-cleaner race the right way.
- **`ObjectStore` trait extension.** Multipart methods alongside the
  existing put/get/delete; one impl serves every S3-compatible
  backend. The placeholder `s3.rs` (currently
  `Error::NotImplemented`) already anticipates this.
- **Uppy `AwsS3Multipart` mapping.** The four-callback shape is the
  off-the-shelf one; no custom client code needs to ship.

---

## 5. Implementation order — suggested adjustments

The plan's eight steps are fine, with two changes:

- **Insert a step 0:** apply the schema migration. Define
  `upload_session` with PERMISSIONS / DEFAULTs as in §1.1, and
  write a `cross_tenant_isolation`-style test that proves a
  request authenticated as tenant A cannot SELECT, UPDATE, or
  DELETE a session row created by tenant B — engine-side, not
  app-side. This is the same template `backend/tests/cross_tenant_isolation.rs`
  uses today.
- **Step 5 ("the five endpoints") is the largest single step.**
  Split it: (5a) `/uploads` + session-store trait method; (5b)
  `/sign-part`; (5c) `/complete` including the CAS state
  transition (§1.2) and the canonical_id-conflict policy (§1.3);
  (5d) `DELETE` + `GET`. Each is independently testable; (5c)
  is the one the integration tests in §`Tests` actually exercise.

---

## 6. Summary

The design is right. The doc is roughly **70% of the precision** a
fresh agent needs to land it without re-deriving decisions. The gaps
that matter:

- §1.1 `upload_session` schema must use the
  PERMISSIONS/DEFAULT/ASSERT pattern.
- §1.2 atomic CAS on state transitions; pick a policy for
  DELETE-during-validate.
- §1.3 explicit policy for `canonical_id` collision at complete.
- §1.4 don't overpromise S3-enforced Content-Type / Content-Length.
- §1.5 specify Authed vs System DB; add upload-session methods to
  the storage trait.
- §1.6 validator must have a bounded download / memory budget, not
  just timeout + page count.
- §1.7 role gate on the new endpoints.

Everything else (§2, §3) is precision work that prevents foreseeable
churn but doesn't change the architecture. Once §1 is folded into
`ingestion-v2.md`, a fresh agent can build it directly off the doc
without needing to read the surrounding modules.
