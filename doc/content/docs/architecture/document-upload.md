---
title: Document Upload and Lifecycle
description: Event-sourced document upload — the API contract, why the work queue exists, concurrency and conflict handling, projections, and reclamation.
---

# Document Upload and Lifecycle

Documents are event-sourced. A NATS JetStream log is the source of truth;
Postgres is a projection rebuilt from it. The browser uploads bytes directly to
S3-compatible object storage, and a document comes into existence only once
those bytes have been assembled and scanned.

> **Status: implemented, and this page is the authority.** The design is live in
> `crates/document-domain`, `crates/document-app`, `crates/document-adapters`,
> `services/api-service`, and `services/document-worker`. Where the code settled
> somewhere different, see [Implementation Notes](#implementation-notes).
>
> `specs/document-lifecycle-implementation.md` is a **superseded** pre-build
> spec; it still describes presign-at-preflight, Postgres `upload_attempt` rows,
> and a blob GC, none of which exist. It carries a banner listing the
> divergences and is kept as history.

```d2
direction: right

Browser: "Browser\nReact + Uppy"
API: "api-service\ncommands + queries"
Worker: "document-worker\nupload + projection"
Log: {
  label: "JetStream DOCUMENT_EVENTS\nLimits retention, kept forever"
  shape: cylinder
}
Work: "JetStream DOCUMENT_WORK\nWorkQueue, deleted on ack"
KV: {
  label: "NATS KV UPLOAD_STATE\nthe whole upload, UPLOAD_TTL"
  shape: cylinder
}
S3: {
  label: "S3 / MinIO\nimmutable blobs"
  shape: cylinder
}
PG: {
  label: "Postgres\nprojection"
  shape: cylinder
}

Browser -> API: "preflight, complete, poll"
Browser -> S3: "PUT parts directly"
API -> KV: "create, then CAS the status"
API -> Work: "publish UploadCompleted"
API -> PG: "read projection"
Work -> Worker: "pull command"
Worker -> S3: "assemble, validate, scan"
Worker -> KV: "read, then CAS the outcome"
Worker -> Log: "append one event"
Log -> Worker: "projection loop folds"
Worker -> PG: "upsert row + checkpoint"
```

## State Ownership

| State | Owner | Rebuildable |
| --- | --- | --- |
| Document truth and history | `DOCUMENT_EVENTS` | no — this is the source |
| Current rows, versions, states | Postgres projection | yes, from the log |
| Document bytes | S3, immutable per upload | no |
| **The whole upload**, parameters and status | NATS KV `UPLOAD_STATE` | no — transient by design |
| Upload work items | `DOCUMENT_WORK` | no — deleted on ack |

**Postgres holds no upload state.** It is the document projection and nothing
else, which restores a property the schema had lost: *every table here can be
rebuilt by replaying the log.* `upload_attempt` was the one exception — a
rejected upload appends no event, so the row was the sole record of its own
outcome — and it is gone (migration `0007`).

Two retention policies, deliberately:

- **`Limits`** for `DOCUMENT_EVENTS`, with effectively infinite `max_age`. Acking
  never deletes, so every projection keeps its own cursor and any projection can
  be rebuilt from zero.
- **`WorkQueue`** for `DOCUMENT_WORK`. Acking deletes. A command is an
  indestructible work item until its consequences are durable.

## Why the Work Queue Exists

`/complete` does not finish the upload. It publishes a command and returns `202`.

This is not merely about keeping the request fast. Assembling the S3 object is an
**irreversible external side effect**, and it must be bracketed by a durable
record created before it and discharged only after its consequences are recorded.
The unacked work item is that record.

Without it, a crash between `CompleteMultipartUpload` and the event append would
leave a materialised object that nothing references and nothing will ever retry.
That orphan is permanent: it is no longer an *incomplete* multipart, so the
incomplete-upload reaper cannot see it, and no sweeper looks at assembled
objects. The redelivered work item is the only thing that ever comes back for it.

That gives the governing rule for the whole flow:

> **Ack after append, never before.**

## No Pending States in the Log

An upload in flight produces **no event**. The document is unchanged until
validated bytes exist, and only then does a single event appear.

The alternative — appending an `uploading` event up front, with per-version
states flipping `uploading → validating → ready` — was considered and rejected.
It buys cross-session visibility and a claimed version ordinal, and it costs:

- A per-version state machine in the fold.
- An expiry sweeper plus `VersionExpired` events for abandoned uploads.
- Permanent junk events for uploads that never completed.
- A projection field pointing at **unscanned bytes**, which is exactly the hazard
  this design otherwise eliminates by construction.

The claimed ordinal also does not survive scrutiny. If claim order is enforced, a
stalled uploader blocks everyone behind it until its timeout fires — head-of-line
blocking that scales with file size. If it is not enforced, the promised ordinal
was never a commitment.

The discriminator: **an in-flight upload is a property of the upload, not of the
document.** That is now literal — the upload lives in its own store, with its own
lifetime, and the document only learns about it when an event is appended.

The cross-session visibility this alternative would have bought is simply not
offered. See [the TTL section](#the-ttl-is-the-only-cleanup).

## Concurrency: Last Write Wins, Loudly

Validation is expensive — a full scan of a large object. When two users upload to
the same document concurrently, discarding the loser's work would waste that
effort for no benefit, since both blobs are valid.

So both apply, in log order:

```text
two uploads → both validate → both append
  first  wins the CAS at seq N
  second retries, appends at N+1, becomes current
  the first blob is now unreferenced, and is kept
```

The failure mode this creates is *surprise*, not data loss — the superseded
version is still in history and can be reverted to. Surprise is fixed with
notification rather than with locking:

- `POST /complete` carries `if_match`, the version the uploader was looking at.
- The worker records `based_on_version` on the event when it does not match.
- The status endpoint reports `superseded: true`, so the UI can say *"your upload
  replaced a newer version"*.
- A client that prefers to fail instead can send `on_conflict: "fail"`.

There is deliberately **no pre-flight warning** that someone else is already
uploading to the same document. Answering that means querying uploads across
users, and an upload is now a user-scoped KV record; the query has nowhere to
run. The clash is still detected — after the fact, by `if_match`, and reported
as `superseded`.

## Identity and Versioning

| Identifier | Minted by | Purpose |
| --- | --- | --- |
| `document_id` | server, ULID, at preflight | the aggregate; never changes |
| `upload_id` | server, ULID, per attempt | identifies one upload; **is** the object path |
| `version` | the writer, in the event payload | dense, user-visible; sent back as the `if_match` body field |
| `stream_seq` | JetStream | sparse CAS token, internal only |

`version` is dense and increments by exactly one per user-visible mutation.
`stream_seq` increments on every event, including facts that leave `version`
unchanged. Both live on the projection row; only `version` is exposed.

Identity precedes existence. `document_id` is minted at preflight and returned
immediately, long before any event exists — it is an *input* to `DocumentCreated`,
not a product of it. That is what lets worker redelivery converge on one document
instead of many.

## Object Storage

```text
tenants/<tenant_id>/blobs/<upload_id>/original
```

A pure function of `(tenant_id, upload_id)`. No URL is ever persisted; the
projection's `current_blob` holds the `upload_id` and every consumer derives the
path. Objects are immutable and written exactly once — nothing is ever copied or
moved, so there is no quarantine copy to pay for.

Unvalidated bytes sit at a key **no projection field references**, so they are
unreachable by construction rather than by convention. Promotion is a single
column change in the fold, not a byte movement.

`upload_id` is a ULID: 48 bits of timestamp plus **80 bits of randomness**, so the
path is not guessable even knowing when the upload happened. Two conditions make
that hold: generate with `Ulid::new()` rather than a monotonic generator, which
would increment the random component within a millisecond and make neighbours
derivable from one known id; and keep the bucket private with no endpoint ever
presigning a caller-supplied key.

Note the accepted trade-off: `upload_id` also appears in API responses, URL paths,
and logs, so anything logging one has logged the object location. That matters
only if the two protections above fail.

### Presigned URLs are bearer capabilities

The object key is **cleartext in the URL path**; only the signature is hashed, and
it obscures nothing:

```text
https://<public-endpoint>/<bucket>/tenants/<tenant>/blobs/<upload_id>/original
  ?uploadId=…&partNumber=1                     ← cleartext
  &X-Amz-Credential=<access-key-id>/…          ← access key ID, cleartext
  &X-Amz-Expires=300
  &X-Amz-Signature=<HMAC-SHA256 of the canonical request>
```

The signature authorises exactly that method, key, and part number, and cannot be
extended or repurposed. But anyone holding the URL can write that part until it
expires — with *arbitrary* content, since presigned PUTs use `UNSIGNED-PAYLOAD`.
Hence a short part TTL, TLS, and never logging a presigned URL. "Public endpoint"
means browser-reachable, not publicly readable.

## The Upload State

One NATS KV record is the **entire** upload:

```text
UPLOAD_STATE   key <tenant_id>/<user_id>/<upload_id>   max_age = UPLOAD_TTL
```

```text
preflight ─ uploading ─ /complete ─ scanning ─ worker ─ accepted | rejected
            └──────────────── one KV record, TTL-bounded ──────────────┘
                                                           └ doc event ┘
```

It carries `document_id`, `mode`, `storage_key`, `multipart_upload_id`, the
declared filename, content type and size, the part geometry — and the status.

That record is the crossover point in the whole design. Everything before the
event is **user-scoped and temporary**; everything after it is **tenant-scoped
and permanent**. The single event the worker appends is where an upload stops
being a private, disposable thing and becomes a document the tenant owns.

Deliberately not stored: presigned URLs (bearer capabilities that expire —
regenerate, never store), part ETags (they arrive in the `/complete` body), and
the metadata patch (supplied at `/complete`).

### Create-once, then compare-and-swap

The parameters half is written once and never rewritten: nothing may change an
upload's geometry after the client has started slicing to it. Only `status`
moves, and only through a CAS on the record's revision, because **two writers
reach it and they are not ordered**:

- `/complete` publishes the work item and *then* marks the record `scanning`.
- The worker marks it `accepted` or `rejected` as soon as it finishes.

A small file can finish inside that window, so the api-service's write would
land second. The rule is therefore that **a terminal status is final** — the
late `scanning` is dropped. Without it a finished upload reports `scanning`
forever, because nothing is left to advance it. The same rule keeps the *first*
reject reason, which is the one that says what actually went wrong.

### The TTL is the only cleanup

Nothing sweeps this bucket, and nothing needs to. `max_age` retires the record,
and that is the whole retention story — which is why the worker's third
leader-elected task, its sweeper, and two config knobs no longer exist.

The record and the incomplete multipart expire on the **same** configured
value, and neither has to go first — see
[One number for the upload's lifetime](#one-number-for-the-uploads-lifetime).

Two things are given up for this, and both are deliberate:

- **There is no history of finished uploads.** After the window `GET
  /api/uploads/{id}` is a `404`. A rejection appends no event, so past that
  point the only evidence is the document — if it got that far.
- **Nobody can ask "who else is uploading to this document?"** That is a
  cross-user query over a user-scoped keyspace. `uploads_in_progress[]` is gone
  from `GET /api/documents/{id}`, and serving it was the only thing that had
  ever kept upload state in Postgres.

### When the record is gone before the work item runs

The worker reads the record, and a miss means the TTL won. Its job is then to
clean up and give up: abort the multipart, delete the object, reject with
`upload_expired`, and ack.

**With one check that is not optional.** "No record, so delete the object"
destroys live documents. A successful upload whose ack was lost, redelivered
after the TTL, also has no record — and its bytes are what a document is
serving. So the event log is consulted first and is authoritative: if this
`upload_id` already appears as a `blob_ref` anywhere in the document's history,
the upload succeeded, and the worker acks without touching storage. The search
is over the whole history rather than the head, because a later upload may
already have superseded this one.

A failed log read never deletes either — it retries.

## API Contract

```text
POST  /api/uploads                        preflight → ids and geometry
GET   /api/uploads/{upload_id}/parts      what storage already holds → resume
POST  /api/uploads/{upload_id}/renew      sign parts
POST  /api/uploads/{upload_id}/complete   202, work queued
GET   /api/uploads/{upload_id}            uploading | scanning | accepted | rejected
GET   /api/documents/{document_id}        404 until the first blob is validated
GET   /api/documents?limit=&cursor=       keyset page, newest first
```

All writes require the realm role `ingester`; `owner` is a composite that
includes it.

**Reads are tenant-scoped.** A document belongs to the tenant, not to whoever
uploaded it, so every member may read every document — by id and in listings
alike. `owner_user_id` is retained as provenance, never as an access predicate.
The two once disagreed, `get` being tenant-scoped while `list` filtered by
owner, which made a document readable by id yet invisible in its own tenant's
listing.

Upload *attempts* stay owner-scoped: `GET /api/uploads/{upload_id}` is a record
of an operation you performed, not of a document the tenant owns, and another
user gets `404` rather than `403` so nothing about its existence leaks.

### POST /api/uploads

```jsonc
{
  "document_id": "01JZ…",     // omit = create, present = replace
  "filename": "annual-report.pdf",
  "size": 734003200,
  "content_type": "application/pdf"
}
→ 201 { upload_id, document_id, key, part_size_bytes, part_count }
```

**Preflight presigns nothing.** Clients sign each part immediately before
uploading it, so a batch minted here would mostly expire unused — and computing
it costs an HMAC apiece, up to 10 000 of them, on a request the user is waiting
on before a single byte moves.

`size` is capped at `DELPHI_DOCUMENT_MAX_UPLOAD_BYTES`, checked before the
multipart is opened so an oversized declaration costs nothing — `413`. S3's own
5 TiB ceiling is not a policy, it is the absence of one: every later stage reads
the whole object, so the real bound is what one worker can stream past a scanner
inside the redelivery window. The declaration is only a promise here; the `HEAD`
after assembly is what enforces it against the bytes that actually arrived.

Replace mode authorises the target **here** — `404` missing, `403` tenant
mismatch, `409` deleted. That is the point of naming the document at preflight:
otherwise a user uploads 400 MB and only then learns they cannot write it.

No metadata at this step. It is supplied at `/complete`, because the user may
edit the title during a long upload and the later value should win. It still
lands in the same event as the blob, so atomicity with the promotion is
preserved.

### Part geometry

Server-owned, and the client MUST slice at exactly what it returns:

```text
reject if size == 0                     -> 400
reject if size > MAX_UPLOAD_BYTES       -> 413, before anything else happens

part_size  = max(PART_SIZE_BYTES, ceil(size / 10_000))
part_count = ceil(size / part_size)
part N     = [ (N-1)·part_size , min(N·part_size, size) )
```

The size check is first and unconditional: an over-cap declaration is refused
before a multipart is opened, so it costs nothing. `declared_size` is a promise,
and the `HEAD` after assembly is what holds the upload to it.

The client MUST call this as a **preflight, before constructing its uploader**.
Uppy fixes chunk boundaries in the `MultipartUploader` constructor — before the
`createMultipartUpload` hook runs — so a part size fetched inside that hook
arrives after the file has already been sliced. The server guarantees
`ceil(size / part_size) ≤ 10 000`, so Uppy's own clamp can never fire and
re-slice the file underneath a presigned set.

`ceil` is used in both places deliberately. A `floor`/`ceil` mismatch between
server and client is precisely how the last part goes missing.

#### Count scales first, size only when forced

The formula has two regimes, and the `max()` is what switches between them:

| | part size | part count |
| --- | --- | --- |
| up to `PART_SIZE_BYTES × 10 000` | flat, as configured | grows, 1 → 10 000 |
| above it | grows | pinned at 10 000 |

At the shipped 20 MiB that boundary is **200 GiB**.

That ordering is deliberate, and it is the opposite of the intuitive one.
Growing the part size for large files sounds more efficient, but **retry cost is
highest exactly where size-first scaling hurts most**: big files take longer, so
they are the ones most likely to meet a network blip, and a bigger part size
makes the unit lost to that blip bigger for precisely those files.

The usual argument for growing the part size instead is round-trip count, and
in this design that argument is weak: signing is batched, so a client can sign a
whole upload in one request rather than one per part. The other count-driven
costs stay comfortable too — a 1 639-part `/complete` body is about 82 KB
against a 7 MiB ceiling, and `ListParts` pages transparently at 1 000.

#### Where the numbers land

| file | parts |
| --- | --- |
| ≤ 20 MiB — the overwhelming majority of documents | **1** |
| 200 MiB | 10 |
| 1 GiB | 52 |
| 32 GiB (`MAX_UPLOAD_BYTES` as shipped) | 1 639 |
| 200 GiB | 10 000 — the size term engages here |

20 MiB is on the large side of the usual defaults (the AWS CLI and SDK transfer
managers use 8 MiB, Uppy's own default is `ceil(size / 10 000)` over a 5 MiB
floor), and that is on purpose: **it keeps the common case a single-part
upload** — no `ListParts`, no resume bookkeeping, a plain ETag. Lowering it
would not help documents under the threshold at all; it would only fragment the
tail.

The one reason to lower it is browser memory under concurrency, since `limit: 6`
puts up to six parts in flight. Blob slices are disk-backed and streamed rather
than held resident, but if mobile uploads start failing, 8–10 MiB is the first
thing to try.

#### Both halves are configuration, and startup says when they disagree

`PART_SIZE_BYTES` and `MAX_UPLOAD_BYTES` are set together, and their product
decides whether the configured part size actually applies:

```text
honoured up to  =  PART_SIZE_BYTES × 10 000
```

Nothing breaks past that point — the geometry grows the part size instead, and
the 10 000 cap still holds for every input. But the configured value has
silently stopped applying to the largest files the deployment allows, which is
the kind of thing an operator should be told rather than discover from a
surprising `part_size_bytes` in a preflight response. So **api-service logs an
error at startup** when `MAX_UPLOAD_BYTES > PART_SIZE_BYTES × 10 000`, naming
both values, the size up to which the configured one holds, and how many parts
the cap would otherwise need.

It is an error rather than a refusal because the configuration is still usable
— refusing to boot over a working setup would be worse than the surprise it
prevents.

`PART_SIZE_BYTES` is itself refused outright if it falls outside S3's own
range: below the 5 MiB floor every non-final part would be rejected by storage,
so no multi-part upload could ever complete. It is refused rather than quietly
raised, so a deployment cannot believe it uses a part size it does not.

#### The two S3 limits this defends against

| limit | value | how it is held |
| --- | --- | --- |
| parts per upload | 10 000 | the `ceil(size / 10_000)` term; `part_count` is `u16` |
| min part size, non-final | 5 MiB | `PART_SIZE_BYTES` is refused below it, at startup and in the geometry |
| max part size | 5 GiB | `PART_SIZE_BYTES` is refused above it; the grown size stays under it even at 5 TiB |
| max object | 5 TiB | `MAX_OBJECT_BYTES`, rejected at preflight |

The 5 MiB floor is deliberately **not** a term in the formula. Putting it there
would silently raise a too-small configured value into a working one; refusing
it instead means the mistake is visible. `no_input_can_produce_more_than_ten_thousand_parts`
sweeps the whole space — three part sizes against the boundaries of each — and
asserts both the part count and the part size stay legal.

The 10 000 cap is real and enforced server-side, not a client convention —
`storage_refuses_a_part_number_above_the_ten_thousand_cap` pins it against live
storage, because the entire second term exists to defend against that one
number:

```text
part 10000 -> 200 OK
part 10001 -> 400 InvalidArgument
              "Part number must be an integer between 1 and 10000, inclusive"
```

Note that *presigning* part 10 001 succeeds — signing is local SigV4 with no
validation — so without the geometry the failure would surface only at PUT time,
after real bytes had moved.

### GET /api/uploads/{upload_id}/parts

```jsonc
→ 200 { part_size_bytes, part_count,
        parts: [ { "part_number": 1, "etag": "\"a54357…\"", "size": 20971520 } ] }
```

What storage already holds, and the **resume half of the contract**. A part the
client did not upload in this pass has an ETag the client has never seen, and
`CompleteMultipartUpload` needs every ETag — so without this endpoint a client
can only ever restart. `410 Gone` once the multipart no longer exists.

Only parts whose length matches the geometry are reported. A part of any other
size cannot have come from a correct slicing of this file, so omitting it makes
the client re-upload rather than resume onto bytes that are wrong.

### POST /api/uploads/{upload_id}/renew

The part-signing endpoint. Two modes, which want opposite answers about parts
already in storage:

- **`from_part` given** — the client is *naming* the parts it wants URLs for.
  Honoured even for a part already uploaded: an uploader retrying a PUT whose
  response it never saw asks for exactly that, and refusing leaves it unable to
  finish. This path never touches storage — signing is local computation, and it
  runs once per part.
- **`from_part` omitted** — "where do I resume?". Consults `ListParts`, starts at
  the first gap, skips what is already stored, and returns `410 Gone` if the
  multipart is missing.

```jsonc
{ "from_part": 1, "count": 1 }
→ 200 { parts: [ { "part_number": 1, "url": "https://…", "expires_at": "…" } ] }
```

Geometry is not echoed — it was fixed at preflight and cannot change — and the
verb is not reported, because a part URL is always a `PUT`.

**`count` is unbounded — omit it and every remaining part is signed.** The
current client passes `count: 1`, but batching is a supported path, not a future
migration: a whole upload signs in one request and, on the `from_part` branch,
still costs no call to storage.

There is no batch cap because there is nothing left for one to protect. The part
count is already bounded by the geometry — `part_count ≤ 10 000` by
construction, and `MAX_UPLOAD_BYTES / part_size` is far tighter in practice — so
a separate limit was only a smaller copy of a bound that already held.

Signing is local computation, measured at **~67 µs and ~578 bytes per part**. At
the configured 32 GiB cap that is 1 639 parts in ~110 ms and ~950 KB; at the
geometric ceiling of 10 000 parts it is ~670 ms and ~5.8 MB. And the batch is
*cheaper for the server* than the equivalent stream of single-part calls: it
verifies the caller and reads the upload record **once**, where N calls do both
N times.

`expires_at` is carried per part for exactly that caller — one signing a window
has to know when its URLs go stale, whereas a client signing just before
uploading never looks.

This is why the part URL TTL stays at 300 seconds while the upload window is 24
hours: a URL is used seconds after it is minted. An earlier design tied the two
together and capped any single upload at roughly five minutes.

### POST /api/uploads/{upload_id}/complete

```jsonc
{
  "if_match": 5,
  "on_conflict": "supersede",     // default; or "fail"
  "title": "Annual Report 2026",
  "tags": ["finance"]
}
→ 202 { "state": "scanning" }
```

**No parts list.** The body carries intent only — the conflict policy and the
metadata patch. The worker asks storage what it holds via `ListParts` and
assembles that, in ascending part order.

The client used to echo back the `{part_number, etag}` pairs it had collected,
on the theory that they were a per-part concurrency token: "the parts I uploaded
are still the parts that are there." That token was always partly hollow. On any
*resumed* upload, some of those ETags came from our own `GET /parts` — the
client handed back values it had never observed, proving nothing about those
parts. And only someone holding a presigned URL for this upload could have
replaced a part in the first place, which is the same user in another tab.

So the round trip bought a guarantee it only sometimes provided, at the cost of
0.64 MiB of work item at the 10 000-part ceiling, ETag bookkeeping in the
browser, and a `413` path. S3 knows the answer authoritatively; the worker asks
it at the moment of use.

A `/complete` that arrives before anything was uploaded is rejected
`invalid_parts`; one that arrives with only some parts assembles a short object
and is rejected `size_mismatch` by the `HEAD` check — the same answer as before,
reached without the client asserting anything.

Publishes `UploadCompleted` with `Nats-Msg-Id = upload-completed:<tenant>:<upload_id>`
and awaits the `PubAck`. **Appends no event and touches no document.**
`document_id` is not accepted here — it was fixed at preflight, and accepting it
again would create a second source of truth.

Duplicate or parallel calls are safe without CAS, through three layers: JetStream
dedupes on the message id inside the duplicate window; outside it the worker is
idempotent, since `complete_multipart` returns `NoSuchUpload` and a `HEAD`
confirms the object; and the append-time CAS is the final arbiter.

Because the message id derives from the upload id alone, **the first part list
wins** — a second `/complete` with different ETags is deduped and ignored.

### GET /api/uploads/{upload_id}

```jsonc
{ "state": "uploading", "document_id": "…" }
{ "state": "scanning",  "document_id": "…" }
{ "state": "accepted",  "document_id": "…", "version": 7, "superseded": true }
{ "state": "rejected",  "document_id": "…", "reason": "malware_detected" }
```

Read straight off the KV record. `document_id` is known from preflight, so it is
reported at every stage — a client that lost its preflight response can still
find the document it is making.

`404` once the TTL elapses, and there is no archive behind it: ask inside the
window or not at all.

A `404` from `GET /api/documents/{id}` is expected until the event has been
**folded into the projection** — durable and readable are not the same instant. `accepted` and
`rejected` are both terminal — a `202` from `/complete` is not a guarantee.

**A terminal state is final and first-writer-wins** — see
[Create-once, then compare-and-swap](#create-once-then-compare-and-swap).

One reject reason is internal: `upload_expired` means the record was gone before
the work item ran, so there was nowhere to report it. It exists for the log line
rather than for a client, which by then has had a `404` from this endpoint.

### GET /api/documents

```jsonc
→ 200 { items: [ … ], next: "31373535…" | null }
```

Keyset pagination, newest first. `next` is **opaque**: pass it back as `?cursor=`
verbatim. A cursor the server did not mint is a `400`, never "start again" — a
paging client that silently restarted would loop forever.

Two things this shape is deliberately fixing:

- **The cursor is the whole ordering key, not a timestamp.** `updated_at` is not
  unique. When it was the cursor, a strict `updated_at < cursor` skipped every
  row that tied with the last row of the previous page — with five documents
  sharing a timestamp and `limit=2`, the listing showed two of them and then
  declared itself finished. The key is `(updated_at, document_id)`, compared as
  a row, and `document_tenant_page_idx` is ordered to match so the page is an
  index range scan. Keeping the encoding opaque is what lets that key change
  again without breaking clients.
- **`limit` is clamped where `next` is computed.** They used to live in different
  layers, so `limit=500` returned `MAX_LIST_LIMIT` items with `next: null` —
  indistinguishable from the end of the listing, and silent data loss for
  anything that asked for a big page. The service returns the page and its
  cursor together for that reason; the handler has nothing left to get wrong.

## Lifecycle

```d2
shape: sequence_diagram

Browser: Browser
API: api-service
KV: NATS KV UPLOAD_STATE
S3: S3 / MinIO
Work: JetStream DOCUMENT_WORK
Worker: document-worker
Log: JetStream DOCUMENT_EVENTS
PG: Postgres projection

Browser -> API: 1 POST /uploads preflight
API -> API: 2 authorize, check size cap, derive part size + count
API -> S3: 3 CreateMultipartUpload
API -> KV: 4 create upload state (uploading)
API -> Browser: 5 201 ids + geometry (no URLs)

Browser -> Browser: 6 slice at part_size_bytes
Browser -> API: 7 GET /parts, skip what storage holds

For each remaining part: {
  Browser -> API: 8 POST /renew, sign this part
  Browser -> S3: 9 PUT part
  S3 -> Browser: 9 ETag
}

Browser -> API: 10 POST /complete with parts + metadata
API -> KV: 11 read state
API -> Work: 12 publish UploadCompleted
API -> KV: 13 CAS -> scanning (dropped if already terminal)
API -> Browser: 14 202 scanning

Work -> Worker: 15 deliver command, ACK pending
Worker -> KV: 16 read state; expired or terminal short-circuits here
Worker -> S3: 17 ListParts, then CompleteMultipartUpload
Worker -> S3: 18 HEAD, size must match declared
Worker -> S3: 19 ranged GET of the first 512 bytes, sniff
Worker -> S3: 20 full-stream scan + sha256
Worker -> Log: 21 append DocumentCreated
Worker -> KV: 22 CAS -> accepted
Worker -> Work: 23 ACK

Log -> Worker: 24 projection loop folds
Worker -> PG: 25 upsert row + checkpoint in one transaction
```

Steps 17–21 all run inside one unacked work item. Four kinds of durable write
precede the worker — the multipart handle, the part bytes, the KV record, and
the work item — **none of them events**. The worker emits exactly one event, or
none at all if it rejects. Step 25 is the first and only time Postgres is
written.

Steps 13 and 22 race, and 13 is the one that must lose: see
[the CAS rule](#create-once-then-compare-and-swap). Step 16 is what makes an
expired record safe and a redelivered-after-success item cheap — a terminal
record is replayed rather than re-scanned.

One worker handles `DELPHI_DOCUMENT_WORK_CONCURRENCY` items at once. Steps 17–21
are almost entirely waiting on storage — and step 20 streams the whole object —
so running them one at a time let a single large file block every small upload
behind it on that instance. The semaphore is taken before the next message is
pulled, so backpressure reaches JetStream instead of accumulating unacked work.

## Validation

Three classes, split by whether the user is still waiting:

| Class | Examples | Where | On failure |
| --- | --- | --- | --- |
| Upload liveness | the record still exists | worker, before anything else | reclaim the bytes, `upload_expired` |
| Metadata | title length, tag shape | command handler, before append | `400`, nothing happened |
| Cheap object | size matches declared, magic bytes | worker, after assembly, before the event | delete object, record failure, **no event** |
| Expensive | virus scan, deep parse | worker (scan) / pipeline (later stages) | scan → no event; later stages → `DocumentStageFailed` |

> **If the check finishes fast enough that the upload flow can still report it,
> reject before the first event. If the user has moved on, create the document and
> let it fail visibly.**

The scan sits **before extraction**, always. An extractor parses untrusted input,
so running it on unscanned bytes hands an attacker code execution inside the
worker.

The `HEAD` after assembly is also where `size` stops being an unverified client
assertion.

## Events

The worker produces `DocumentCreated` (create) or `DocumentBlobValidated`
(replace), and nothing else in this slice. The fold handles the full catalogue so
the projection is complete when later producers arrive:

| Event | Bumps `version`? | Produced by |
| --- | --- | --- |
| `DocumentCreated` | yes | upload worker |
| `DocumentBlobValidated` | yes | upload worker |
| `DocumentMetadataChanged` | yes | `PATCH`, later |
| `DocumentReverted` | yes | revert command, later |
| `DocumentDeleted` | yes | delete command, later |
| `DocumentTextExtracted` | no | extraction stage, later |
| `DocumentIndexed` | no | indexing stage, later |
| `DocumentStageFailed` | no | any pipeline stage, later |
| `DocumentBlobPruned` | no | retention, later |

Events that do not bump `version` record a fact *about* that version. Full
attribute definitions are in the implementation spec.

Two rules that make append idempotent: `event_id` must be **deterministic**,
derived from the command rather than freshly generated, or `Nats-Msg-Id` dedupe
silently does nothing; and a `Conflict` on a create means a previous delivery
already succeeded, so it must be treated as success rather than retried forever.

## Projections

**Projectors fold. They never judge.** A projector may fail only for
infrastructure reasons — a business rejection would make the projection's state
depend on which events happened to fail, and replay would stop being
deterministic.

The projection loop runs as a leader-elected task inside `document-worker`,
alongside the work-queue consumer. The consumer scales horizontally with
competing consumers; the projection must be single-writer, so instances contend
for a Postgres advisory lock and non-leaders stand by to take over.

Rows and checkpoint advance in **one transaction**, which is what makes the
projection exactly-once without idempotency guards — though the guards stay,
because they cost nothing and cover a misconfigured lock.

Poison events are parked per key: written to `projection_failure`, checkpoint
advanced, alert raised, and the affected document surfaced in an error state.
Because the projection is keyed per document, a hole affects one document rather
than freezing the read model.

The schema is deliberately permissive — `text` not `varchar(n)`, no `CHECK`
constraints, no foreign keys, `jsonb` for anything unqueried. A projection that
cannot reject an event cannot be poisoned by one.

## Reclamation

**Blobs are kept.** There is no unreferenced-blob sweeper. A superseded
version's bytes are the only copy of that version, and reclaiming them on a
timer would quietly make the version history metadata-only: the log would list
versions whose content no longer exists.

Retiring old versions is a **retention policy**, not garbage collection. It has
to name the blob it retires and record that in the log via `DocumentBlobPruned`,
so it belongs at blob-update time rather than in a sweep. That is designed
separately; until then storage grows with every replace, which is the intended
trade.

**Nothing in this codebase sweeps anything any more.** Everything expires, or is
reclaimed inline by the code that created it:

| what | who reclaims it | how |
|---|---|---|
| upload records | NATS | KV `max_age` = `UPLOAD_TTL` |
| work items | JetStream | deleted on ack — and on term, which is the same thing here |
| **incomplete** multiparts | `minio-gc` sidecar | `mc rm --incomplete --older-than $UPLOAD_TTL` |
| assembled objects that never became a document | the worker | reject and expiry paths, inline |
| assembled objects that did | nobody — kept | version retention, later |

The one leader-elected sweeper this design used to run existed solely to age out
Postgres upload rows. Those are gone, and so is it.

`minio-gc` cannot see an assembled object and the worker's paths cannot see an
incomplete multipart, so the two never contend: `ListObjectsV2` does not report
incomplete multipart uploads, and `mc rm --incomplete` does not touch objects.

Neither is the primary path. The reject path aborts and deletes immediately,
`unwind_preflight` aborts on a failed preflight, and the expiry path reclaims
what a dead record left behind. The sidecar only catches what crashed.

Bounded redelivery is what makes that work. `max_deliver` is finite and refused
at zero, because the **final delivery is what converts a transient failure into
a rejection** — and the rejection is what aborts the multipart and deletes the
object. With unlimited retries an upload that never succeeds is also never
cleaned up.

## Configuration

Every variable is `DELPHI_DOCUMENT_<NAME>` and **required**. Missing
configuration fails at startup rather than defaulting into a surprising
production behaviour.

| Variable | Compose value | What it bounds |
| --- | --- | --- |
| `UPLOAD_TTL_SECS` | 86 400 (24h) | how long an upload exists — **api-service and the reaper only** |
| `PART_URL_TTL_SECS` | 300 | presigned part URL expiry (api-service only; capped at 1h) |
| `MAX_UPLOAD_BYTES` | 32 GiB | largest declarable file; refused at preflight with `413` |
| `PART_SIZE_BYTES` | 20 MiB | [slice size](#part-geometry) while the part cap is not binding; must be 5 MiB–5 GiB |

The `/complete` work item has **no size ceiling of its own** — only NATS's 8 MiB
`max_payload`. There used to be a 4 MiB check, and removing it is worth
recording, because it was the second-order cost of a decision made elsewhere:

- While the client's parts list rode on the command, the worst case was
  0.68 MiB — 0.64 MiB of it parts — so a guard was reasonable.
- It cost a **full extra serialisation of every command**: once to measure,
  discarded, then again to publish.
- With the parts list gone the worst case is ~40 KiB, two orders of magnitude
  under the transport limit, and what bounds it is `validate_metadata_patch`
  running three lines earlier.

So the regression actually worth catching is "somebody raised a metadata limit",
and `the_largest_valid_command_is_far_under_the_transport_limit` catches that at
build time — asserting a 10× margin, not merely that it fits — instead of paying
for it on every upload.
| `ACK_WAIT_SECS` | 120 | delivery deadline, extended by progress heartbeats |
| `MAX_DELIVER` | 5 | deliveries before the item is rejected and reaped |
| `MAX_ACK_PENDING` | 64 | JetStream's cap on unacked items per consumer |
| `WORK_CONCURRENCY` | 8 | uploads one worker finishes at once |
| `PROJECTOR_LOCK_ID` | 7243178541 | Postgres advisory lock electing the single projection writer |
| `PROJECTOR_ELECTION_SECS` | 15 | how often a stand-by contends for the lease |
| `PROJECTION_BATCH` | 500 | events folded per transaction |

No variable is constrained against another any more. Two are bounded
absolutely, and startup refuses a value outside the bound:

- `PART_URL_TTL ≤ 1h`. A part URL is a bearer capability anyone holding it can
  PUT arbitrary bytes through, so the hazard is someone sitting on it — not that
  it might outlive the upload, which merely makes it useless. The old check was
  relative to `UPLOAD_TTL`, and running it was the last reason api-service
  needed to know a number that belongs to the topology.
- `1 ≤ WORK_CONCURRENCY ≤ MAX_ACK_PENDING`. Above that cap the extra slots can
  never be filled — JetStream simply stops delivering — so the number would only
  mislead.

That list used to be twice as long. The rest were removed rather than relaxed,
because **a constraint that no longer holds is worse than none** — it reads like
it is protecting something. Every one of them existed to reconcile two variables
that no longer needed to agree.

### One number, and only its two enforcers hold it

An upload is held in two places: a KV record, and an incomplete multipart in
object storage. Both expire on `DELPHI_DOCUMENT_UPLOAD_TTL_SECS`, declared once
and referenced by exactly the two components that act on it:

| | holds `UPLOAD_TTL` | why |
|---|---|---|
| `api-service` | yes | declares the KV bucket's `max_age` |
| `minio-gc` | yes | aborts incomplete multiparts older than it |
| `document-worker` | **no** | binds to the topology; declares nothing |

`mc` takes a bare `<n>s` duration, so the two no longer disagree about units. In
compose that is a YAML anchor; in k8s it is one ConfigMap key and two
`configMapKeyRef`s.

**api-service is the topology's author** because it is the writer that creates
upload records — owning the bucket's lifetime belongs with the thing that puts
entries in it. Everything else calls `bind_upload_state`, which can only
`get_key_value`; a non-author physically cannot express an opinion about the
topology, which is *why* it no longer needs the numbers describing one. A
missing bucket is a hard startup failure rather than a silent create, because
quietly creating one with defaults is how drift returns.

That rule was learned the hard way. Every service used to assert the topology at
startup, so each carried its own copy of the TTL. Harmless under
`get_or_create` — the second asserter was a no-op — but that same no-op is what
let the running config drift away from the code. Switching to
`create_or_update` fixed the drift and replaced it with a worse one: the bucket
took whichever service restarted last, measured flipping `1d → 1h → 1d`.
Multiple `api-service` replicas are fine, because they share one config and the
write is idempotent.

#### Why not a per-entry TTL

NATS supports one (`Nats-TTL`, server 2.11+; we run 2.14.1), and it looks like
the tidier answer — the TTL would belong to the record rather than the bucket.
It does not work here: `update()` sends no TTL header, so a compare-and-swap
does **not** inherit it, and with `history: 1` the surviving revision would be
the untagged one. The record would stop expiring the moment its status changed.

Worse for this purpose, the worker also writes the record — it CASes the
terminal status — so a per-entry TTL would put the value in *both* services.
Bucket-level `max_age` keeps it in one.

They are the same number because they are the same concept, **not** because
either depends on the other. Whichever expires first, the other side cleans up
after it:

| what expires first | what happens | artefacts |
|---|---|---|
| the KV record | the worker's expiry path aborts the multipart and rejects `upload_expired` | none |
| the multipart | the worker rejects `multipart_lost` into the surviving record | none |
| both, before `/complete` | `404`; the reaper takes the parts | none |
| neither — the object is already assembled | the pipeline finishes; a dead record only costs the status | none |

There was an ordering rule here (the multipart had to die first, so the record
survived to say *why*). It is gone. All it ever bought was **which of two
failure messages the client saw** — and the client's next move is a fresh upload
either way — while costing the one constraint no process could check, spanning
two systems in two different units. That is a bad trade in both directions.

### Why the work stream never expires

`DOCUMENT_WORK` has `max_age: 0`. **`max_deliver` bounds behaviour; `max_age`
only bounds storage.** After its final delivery an item is never handed out
again, so it cannot come back later and reclaim something — keeping it costs
bytes, not correctness.

A finite value was once justified by "a Termed message would otherwise live
forever". That is untrue on this server: TERM removes a message from a
WorkQueue stream exactly as ACK does. Measured, not assumed.

What an infinite age does retain is the single case of an item that exhausted
`max_deliver` **without ever being acked** — which requires the process to die
mid-handler on every delivery. That is a poison item worth finding rather than
garbage worth collecting, it is invisible to consumers (`num_pending` does not
count it), and clearing it is a manual purge. `MAX_DELIVER` is refused at zero
for the same reason it always was, and it now carries the whole weight: the
final delivery is the only thing that turns a stuck upload into a rejection, and
rejection is the only thing that deletes its bytes.

Topology is reconciled, not merely created: streams, the consumer, and the KV
bucket all go through `create_or_update`. `get_or_create` returns whatever is
already there, so a changed `ack_wait`, `max_deliver`, or `max_age` would apply
on a fresh deployment and be silently ignored everywhere that had already run —
the worst kind of drift, because the code and the running system disagree with
nothing to show for it. Subjects and retention policy still cannot change in
place; either needs a rebuild.

## Implementation Notes

The design landed essentially as written. These are the places the code settled
somewhere slightly different, and why.

- **The projection's monotonic guard is on `stream_seq`, not `version`.** Several
  event types deliberately repeat the version — `DocumentIndexed`,
  `DocumentTextExtracted`, `DocumentStageFailed`, `DocumentBlobPruned` all record
  a fact *about* the current version. A `WHERE version < :version` guard would
  therefore discard every one of them. `stream_seq` is strictly increasing across
  the whole log, so it is the guard that actually holds.
- **`AttemptStore::set_status` carries the whole row.** The upsert has to be able
  to INSERT, not only UPDATE: the reject path and the final-delivery path can each
  run more than once, and a redelivery can reach the worker before preflight's row
  is visible. A `(tenant, upload_id, status)` signature could not have inserted
  the NOT NULL columns. Its conflict branch carries
  `WHERE status NOT IN ('accepted','rejected')` — the terminal-state rule above.
- **The content sniff is a ranged GET, not `open_read().take(512)`.** Taking the
  first bytes off a full-object GET makes storage begin streaming the whole body
  and then abandons the connection; for the multi-gigabyte objects this pipeline
  exists for, that is the difference between one small request and a cancelled
  large one. `BlobStore` has a separate `read_prefix` for it, so the port says
  which of the two reads a caller wants.
- **The work-queue consumer runs items concurrently.** It used to await each
  handler in the receive loop, so one instance finished one upload at a time no
  matter what `max_ack_pending` allowed — and finishing an upload is nearly all
  waiting on storage, including a full-object scan. A semaphore of
  `WORK_CONCURRENCY` permits, acquired before the next message is pulled, keeps
  the backpressure in JetStream rather than in unacked work here.
- **There is no inverse of `object_key`.** `parse_object_key` existed only for the
  blob sweeper, which listed the bucket and asked "whose is this?". Blobs are kept
  now, so every key is reached from a record that already knows both halves, and
  keeping a parser for a format nothing parses is how the two drift apart.
- **The reject path aborts the multipart as well as deleting the object.** If
  `complete_multipart` never succeeded, the multipart is still open and deleting
  the object alone would leave it for MinIO's reaper. Both calls are best-effort
  and idempotent.
- **The parts list is read from storage, not accepted from the client.** S3
  requires ascending, unique part numbers; the worker sorts what `ListParts`
  returns, so there is no client-supplied ordering left to validate. This
  replaced an API-side `validate_parts` and the `413` guard that bounded the
  list's size.
- **`UploadStateStore` has a `delete()`,** so a preflight that fails after
  writing the record can unwind it. Without that, a stale record could outlive
  the multipart it points at.
- **A KV tombstone is not a record.** A deleted or purged key still comes back
  from `entry()`, with an empty value and a non-`Put` operation. Reading it as a
  record decodes garbage, so the adapter checks the operation.
- **A lost CAS and a vanished key are the same JetStream error.** Both surface as
  a wrong-last-revision failure, but one retries and the other cleans up
  storage, so the adapter re-reads to tell them apart.
- **A `Principal` newtype does the identifier validation.** `tenant_id` and
  `user_id` become NATS key segments, and `tenant_id` also becomes a subject
  token, where `.`, `*`, or `>` would corrupt the subject space. Constructing the
  principal *is* the check, so no handler can be reached with an unsafe one.
- **NATS `max_payload` is set from a config file.** `nats-server` has no CLI flag
  for it, so the whole server config moved to `ops/nats/nats.conf`. A full
  10 000-part command measures roughly 1 MB, comfortably under the 8 MB ceiling —
  which means the API's own `413` guard is not reachable with realistic input, and
  that is the intended margin rather than an accident.
- **Uppy's `completeMultipartUpload` hook returns `{}` and reports nothing.** It
  still collects ETags internally — that is how its own resume bookkeeping works
  — but they never leave the browser. Letting the browser call S3's
  `CompleteMultipartUpload` directly would bypass the work item that makes the
  pipeline crash-safe.
- **Every pass takes Uppy's *restore* path.** `MultipartUploader.start()` only
  calls `listParts` when the file already carries `s3Multipart: { uploadId, key }`
  — otherwise it calls `createMultipartUpload` and uploads every chunk blind,
  which is a restart, not a resume. Both ids exist from preflight, so the file is
  seeded with them and `createMultipartUpload` is never reached. A hook can be
  implemented perfectly and still never run; this is the option that arms it.
- **Retry is resume.** A failure that outlasts Uppy's own five per-chunk attempts
  re-enters with a fresh Uppy instance and the same `upload_id`; `listParts` then
  skips whatever landed. `abortMultipartUpload` is deliberately a no-op — aborting
  would destroy the multipart the next pass resumes from, and an upload nobody
  ever finishes stays an *incomplete* multipart, which storage's own reaper
  clears.
- **Resume does not survive a page reload.** `upload_id` and `key` live only in
  the uploader's closure. Everything server-side is ready for it; persisting them
  client-side is not done.
- **`migrations/0002_drop_upload_session.sql` now actually runs.** It was dead
  before: the old code `include_str!`-ed `0001` alone and could not enumerate a
  directory. The ordered runner in `document-adapters` replaced that and covers
  chat's schema too.

### What was removed

The pre-event-sourcing document path is gone rather than adapted: the upload saga
and its KV bucket in `crates/nats`, `IngestionRepository` in `crates/storage`, the
upload and ingestion types in `crates/contracts`, and the `/api/ingestion/uploads*`
handlers. The `ingestion_job` and `outbox_event` tables are dropped by migration
`0003`. Chat is untouched.

## Open Decisions

- **Event store.** JetStream provides everything this design needs, including
  per-aggregate CAS via `Nats-Expected-Last-Subject-Sequence`. A dedicated event
  store becomes worthwhile only if measured subject cardinality, per-replica
  memory, or stream recovery time become constraints. Building behind an
  `EventStore` trait keeps that swap cheap.
- **User-visible failed uploads.** Rejected uploads currently leave no document at
  all, only an operational record. If failed uploads should appear in the library
  with a diagnostic, that requires a document to exist before validation — which
  reopens the pending-state question above.
- **Per-document ACLs.** Authorisation is currently tenant plus role. The
  preflight check is the single place to extend.
- **Version retention, and with it content revert.** Superseded blobs are kept
  indefinitely, so the bytes for every version still exist — but nothing prunes
  them either, and `DocumentReverted` carries no `blob_ref`, so revert restores
  metadata only. The planned mechanism enforces retention at write time:
  when the worker appends a new blob version it prunes the oldest beyond the
  retention count, deletes that object, and appends `DocumentBlobPruned` — the
  producer that event type is reserved for. The replace path already folds the
  document's full history for its redelivery guard, so the blob list is available
  at exactly the right moment. Retention is bounded by version count rather than
  age, so storage grows with `documents × N` rather than with time.

  Until it lands, storage grows with every replace and nothing reclaims it.
