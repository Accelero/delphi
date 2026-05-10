# Delphi — Discovery Feed

The first user-facing surface of the SPEC's Discovery pillar. Renders
the documents that source adapters have surfaced (and that filters
have accepted) as a reverse-chronological infinite-scroll feed with
live updates.

Sister doc to [`ARCH.md`](../ARCH.md). Authoritative schema lives in
`backend/schema.surql`; authoritative wire shape lives in
`backend/src/api/discovery.rs`.

## Endpoints

All under `/api/discovery/`, all protected (any authenticated user, no
special role).

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/feed?sort=&cursor=&limit=` | Cursor-paginated list of documents joined with the caller's read state. |
| `POST` | `/items/{key}/read` | Mark a document read. Idempotent. |
| `DELETE` | `/items/{key}/read` | Mark a document unread. Idempotent. |
| `GET` | `/feed/events` | SSE stream pushing `event: new_document` records as ingestion accepts new papers. |

`{key}` is the bare SurrealDB record key (everything after
`document:`). Keeps URLs free of the table prefix; the frontend has a
`docKey()` helper that strips it from feed-item ids.

### Cursor

Opaque base64-encoded JSON blob. The wire shape is intentionally
decoupled from the storage shape (`storage::FeedCursor`) so:

- The server can change sort algorithms (recency now, score+age later)
  without changing the API contract — only the encoder/decoder updates.
- The client never inspects the cursor; it just round-trips whatever
  the server handed back.

Today's recency cursor encodes `{ p: ingested_at_RFC3339, i: doc_key }`
and the storage layer translates that into a `WHERE (ingested_at, id) <
($p, $i)` clause for stable pagination across new arrivals at the top.

A response with `next_cursor: null` signals end-of-feed (the page came
back partial). Pages are full-or-final; clients hide "Load more" when
`next_cursor` is null.

## Read state

```surql
DEFINE TABLE feed_read SCHEMAFULL;
DEFINE FIELD user     ON feed_read TYPE record<app_user>;
DEFINE FIELD document ON feed_read TYPE record<document>;
DEFINE FIELD read_at  ON feed_read TYPE datetime DEFAULT time::now();
DEFINE INDEX feed_read_user_doc ON feed_read FIELDS user, document UNIQUE;
```

Existence is the source of truth: a row means the user has read the
document; absence means unread. Mark-read is an upsert (idempotent);
mark-unread is a delete (idempotent). The unique `(user, document)`
index doubles as the lookup index for the join.

`Storage::list_feed` issues two queries — documents page, then a
`document IN $ids` lookup against `feed_read` — and merges in
application code. A single nested-SELECT join was tried and rejected:
the SDK refuses to deserialize `FeedItem<#[serde(flatten)] Document>`
when `Document` carries a `serde_json::Value` field (flatten + untagged
enum collide). Two queries cost one extra round trip per page; at the
default 50 items it's not measurable.

## Live updates: NotifyingSink

The backend's `IngestSink` trait composes — wrappers around the canonical
`Pipeline` add cross-cutting concerns without changing callers.
`NotifyingSink` is one such wrapper:

```text
HTTP /api/ingestion/documents ─┐
                               ├─▶ NotifyingSink ─▶ Pipeline ─▶ Storage
sources::scheduler ────────────┘                  │
                                                  └─▶ broadcast::Sender (on Created)
```

Both ingest paths share one `NotifyingSink` instance constructed at
startup and stored in `AppState`. Fan-out happens once, regardless of
where the document came from. Only the `Created` outcome fires;
`Unchanged` and `Versioned` are silent (the user has already seen those
papers — surfacing version bumps is out of scope for v1).

The broadcast channel is bounded (256 events) and lossy by design. Slow
SSE clients lag rather than blocking ingestion; the SSE handler swallows
`RecvError::Lagged` and continues — the user just doesn't see those
events as "new in this session."

## SSE handler

Plain `text/event-stream`, **not** the AI SDK Data Stream Protocol
(which `/api/chat` uses). Records:

```
event: new_document
data: {"id":"document:...","canonical_id":"...","source_type":"...","title":"...","ingested_at":"..."}
```

`KeepAlive::default()` injects a comment line periodically so idle
connections survive proxy timeouts (relevant for Tier 2 through Traefik
+ oauth2-proxy).

The handler is a `subscribe()` per request → `futures::stream::unfold`
adapter that turns the broadcast receiver into an `Sse<Stream>`. No
shared state; tearing down the connection drops the receiver.

## Frontend

`Feed.tsx` is the page; `PaperCard.tsx` is the row; `useFeedEvents.ts`
is the SSE hook.

### Pagination

`@tanstack/react-query`'s `useInfiniteQuery` keyed by `["discovery-feed",
{ sort }]`. `getNextPageParam` reads `next_cursor` from the response;
"Load more" calls `fetchNextPage()`. Switching sort resets the query
because the key changes.

### Live updates + scroll preservation

When the SSE hook fires `new_document`:

1. Add the doc id to a per-session `Set<string>` of "newly arrived"
   ids.
2. `queryClient.invalidateQueries({ queryKey, exact: true })` — refetches
   only the first page so the full record (with summary, authors, etc.)
   lands. Loaded subsequent pages stay intact.
3. The card mounts at the top with `data-new="true"`, which applies a
   glow ring + a "New" badge.

Scroll position is preserved by the browser's native `overflow-anchor:
auto` (default in modern browsers). When DOM nodes prepend above the
viewport, the browser pins the user's content to its current scroll
position — no JS bookkeeping. This is the inverse of the Chat surface,
which uses sentinel + IntersectionObserver to *follow* the bottom; both
patterns coexist because they solve opposite problems.

### Newness fade

A page-level IntersectionObserver (root = `#app-scroll` from
`__root.tsx`, threshold 0.5) watches every `[data-new="true"]` card.
When a card has been ≥50% visible for 1s, its id is removed from the
"new" set and React re-renders the card without the highlight. Timers
are cancelled if the card leaves the viewport before they fire. A
MutationObserver-equivalent scan on next animation frame picks up
freshly mounted cards without polling.

### "N new above" pill

Shown when `scrollTop > 100 && newSet.size > 0`. Click smooth-scrolls
to the top of `#app-scroll`. Style mirrors the Chat surface's
"scroll-to-bottom" floating button.

### Optimistic mark-read

Card click → `useMutation` with `onMutate` that flips `read: true` in
the React Query cache (`setQueryData` over all pages). On error the
mutation rolls back from the captured `prev` snapshot. Mark-unread is
the same shape, fired by the "Read" chip's click handler (which
`stopPropagation`s so it doesn't re-trigger mark-read).

## Sort architecture

Today only `sort=recency` is recognised; the API accepts any string and
falls through to recency rather than 400-ing, so the frontend can ship
new sort values before the backend implements them. Each sort
algorithm owns its cursor encoding (opaque to clients), so adding e.g.
`score` later only changes:

- the storage layer (new query path)
- the cursor encoder/decoder in `discovery.rs`
- the dropdown in `Feed.tsx`

No API contract change.

## Tenant scoping (deferred)

Matches the existing TODO at `ingestion/pipeline.rs`. `list_feed` does
NOT filter by tenant in v1; `feed_read` rows are per-user, which is
per-tenant naturally. When the multi-tenant slice lands, the storage
methods grow a `tenant_id` parameter and queries gain `WHERE tenant =
$tenant`. The wire contract doesn't change — `tenant_id` flows in from
the `AuthContext`.

## Files

| Layer | Path |
|---|---|
| Schema | `backend/schema.surql` |
| Storage trait + types | `backend/src/storage/{mod,models,surreal}.rs` |
| Notifier wrapper | `backend/src/ingestion/notifier.rs` |
| API handlers | `backend/src/api/discovery.rs` |
| Backend tests | `backend/tests/discovery_feed.rs` |
| Page | `frontend/src/components/discovery/Feed.tsx` |
| Card | `frontend/src/components/discovery/PaperCard.tsx` |
| SSE hook | `frontend/src/hooks/useFeedEvents.ts` |
| API client | `frontend/src/lib/api.ts` (`api.discovery.*`) |
| Route | `frontend/src/routes/feed.tsx` |
| Frontend tests | `frontend/src/components/discovery/{PaperCard,Feed}.test.tsx` |
