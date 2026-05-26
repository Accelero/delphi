# Feed Microservice Migration Plan

Status: planned. This is the third rebuild slice. Feed comes after chat and
ingestion because it needs product rework, not only a replacement for the old
process-local SSE broadcast.

Reference old implementation for behavior and constraints:

- `old/docs/architecture/discovery-feed.md`
- `old/backend/src/api/discovery.rs`
- `old/backend/src/ingestion/notifier.rs`
- `old/frontend/src/components/discovery/Feed.tsx`
- `old/frontend/src/hooks/useFeedEvents.ts`

## 1. Goal

Define and rebuild the feed as a durable, tenant-isolated product surface
with optional live updates. Missed live events must always recover from a
normal durable query.

The feed should not depend on ingestion workers and browser clients being
connected to the same backend process.

## 2. Product Decision First

Before implementation, lock the feed model:

- Document-centric feed: shows ready documents ordered by recency.
- Activity-centric feed: shows ingestion, source, enrichment, and agent
  activity.
- Source-centric feed: groups results by source adapter/query.
- Hybrid feed: durable activity stream with document cards as the main
  rendering.

Recommended initial choice: document-centric feed with room for activity
metadata. This best matches the old discovery feed and is easiest to validate
after ingestion.

## 3. Target Architecture

```text
ingest-publisher
  `- emits document.ready event after state=ready

feed-service / api-service
  +- reads durable ready documents/feed rows
  `- exposes paginated feed API

realtime-service
  +- subscribes to feed.events.<tenant>
  `- forwards live feed hints to WebSocket clients

frontend
  +- queries durable feed pages
  `- optionally prepends live updates or shows "new items" affordance
```

The live path is a hint layer. The durable feed query is the source of
truth.

## 4. Storage Model

Initial model:

- `feed_item`
  - `id`
  - `tenant_id`
  - `kind`: `document_ready`
  - `document_id`
  - `source_type`
  - `source_uri`
  - `title`
  - `summary`
  - `authors`
  - `occurred_at`
  - `created_at`
  - `metadata`

Rules:

- Feed rows are created only after the document is ready.
- Feed queries are tenant-scoped.
- Feed item payload should be denormalized enough for fast cards.
- Document detail remains read from document storage.
- Missing or duplicate live events must not corrupt durable feed ordering.

## 5. NATS Design

Subjects:

- `feed.events.<tenant_id>`

Payload:

```ts
type FeedEvent = {
  v: 1;
  type: "feed_item_ready";
  tenant_id: string;
  feed_item_id: string;
  document_id: string;
  occurred_at: string;
};
```

Behavior:

- `ingest-publisher` writes the durable feed row after document readiness.
- Then it publishes `feed_item_ready`.
- `realtime-service` forwards the event to subscribed clients.
- Frontend either fetches the item by id or invalidates the feed query.
- If the event is missed, the next feed query still returns the item.

Core NATS pub/sub is acceptable for live hints. JetStream may be used if we
want short replay, but the durable feed query remains authoritative.

## 6. API Surface

Initial endpoints:

- `GET /api/feed?cursor=&limit=`
  - returns reverse-chronological feed page.
- `GET /api/feed/items/:id`
  - optional item lookup for live event hydration.

WebSocket:

- Extend realtime service with:
  - client message `{ type: "subscribe_feed" }`
  - client message `{ type: "unsubscribe_feed" }`
  - server event `{ type: "feed_event", event: FeedEvent }`

Cursor:

- Opaque cursor based on `occurred_at` plus stable id tie-breaker.
- Cursor details stay server-owned.

## 7. Frontend

Build feed after ingestion provides ready documents.

Behavior:

- Paginated reverse-chronological list.
- Cards optimized for scanning documents.
- Live events do not force-scroll the user.
- If user is near top, live items may prepend.
- If user has scrolled away, show a "new items" pill.
- Clicking the pill scrolls to top and refreshes/prepends new items.

Reuse old feed UX ideas, but avoid depending on old process-local SSE.

## 8. Test Plan

Unit tests:

- feed cursor encode/decode.
- tenant-scoped feed query.
- idempotent feed row creation for a document.
- live event payload serialization.

Integration tests:

- document ready creates one feed item.
- duplicate document-ready event does not duplicate feed item.
- feed page ordering is stable.
- realtime feed subscription receives live event.
- missed live event recovers through feed query.

T2 e2e tests:

- upload document to ready.
- feed shows document.
- second tab receives live feed hint.
- scrolled-away user sees new-items affordance.
- refresh shows same durable feed content.

Manual gate:

- Ready documents appear in feed.
- Live updates work across realtime replicas.
- Missed events recover by refresh/query.
- Feed does not show staging or failed documents.

## 9. Assumptions

- Feed is rebuilt after ingestion.
- Initial feed is document-centric unless explicitly changed before work
  starts.
- Live feed updates are best-effort hints.
- Durable feed queries are the source of truth.
