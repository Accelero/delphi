# Chat Microservice Migration Plan

Status: implementation started. This is the first implementation slice of the greenfield
microservice rewrite. It replaces the old single-backend chat path with a
NATS/JetStream-backed, horizontally scalable chat system.

Current implementation baseline:

- Root Rust workspace with `api-service`, `realtime-service`, `chat-worker`,
  and shared `auth`, `config`, `contracts`, `storage`, and `nats` crates.
- React/Vite/Tailwind frontend shell with conversation sidebar, chat pane,
  WebSocket envelope handling, and smoothed assistant rendering.
- `docker-compose.t2.yml` scaffolds the intended T2 shape with Traefik,
  oauth2-proxy, Keycloak, Redis, NATS, SurrealDB, frontend, and the three
  backend binaries.
- Chat API state is backed by SurrealDB, chat commands/events/locks are backed
  by NATS JetStream/KV, and the `chat-worker` calls the configured LLM provider
  before atomically committing the user/assistant turn.
- Realtime WebSocket subscriptions authorize against SurrealDB and forward
  authorized live NATS events. Replay/cursor recovery is still tracked in the
  realtime replay refinement plan.

Reference old implementation for behavior, not structure:

- `old/frontend/src/hooks/useChatStream.ts`
- `old/frontend/src/components/chat/Chat.tsx`
- `old/frontend/src/components/chat/MessageBody.tsx`
- `old/backend/src/api/chat.rs`
- `old/backend/src/chat/worker.rs`
- `old/backend/src/api/chat_stream.rs`

Realtime replay refinement:

- [Chat realtime replay plan](./chat-realtime-replay-plan.md)

The new protocol uses WebSocket instead of SSE. Do **not** tunnel raw SSE
frames over WebSocket. Use structured JSON envelopes that explicitly carry
the fields EventSource used to provide implicitly: event type, event id,
cursor replay, reconnect behavior, and control messages.

The important UX contract stays the same: no optimistic local-only message,
server-authoritative live state, replay for late join, smoothed character
rendering, multi-tab convergence, conversation-scoped stop, and atomic
commit.

## 1. Target Architecture

```text
Browser React app
  +- HTTP /api/chat/* --------> api-service
  `- WS   /ws/chat -----------> realtime-service

api-service
  +- validates JWT/AuthContext
  +- reads/writes SurrealDB
  `- publishes chat.commands.turn_requested / chat.control.stop

realtime-service
  +- validates JWT/AuthContext
  +- owns browser WebSocket connections
  +- authorizes conversation subscriptions via SurrealDB
  `- replays/follows chat.events.<tenant>.<conversation> from JetStream

chat-worker
  +- consumes turn_requested commands
  +- verifies and renews the per-conversation lock
  +- publishes live turn events to JetStream
  +- calls retrieval/LLM
  +- listens for stop control messages
  `- commits user+assistant messages atomically to SurrealDB
```

Core rule: HTTP commands create intent, NATS carries live session state,
SurrealDB stores committed truth, and WebSocket only relays authorized NATS
events to browsers.

## 2. Step-by-Step Implementation

### Step 1: T2 Chat Platform Skeleton

Build the minimal full-auth stack before chat behavior:

- Create a Rust workspace with `api-service`, `realtime-service`,
  `chat-worker`, and shared crates for auth, config, contracts, storage,
  and NATS bootstrap.
- Create a React frontend with Vite, React Router or TanStack Router,
  TanStack Query, Tailwind, and shadcn/ui.
- Add Tier 2 compose services: Traefik, oauth2-proxy, Keycloak, Redis,
  SurrealDB, NATS with JetStream enabled, frontend Caddy bundle,
  `api-service`, `realtime-service`, and `chat-worker`.
- Route `/api/*` to `api-service`, `/ws/*` to `realtime-service`,
  `/healthz` as public health, and `/` to frontend.
- Keep oauth2-proxy as the BFF; services receive
  `Authorization: Bearer <jwt>` and validate via Keycloak JWKS.
- Derive a shared `AuthContext { user_id, tenant_id, email, roles }`.

Manual gate:

- Bring up Tier 2 from zero.
- Login through Keycloak.
- Frontend calls `/api/auth/me`.
- API, realtime, chat-worker, SurrealDB, and NATS health checks pass.

### Step 2: Chat Storage Schema

Use a new clean SurrealDB schema. Do not preserve old table shapes except
where it helps the frontend contract.

Tables:

- `tenant`: `id`, `name`
- `app_user`: `id`, `tenant_id`, `email`, `display_name`
- `conversation`: `id`, `tenant_id`, `user_id`, `title`, `created_at`,
  `updated_at`, `deleted_at`
- `message`: `id`, `tenant_id`, `conversation_id`, `role`, `content`,
  `citations`, `turn_id`, `created_at`, `ordinal`
- `chat_turn`: `id`, `tenant_id`, `conversation_id`, `user_message_id`,
  `assistant_message_id`, `parent_message_id`, `status`, `created_at`,
  `updated_at`, `error`

Rules:

- Every domain row carries `tenant_id`.
- Read APIs only return rows for the authenticated tenant/user.
- `message.ordinal` is monotonic per conversation.
- User and assistant rows are inserted in one transaction at commit.
- Cancelled turns persist no user/assistant messages.
- A failed generation may persist user + assistant failure text only if the
  worker reaches the commit branch; never user-only.

Manual gate:

- Create, list, get, rename, and delete conversations through authenticated
  API calls.
- Verify tenant isolation with two Keycloak users/tenants.

### Step 3: NATS Chat Contracts

Create versioned JSON contracts in the shared contracts crate. Every event
includes:

```json
{
  "v": 1,
  "tenant_id": "tenant-a",
  "conversation_id": "01...",
  "turn_id": "01...",
  "ts": "RFC3339"
}
```

`event_id` is not part of the event before publish. It is assigned from the
JetStream sequence returned in the PubAck. The realtime service wraps stored
events as `{ event_id, event }` when sending them to browsers.

Streams and subjects:

- `CHAT_COMMANDS`
  - subject: `chat.commands.turn_requested`
  - durable consumer: `chat-worker-turns`
  - retention: work queue
- `CHAT_EVENTS`
  - subject: `chat.events.<tenant_id>.<conversation_id>.<turn_id>`
  - retention: limits
  - cursor: JetStream stream sequence
  - stores live turn events for reconnect and late join
- worker control subject
  - subject: `chat.control.worker.<worker_id>.stop`
  - core NATS low-latency wake-up
  - payload carries `{ tenant_id, conversation_id, turn_id }`
  - not a queue group; the target worker is read from `CHAT_LOCKS`
- `CHAT_LOCKS`
  - JetStream KV bucket
  - key: `<tenant_id>/<conversation_id>`
  - requested value:
    `{ turn_id, state: "requested", worker_id: null, stop_requested, stop_requested_by, stop_requested_at, lease_expires_at }`
  - running value:
    `{ turn_id, state: "running", worker_id, stop_requested, stop_requested_by, stop_requested_at, lease_expires_at }`
  - create-only claim by API; compare-and-set ownership update by worker;
    release on finish/interrupted/clear; TTL handles crashed workers
  - stop fields live on this same key so immediate stop and worker claim race
    through one compare-and-set state machine

Worker identity:

- Each `chat-worker` reads `CHAT_WORKER_ID` or generates a random UUID at
  startup.
- The worker id is process-lifetime stable and included in logs.
- Later Kubernetes deployments can set it from pod name plus pod UID.

Command payload:

```ts
type TurnRequested = {
  v: 1;
  command_id: string;
  tenant_id: string;
  user_id: string;
  conversation_id: string;
  turn_id: string;
  user_message_id: string;
  text: string;
  parent_message_id: string | null;
  bearer_subject: string;
};
```

Live event payloads:

```ts
type ChatEvent =
  | { type: "turn_started"; turn_id: string }
  | { type: "user_message"; id: string; content: string }
  | { type: "citations"; citations: CitationEntry[] }
  | { type: "text_delta"; delta: string }
  | { type: "finish"; assistant_message_id: string; finish_reason: "stop" | "error" }
  | { type: "interrupted"; assistant_message_id: string; content: string; finish_reason: "user_interrupted" }
  | { type: "clear"; reason: "cancelled" | "worker_lost" | "failed_before_commit" }
  | { type: "error"; message: string }
  | { type: "title_updated"; title: string };
```

Rules:

- `api-service` claims the single-flight lock before returning 202.
- `user_message` is published by the worker after it verifies ownership of
  the lock, not by the API.
- `text_delta` preserves provider chunk boundaries; frontend smoothing owns
  visual pacing.
- `finish` means the DB commit already succeeded.
- `interrupted` means the DB commit already succeeded with partial assistant
  content and interrupt metadata.
- `clear` means the turn did not commit and clients must drop in-flight UI.
- `title_updated` is best-effort and only sent after the title is durable.

### Step 4: API Service Surface

Implement REST commands before frontend polish.

Endpoints:

- `GET /api/auth/me`
- `GET /api/chat/conversations`
- `POST /api/chat/conversations`
- `GET /api/chat/conversations/:id`
- `PATCH /api/chat/conversations/:id`
- `DELETE /api/chat/conversations/:id`
- `POST /api/chat/conversations/:id/turns`
- `POST /api/chat/conversations/:id/stop`

Submit endpoint behavior:

- Body: `{ user_message_id, turn_id, text, parent_message_id }`.
- Validate ULIDs, non-empty text, and conversation ownership.
- Check `parent_message_id` equals the committed tail.
- Reject stale tail as `409 stale_parent`.
- Atomically create `CHAT_LOCKS/<tenant_id>/<conversation_id>` with TTL.
- If the lock already exists, return `409 in_flight`.
- Create or update `chat_turn(status=requested)`.
- Publish `TurnRequested` with deterministic `Nats-Msg-Id = turn_id`.
- Wait for PubAck before returning.
- Return `202 Accepted`.

Stop endpoint behavior:

- Authorize conversation access exactly like submit/get.
- Read `CHAT_LOCKS/<tenant_id>/<conversation_id>`.
- If no lock exists, return `204`.
- Compare-and-set the same lock revision to set
  `stop_requested=true`, `stop_requested_by`, and `stop_requested_at`.
- If the CAS fails, re-read and retry; this resolves stop-vs-worker-claim
  races.
- If the updated lock has `worker_id`, publish a low-latency wake-up to
  `chat.control.worker.<worker_id>.stop`.
- If the lock is still requested and has no worker owner yet, do not publish
  control; the worker sees `stop_requested` during its ownership CAS before
  provider start.
- Return 204 whether or not a turn is running.
- Do not mutate visible chat state directly; worker live events are the
  source of truth.

HTTP error shape:

```ts
type ApiError = {
  error: {
    code:
      | "unauthorized"
      | "forbidden"
      | "not_found"
      | "invalid_request"
      | "stale_parent"
      | "in_flight"
      | "internal";
    message: string;
  };
};
```

Manual gate:

- Authenticated curl or browser context can create a conversation, submit a
  turn, receive 202, and observe a NATS command.

### Step 5: Chat Worker

Worker lifecycle:

1. Pull `TurnRequested` from `CHAT_COMMANDS`.
2. Validate tenant, user, and conversation still exist.
3. Verify `CHAT_LOCKS` belongs to `turn_id`.
4. Compare-and-set `CHAT_LOCKS` to `state=running` with this worker's
   `worker_id`, preserving stop request fields.
5. If the claimed lock has `stop_requested=true`, interrupt before provider
   start.
6. Create/update `chat_turn(status=running)`.
7. Publish `turn_started`.
8. Publish `user_message`.
9. Start periodic command progress ACKs.
10. Renew the lock lease while the turn is running.
11. Load committed message history.
12. Run retrieval placeholder first; real RAG can come later.
13. Start LLM stream.
14. Publish each provider chunk as a `text_delta` unchanged and append it to
    the worker's partial assistant buffer.
15. Race LLM stream against the local cancellation token fed by
    `chat.control.worker.<worker_id>.stop` and lock watch/poll fallback checks.
16. On stop before normal finish: stop reading the provider stream, insert
    user + partial assistant messages in one transaction, mark the assistant
    message/turn `interrupted`, publish `interrupted`, and release lock.
17. On LLM EOF: insert user + assistant messages in one transaction, mark
    turn `committed`, publish `finish`, and release lock.
18. On pre-commit fatal error: publish `error`, publish `clear`, mark turn
    `failed`, and release lock.
19. On worker crash: lock TTL expires; reconciler publishes `clear` for the
    stale running turn and marks it failed/cancelled.

First validation should use a deterministic fake streaming provider in T2.
It should emit uneven timing and chunk sizes so frontend smoothing can be
inspected. Real provider wiring follows after the protocol and UI are stable.

Title generation is deferred until basic chat works. When added, it runs
after commit, persists the title, and publishes `title_updated`.

Manual gate:

- Submit a message and watch `CHAT_EVENTS`.
- Stop mid-stream and verify `interrupted` plus committed partial assistant
  content.
- Start two chat-worker replicas and verify only one verifies ownership and
  processes the turn.
- Kill a worker mid-turn and verify TTL/reconciler clears the UI.

### Step 6: Realtime WebSocket Service

WebSocket URL:

```text
GET /ws/chat
```

Connection behavior:

- Browser WebSocket clients cannot set custom `Authorization` headers.
  Authentication relies on same-origin cookies to oauth2-proxy; Traefik
  forward-auth must inject the bearer token into the upstream upgrade
  request before `realtime-service` accepts the connection.
- Authenticate via the forwarded bearer JWT before upgrade.
- Hold no SurrealDB connection after initial authorization checks.
- Use one process-level NATS connection per realtime replica.
- Use one NATS/JetStream consumer per active conversation per realtime
  replica, not per browser tab.
- Fan out to all local WebSocket clients subscribed to that conversation.

Do not send raw SSE frame strings over WebSocket. WebSocket messages are
structured JSON envelopes:

```ts
type WsEventEnvelope = {
  v: 1;
  type: "event";
  conversation_id: string;
  event_id: string;
  event: ChatEvent;
};
```

Client messages:

```ts
type ClientMessage =
  | { type: "subscribe_conversation"; conversation_id: string; last_event_id?: string | null }
  | { type: "unsubscribe_conversation"; conversation_id: string }
  | { type: "ping"; nonce?: string };
```

Server messages:

```ts
type ServerMessage =
  | { type: "subscribed"; conversation_id: string }
  | { type: "event"; conversation_id: string; event_id: string; event: ChatEvent }
  | { type: "resync_required"; conversation_id: string }
  | { type: "error"; code: string; message: string }
  | { type: "pong"; nonce?: string };
```

Replay rules:

- Subscribe without `last_event_id`: replay current retained in-flight turn
  events if present.
- Subscribe with `last_event_id`: start after that JetStream sequence.
- If the requested cursor is older than the retained floor, send
  `resync_required`.
- Client handles `resync_required` by clearing transient overlay and
  refetching committed history.
- If no turn is in-flight and no retained event is relevant, subscription
  still succeeds and waits for future events.

Connection liveness:

- Server sends ping or expects browser ping every 20-30 seconds.
- Browser reconnects with exponential backoff.
- Browser stores last event id per conversation in memory.
- No localStorage persistence for live cursors in v1.

Backpressure:

- Each socket gets a bounded outbound queue.
- One slow socket must not block the per-conversation NATS reader or other
  sockets.
- If a socket falls behind, close it with a retryable error and let the
  browser reconnect with its last applied `event_id`.
- Advance the browser's `lastEventId` only after the event is applied to
  local state.

Manual gate:

- Open two tabs connected to different realtime replicas.
- Subscribe both to the same conversation.
- Submit from one tab.
- Both tabs receive identical ordered events.
- Disconnect/reconnect one tab mid-turn and verify replay.

### Step 7: Frontend Data Layer

Build the frontend around server-authoritative state, preserving old
`useChatStream.ts` semantics.

API wrapper:

- Use `fetch(..., { credentials: "same-origin" })`.
- On 401, hard navigate to `/oauth2/sign_in`.
- Keep response types explicit.
- Submit uses `POST /turns` and treats only 202 as success.

Queries:

- `useSession()`
- `useConversations()`
- `useConversation(conversationId)`
- `useCreateConversation()`
- `useRenameConversation()`
- `useDeleteConversation()`

WebSocket hook:

```ts
useChatSocket(conversationId, {
  initialMessages,
  onTurnEnd,
  onTitle,
})
```

State model:

- `messages`: committed seed plus in-flight overlay
- `status`: `ready | submitted | streaming | error`
- `citations`: live citation table for current assistant overlay
- `error`: current visible error
- `lastKnownMessageIdRef`: committed tail id
- `inFlightUserIdRef`: current live user message id
- `overlayRef`: raw assistant target text accumulated from `text_delta`
- `lastEventIdRef`: latest JetStream event id per conversation

Critical reset rule:

- Every `user_message` event clears assistant overlay, clears citations,
  resets current in-flight user id, and starts a new visible turn.
- This applies to both new live turns and replayed events.
- This prevents duplicated text after reconnect.

Submit behavior:

- Generate ULIDs for `turn_id` and `user_message_id`.
- Set status to `submitted`.
- POST the command.
- Do not insert a local optimistic user message.
- The visible user message arrives only from WebSocket `user_message`.
- On `409 stale_parent`, refetch history and show
  "Conversation changed; refreshing."
- On `409 in_flight`, keep/return to streaming state if live state exists;
  otherwise refetch.

Stop behavior:

- POST `/stop`.
- Do not locally clear.
- Stop completion comes from WebSocket `interrupted`.
- `clear` remains only for uncommitted discard cases.

Finish behavior:

- Convert streaming assistant overlay row to a committed row using
  `assistant_message_id`.
- Attach live citations to that row until the history refetch returns.
- Clear live refs.
- Invalidate conversation and conversation list queries.

Clear behavior:

- Drop streaming assistant row.
- Drop in-flight user row.
- Clear citations and overlay.
- Set status ready.
- Invalidate/refetch history.

### Step 8: Frontend Rendering and Smoothing

Reuse the old rendering ideas from `MessageBody.tsx`, adapted to the new
component tree.

Message body:

- Use Streamdown for streaming-safe markdown.
- Use plugins equivalent to old `cjk`, `code`, `math`, and `mermaid`.
- Preserve `<think>...</think>` parsing into a collapsible reasoning block.
- Preserve citation rewrite from `[N]` to links.
- Do not render individual character spans.

Smoothing:

- Keep raw provider deltas unchanged in `overlayRef`.
- `MessageBody` receives full target content.
- `useSmoothedContent(target, isStreaming, "char")` releases display text
  with `requestAnimationFrame`.
- If `isStreaming=false`, shown content starts as full target.
- If `isStreaming=true`, shown starts empty and drains toward target.
- Character mode minimum is `40 chars/sec`.
- Speed increases with backlog: `rate = min + k * buffered_chars`, with
  `k = 2`.
- The buffer continues draining after upstream chunks pause.
- It never snaps full text into view unless the message is non-streaming
  history.
- Rendered markdown receives the smoothed `shown` string.

Important CSS rule:

- Do not use Streamdown's per-token animation span mode for assistant
  streaming. The visual effect is controlled by `useSmoothedContent`;
  markdown is rendered normally.

### Step 9: Frontend Scroll Behavior

Preserve the old `Chat.tsx` scroll model.

Layout:

- Chat root is `flex flex-col h-full min-h-0`.
- Scroll viewport is an absolutely positioned internal div with
  `overflow-y-auto`, `outline-none`, and `[overflow-anchor:none]`.
- Content wrapper uses horizontal padding and top padding only.
- Do not add bottom padding that shifts scroll anchoring.

Turn grouping:

- Group flat messages into turns.
- Each user message starts a turn.
- Assistant/system messages append to the current turn.
- Last turn gets `min-height = scroll viewport height`.
- Old turns collapse to natural height.

New-turn behavior:

- When a new turn id appears, re-enable follow mode.
- On the next animation frame, find `[data-turn-id="<lastTurnId>"]` and
  call `scrollIntoView({ block: "start" })`.
- This pins the user message at the top and reserves viewport space below
  for the assistant reply.

Auto-follow:

- Add a final sentinel div with `aria-hidden` and `className="h-px"`.
- Use `IntersectionObserver` with the scroll viewport as root and
  threshold `0`.
- If sentinel intersects, follow mode is true.
- If sentinel leaves while follow mode is true, call
  `sentinel.scrollIntoView({ block: "end" })` as post-paint layout-shift
  backup.
- On every render, `useLayoutEffect` snaps to bottom if follow mode is
  true: `scrollTop = scrollHeight - clientHeight`.
- Detect user escape by observing actual upward scroll: if new `scrollTop`
  is less than previous `scrollTop - 1`, set follow false.
- Show a floating round shadcn `Button` with `ArrowDownIcon` when follow is
  false.
- Clicking it smooth-scrolls the sentinel into view; the observer
  re-enables follow.

Thinking state:

- Show assistant "Thinking..." row when status is `submitted` or
  `streaming` and the last visible message is still the user message.
- Once the first assistant text appears, replace thinking with assistant
  overlay.

### Step 10: Frontend UI Composition

Use React + shadcn/ui, with a focused chat interface rather than a landing
page.

Pages:

- `/chat`: conversation sidebar plus draft chat pane.
- `/chat/:conversationId`: conversation sidebar plus active chat pane.

Components:

- `ConversationSidebar`: list, active item, create, rename, delete.
- `ChatPane`: query seed, WebSocket hook, turns, error, input.
- `MessageBody`: smoothing, markdown, reasoning, citations.
- `PromptComposer`: textarea, send/stop icon button, disabled/busy states.
- `ScrollToBottomButton`
- `CopyMessageAction`

shadcn usage:

- Use shadcn `Button`, `Textarea`, `DropdownMenu`, `Tooltip`, and related
  primitives where appropriate.
- Do not use shadcn `ScrollArea` for the chat message viewport; the custom
  viewport/sentinel pattern is the source of truth.
- Use lucide icons for send, stop, copy, rename, delete, new chat, and
  scroll down.
- Keep a compact app layout; no landing page or explanatory cards.

Manual gate:

- Chat works on desktop and mobile widths.
- Long streamed markdown does not produce character-span DOM bloat.
- User can scroll up without the stream fighting them.
- Scroll-to-bottom button appears and recovers follow mode.

## 3. Test Plan

Write new tests; do not mechanically port old tests.

Backend unit tests:

- Auth context extraction from JWT claims.
- Conversation key validation.
- Parent-tail stale detection.
- NATS contract serialization.
- Chat lock acquire conflict and release.
- Duplicate `TurnRequested` with the same idempotency key does not start
  two turns.

Backend integration tests:

- API submit publishes command.
- API claims lock; worker verifies ownership and publishes `user_message`,
  `text_delta`, `finish`.
- Stop sets `stop_requested` on `CHAT_LOCKS`, routes control to the lock's
  `worker_id` when present, publishes `interrupted`, and commits user +
  partial assistant messages with interrupt metadata.
- Immediate stop while the lock is still `requested` is not lost; the API stop
  CAS and worker ownership CAS conflict on the same lock key, forcing one side
  to re-read the other's state.
- Finish commits user+assistant atomically.
- Second concurrent submit returns `in_flight`.
- Stale parent returns `stale_parent`.
- Realtime subscribe replays from cursor.
- Realtime subscribe with trimmed cursor emits `resync_required`.

Frontend unit tests:

- `useSmoothedContent` starts empty for streaming and drains toward target.
- Smoothing does not split output into per-character spans.
- `user_message` reset rule clears previous overlay.
- `clear` drops in-flight user and assistant.
- `interrupted` keeps the partial assistant row and marks it interrupted.
- `finish` stamps assistant id and citations before refetch.
- Citation rewrite preserves unresolved markers.

Frontend component tests:

- New turn scrolls user message to top.
- Sentinel controls follow mode.
- Upward scroll disables follow.
- Scroll-to-bottom re-enables follow.
- Stop button appears only while a turn is in flight.
- Two simulated WebSocket clients converge on the same visible messages.

T2 end-to-end tests:

- Keycloak login.
- Create conversation.
- Single-tab chat round trip.
- Two-tab live fan-out.
- Late join mid-turn replay.
- Stop from non-submitting tab.
- Refresh after finish shows committed history.
- Refresh after cancel shows no cancelled turn.
- Two realtime replicas plus two chat-worker replicas.

## 4. Implementation Order

1. Build T2 skeleton and `/api/auth/me`.
2. Add SurrealDB schema and conversation CRUD.
3. Add NATS stream/KV bootstrap.
4. Implement API submit/stop endpoints with command publishing.
5. Implement chat-worker with fake LLM stream.
6. Implement realtime WebSocket subscribe/replay/fan-out.
7. Build frontend shell, routing, auth, conversation sidebar.
8. Build WebSocket hook with old `useChatStream` state semantics.
9. Build message rendering with Streamdown, citations, reasoning, and
   character smoothing.
10. Build scroll model with turn min-height and 1px sentinel.
11. Add stop, reconnect, resync, and title update behavior.
12. Add tests and Tier 2 e2e coverage.
13. Manual review gate before moving to ingestion.

## 5. Assumptions

- WebSocket fully replaces SSE for new chat.
- NATS/JetStream is mandatory from the first implementation; there is no
  in-process chat fallback.
- Fake LLM streaming is acceptable for first manual validation; real
  provider wiring follows once protocol and UI are stable.
- RAG citations are kept in the chat event contract now, but retrieval can
  initially return an empty list.
- The old frontend is reference code; the new frontend should reimplement
  the same behavior with clean components rather than copying the old tree
  wholesale.
